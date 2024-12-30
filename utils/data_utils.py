import pycolmap
import json
import numpy as np
import csv
from scipy.spatial.transform import Rotation as scipy_Rotation
from numpy.linalg import norm
from numpy import arccos, rad2deg
import torch
import logging
import cv2
import time
from src.ply_export import export_ply_text
import datetime
import platform
import psutil
import GPUtil

floor_rotation = pycolmap.Rotation3d(np.array([0, 0.7071068, 0, 0.7071068]))
floor_rotation_inv = pycolmap.Rotation3d(np.array([0, -0.7071068, 0, 0.7071068]))
VERSION = "opencv-camera-model"

def convert_pose_opengl_to_colmap(position, quaternion):
    
    position = np.array([
        position[1],
        position[0],
        position[2] * -1
    ])
    quaternion = np.array([
        quaternion[1],
        quaternion[0],
        quaternion[2] * -1,
        quaternion[3]
    ])

    return position, quaternion


def rectify_floor_portal(qr_pose):
    pos = qr_pose.translation
    rot3d = qr_pose.rotation

    world_forward = rot3d.matrix() @ np.array([0.0, 0.0, 1.0])
    if world_forward[0] < -0.9:
        rot3d = rot3d * floor_rotation
        rot3d.quat = flatten_quaternion(rot3d.quat)
        rot3d = rot3d * floor_rotation_inv
        
        # If flat and also near floor, snap height too. But NOT snapping desk portals to floor!
        if np.abs(pos[0]) < 0.5:
            pos = pos.copy() # avoid modifying input pose
            pos[0] = 0.0

    return pycolmap.Rigid3d(rot3d, pos)


def load_portals_json(file_path):
    portal_poses = {}

    # Have to rotate 90 deg for now since the DMT Recorder captures rotation after doing a 90 deg in the app.
    # This function however loads raw poses from domain which are not rotated 90 deg.
    # This comes from the misconception where apps treat identity quaternions as "floor" while domains are "wall".
    # This should be changed in the DMT recorder to capture same as domain format correctly.
    rot90 = pycolmap.Rotation3d(np.array([np.pi/2, 0.0, 0.0]))

    with open(file_path) as f:
        json_data = json.load(f)
        for entry in json_data["poses"]:
            pos = np.array([entry["px"], entry["py"], entry["pz"]])
            quat = np.array([entry["rx"], entry["ry"], entry["rz"], entry["rw"]])

            # quat = (pycolmap.Rotation3d(quat) * rot90).quat # TODO: remove later (see above)

            pos, quat = convert_pose_opengl_to_colmap(pos, quat)

            portal_poses[entry["short_id"]] = pycolmap.Rigid3d(pycolmap.Rotation3d(quat), pos)

    return portal_poses


def convert_pose_colmap_to_opengl(position, quaternion):
    # The math is symmetric.
    # Separate function for readability.
    return convert_pose_opengl_to_colmap(position, quaternion)


def get_data_paths(group_folder, logger_name=None):
    logger = logging.getLogger(logger_name)
    path_to_truth_portals = group_folder / "portals.json"
    if path_to_truth_portals.exists():
        truth_portal_poses = load_portals_json(group_folder / "portals.json")
    else:
        truth_portal_poses = None

    zip_list = group_folder.glob('**/*.zip')

    # These are manually removed by Robin as they have no Frames.csv
    unwanted_files = [
        "dmt_scan_2024-06-26_10-18-51.zip",
        "dmt_scan_2024-06-26_10-21-58.zip",
        "dmt_scan_2024-06-26_10-42-21.zip",
        "dmt_scan_2024-06-26_10-47-53.zip",
        "dmt_scan_2024-06-26_11-00-32.zip",
        "dmt_scan_2024-06-26_11-03-35.zip",
        "dmt_scan_2024-06-26_11-04-32.zip",
        "dmt_scan_2024-06-26_11-05-02.zip",
        "dmt_scan_2024-06-26_14-07-27.zip",
        "dmt_scan_2024-06-26_14-14-05.zip",
        "dmt_scan_2024-06-26_14-18-11.zip"
    ]

    dataset_paths = []
    zip_count = 0
    unwanted_count = 0
    for file in zip_list:
        if file.name not in unwanted_files:
            dataset_paths.append(file)
            zip_count += 1
        else:
            unwanted_count += 1
    logger.info(f"Found {zip_count} valid zip files, {unwanted_count} unwanted zip files skipped")

    subfolder_count = 0
    for subfolder in group_folder.iterdir():
        if subfolder.is_dir() and (
            subfolder.name.startswith("dmt_scan_")
            or subfolder.name.startswith("20")
        ):
            dataset_paths.append(subfolder)
            subfolder_count += 1
    logger.info(f"Found {subfolder_count} scan subfolders (not zip)")

    logger.info(f"Using in total {len(dataset_paths)} scans from folder '{group_folder.name}'")
    
    if truth_portal_poses:
        logger.info(f"Found {len(truth_portal_poses.keys())} truth portal poses: ")
        logger.info("\n".join(f"{id}: {value}" for id, value in truth_portal_poses.items()))
    else:
        logger.info("No truth provided (portals.json). Will skip comparison with ground truth.")

    return truth_portal_poses, dataset_paths


def load_qr_detections_csv(csv_path):
    detections_per_timestamp = {}
    with open(csv_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            timestamp = round(float(row[0]) * 1e9) # s to ns
            pose_values = [float(val) for val in row[2:9]] # px, py, pz, rx, ry, rz, rw
            pos = pose_values[:3]
            quat = pose_values[3:]

            pos, quat = convert_pose_opengl_to_colmap(pos, quat)

            rot3d = pycolmap.Rotation3d(np.array(quat))

            qr_pose = pycolmap.Rigid3d(
                rot3d,
                np.array(pos)
            )

            coordinates = [float(coord) for coord in row[9:]]

            detections_per_timestamp[timestamp] = {
                "pose": qr_pose,
                "short_id": row[1],
                "corners_wrt_image": [(coordinates[i], coordinates[i + 1]) for i in range(0, len(coordinates), 2)]
            }

    return detections_per_timestamp


def quaternion_to_rotation_matrix(q):
    x, y, z, w = q
    return np.array([
        [1 - 2*(y**2 + z**2), 2*(x*y - z*w), 2*(x*z + y*w)],
        [2*(x*y + z*w), 1 - 2*(x**2 + z**2), 2*(y*z - x*w)],
        [2*(x*z - y*w), 2*(y*z + x*w), 1 - 2*(x**2 + y**2)]
    ])


def average_rotation_matrices(rotation_matrices):
    R_avg = np.mean(rotation_matrices, axis=0)
    U, _, Vt = np.linalg.svd(R_avg)
    R_best_fit = np.dot(U, Vt)
    return R_best_fit


def rotation_matrix_to_quaternion(R):
    r = scipy_Rotation.from_matrix(R)
    return r.as_quat()


def average_quaternions_svd(quaternions):
    # Convert quaternions to rotation matrices
    rotation_matrices = np.array([quaternion_to_rotation_matrix(q) for q in quaternions])

    # Compute the average rotation matrix
    R_avg = average_rotation_matrices(rotation_matrices)

    # Convert the average rotation matrix back to a quaternion
    avg_quaternion = rotation_matrix_to_quaternion(R_avg)

    return avg_quaternion


def mean_pose(poses):
    if len(poses) == 1:
        return poses[0]

    pos = np.mean(np.array([pose.translation for pose in poses]), axis=0)
    #quat = np.array(poses[0].rotation.quat) # TODO select best fit for rotation also
    #quat_sum = np.sum(np.array([pose.rotation.quat for pose in poses]), axis=0)
    #quat = quat_sum / norm(quat_sum)
    quat = average_quaternions_svd(np.array([pose.rotation.quat for pose in poses]))
    return pycolmap.Rigid3d(pycolmap.Rotation3d(quat), pos)


def flatten_quaternion(quat):
    flat = quat.copy()
    flat[1] = 0.0
    flat[2] = 0.0
    flat /= norm(flat)
    return flat


def precompute_arkit_offsets(image_ids, arkit_cam_from_world_transforms, arkit_precomputed={}):
    for image_id in image_ids:
        arkit_cam_from_world = arkit_cam_from_world_transforms[image_id]

        # For first image just use same image instead of previous (to get a zero offset instead)
        prev_image_id = image_id - 1
        if prev_image_id not in image_ids:
            prev_image_id = image_id

        prev_arkit_cam_from_world = arkit_cam_from_world_transforms[prev_image_id]
        arkit_offset = arkit_cam_from_world * prev_arkit_cam_from_world.inverse()

        arkit_gravity_direction = np.matmul(arkit_cam_from_world.matrix(), np.array([-1.0, 0.0, 0.0, 0.0]).transpose())[:3]

        arkit_precomputed[image_id] = {
            "offset_moved": arkit_offset.translation,
            "offset_rotated": arkit_offset.rotation,
            "gravity_direction": arkit_gravity_direction,
            "cam_from_world": arkit_cam_from_world_transforms[image_id]
        }

    return arkit_precomputed


def get_world_space_qr_codes(reconstruction, detections_per_qr, image_ids_per_qr):
    
    qr_world_detections = {}

    for qr_id, cam_space_detections in detections_per_qr.items():
        qr_world_detections[qr_id] = []
        corresponding_image_ids = image_ids_per_qr[qr_id]

        for image_id, qr_pose_in_cam in zip(corresponding_image_ids, cam_space_detections):
            cam_pose = reconstruction.images[image_id].cam_from_world.inverse()
            qr_world_pose = cam_pose * qr_pose_in_cam
            qr_world_detections[qr_id].append(qr_world_pose)

    return qr_world_detections


def save_qr_poses_csv(poses_per_qr, csv_path):
    with open(csv_path, mode='w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        for short_id, qr_poses in poses_per_qr.items():
            for qr_pose in qr_poses:

                pos, quat = convert_pose_colmap_to_opengl(qr_pose.translation, qr_pose.rotation.quat)

                # Create a row for the CSV
                row = [
                    short_id,
                    pos[0], pos[1], pos[2],
                    quat[0], quat[1], quat[2], quat[3]
                ]

                # Write the row to the CSV file
                csv_writer.writerow(row)


def save_failed_manifest_json(json_path, job_root_path, job_status_details):
    save_manifest_json({}, json_path, job_root_path, job_status="failed", job_progress=100, job_status_details=job_status_details)


def save_manifest_json(portal_poses, json_path, job_root_path, job_status=None, job_progress=None, job_status_details=None):
    manifest_data = {
        "portals": [],
        "reconstructionServerVersion": VERSION,
        "jobStatus": job_status if job_status is not None else "unknown",
        "jobProgress": job_progress if job_progress is not None else 0,
        "jobStatusDetails": job_status_details if job_status_details is not None else "",
        "updatedAt": datetime.datetime.now().isoformat()
    }

    # Lots of try catch to just skip data that is not available but still keep the rest

    #-------------------------
    # JOB METADATA
    #-------------------------

    try:
        job_metadata_json_path = job_root_path / "job_metadata.json"
        if job_metadata_json_path.exists():
            job_metadata_json = json.load(open(job_metadata_json_path))

            created_datetime = datetime.datetime.fromisoformat(job_metadata_json["created_at"])
            
            manifest_data["createdAt"] = job_metadata_json["created_at"]
            manifest_data["jobDuration"] = float((datetime.datetime.now() - created_datetime).total_seconds())
            manifest_data["jobID"] = job_metadata_json["id"]
            manifest_data["jobName"] = job_metadata_json["name"]
            manifest_data["reconstructionServerURL"] = job_metadata_json.get("reconstruction_server_url", None)
            manifest_data["domainID"] = job_metadata_json["domain_id"]
            manifest_data["domainServerURL"] = job_metadata_json.get("domain_server_url", None)
            manifest_data["processingType"] = job_metadata_json["processing_type"]
            manifest_data["dataIDs"] = job_metadata_json["data_ids"]
    except:
        pass

    #-------------------------
    # SCAN DATA SUMMARY
    #-------------------------

    portal_sizes = {}
    try:
        scan_data_summary_path = job_root_path / "scan_data_summary.json"
        if scan_data_summary_path.exists():
            scan_data_summary = json.load(open(scan_data_summary_path))
            manifest_data["scanDataSummary"] = scan_data_summary
            for portal_id, portal_size in zip(scan_data_summary["portalIDs"], scan_data_summary["portalSizes"]):
                portal_sizes[portal_id] = portal_size
    except:
        pass

    #-------------------------
    # SERVER DETAILS
    #-------------------------

    manifest_data["serverDetails"] = {}

    try:
        manifest_data["serverDetails"]["os"] = platform.platform()
    except:
        pass

    try:
        manifest_data["serverDetails"]["cpu"] = {
            "model": platform.processor(),
            "cores": psutil.cpu_count(logical=False),
            "threads": psutil.cpu_count(logical=True),
            "load": psutil.cpu_percent(interval=1),
        }
    except:
        pass

    try:
        manifest_data["serverDetails"]["memory"] = {
            "total": psutil.virtual_memory().total,
            "available": psutil.virtual_memory().available,
            "used": psutil.virtual_memory().used,
            "usedPercent": psutil.virtual_memory().percent
        }
    except:
        pass
    
    try:
        if torch.cuda.is_available():
            manifest_data["serverDetails"]["cudaAvailable"] = True
            manifest_data["serverDetails"]["cudaVersion"] = torch.version.cuda
        else:
            manifest_data["serverDetails"]["cudaAvailable"] = False
    except:
        manifest_data["serverDetails"]["cudaAvailable"] = False
        pass

    try:
        manifest_data["serverDetails"]["gpus"] = [
            {
                "name": gpu.name,
                "memoryTotal": gpu.memoryTotal,
                "memoryUsed": gpu.memoryUsed,
                "load": gpu.load,
                "driver": gpu.driver,
            }
            for gpu in GPUtil.getGPUs()
        ] if len(GPUtil.getGPUs()) > 0 else [],
    except:
        pass

    #-------------------------
    # PORTALS
    #-------------------------

    # poses_for_qr has only one pose after refinement, but other parts of the code expects a list of poses per QR.
    # For now we just take the first
    for short_id, poses_for_qr in portal_poses.items():

        pose = poses_for_qr[0]
        pos, quat = convert_pose_colmap_to_opengl(pose.translation, pose.rotation.quat)

        manifest_data["portals"].append({
            "shortId": short_id,
            "pose": {
                "position": {
                    "x": pos[0],
                    "y": pos[1],
                    "z": pos[2],
                },
                "rotation": {
                    "x": quat[0],
                    "y": quat[1],
                    "z": quat[2],
                    "w": quat[3],
                }
            },
            "averagePose": {
                "position": {
                    "x": pos[0],
                    "y": pos[1],
                    "z": pos[2],
                },
                "rotation": {
                    "x": quat[0],
                    "y": quat[1],
                    "z": quat[2],
                    "w": quat[3],
                }
            },
            "physicalSize": portal_sizes.get(short_id, None)
        })

    #-------------------------

    with open(json_path, 'w') as json_file:
        json.dump(manifest_data, json_file, indent=4)


def vec3_angle(v, w):
    value = v.dot(w)/(norm(v)*norm(w))

    # Rounding errors can be slightly outside range of arccos. Clamp to range.
    if value > 1.0 or value < -1.0:
        if np.abs(value) < 1.000001:
            value = np.clip(value, -1, 1)
        else:
            raise Exception(f"BAD VALUE in arccos: {value}. Must be within -1 .. 1 range")

    return rad2deg(arccos(value))


def sorting_key(img):
    return img.image_id


def get_sorted_images(images):
    sorted_images = list(images)
    sorted_images.sort(key=sorting_key)
    return sorted_images


def mp4_to_frames(mp4_path, frames_path, filename_prefix=""):
    capture = cv2.VideoCapture(mp4_path)
    frame_count = 0
    print("Unpacking mp4 to frames:", mp4_path, "->", frames_path)
    while capture.isOpened():
        ret, frame = capture.read()
        if not ret:
            break
        cv2.imwrite(f"{frames_path}/{filename_prefix}{frame_count:06d}.jpg", frame)
        frame_count += 1
    print(f"Unpacked {frame_count} frames from mp4")
    capture.release()


def export_rec_as_ply(rec, path, convert_to_opengl=True, logger_name=""):
    logger = logging.getLogger(logger_name)

    logger.info(f"Converting reconstruction with {len(rec.points3D)} points to PLY: {path}")
    logger.info(f"convert_to_opengl = {convert_to_opengl}")
    logger.info("...")
    # As text for now, as mobile DMT doesn't work with binary domain data blobs
    rec_openGL = pycolmap.Reconstruction()
    for point in rec.points3D.values():
        x,y,z = point.xyz
        if convert_to_opengl:
            x,y,z = y,x,-z
        _ = rec_openGL.add_point3D(np.array([x,y,z]), pycolmap.Track(), point.color)
    export_ply_text(rec_openGL, str(path))
    logger.info(f"PLY export done")


def evaluate_scanned_qr_codes(qr_world_detections, measure_pairs=None, truth_pairs=None):
    
    print()
    for short_id, poses in qr_world_detections.items():
        #print("poses", poses)
        positions = [pose.translation for pose in poses]
        up_vecs = [pose.rotation * np.array([1.0, 0.0, 0.0]) for pose in poses]
        right_vecs = [pose.rotation * np.array([0.0, 1.0, 0.0]) for pose in poses]

        pos_deviation = np.mean(np.std(np.array(positions), axis=0))
        up_deviation = np.mean(np.std(np.array(up_vecs), axis=0))
        right_deviation = np.mean(np.std(np.array(right_vecs), axis=0))
        #print(up_vecs)
        print(f"{short_id}: pos_deviation {pos_deviation}, up_deviation {up_deviation}, right_deviation {right_deviation}")
        #print(positions)
        #print("STD DEV:", std_deviation)

    all_heights = []
    for qr_id, poses in qr_world_detections.items():
        for pose in poses:
            all_heights.append(pose.translation[0])
    print(all_heights)
    print("Average height:", np.mean(all_heights))
    print("Height deviation:", np.std(all_heights))

    if measure_pairs is not None:

        for i, pair in enumerate(measure_pairs):
            a, b = measure_pairs[i]
            pos1 = qr_world_detections[a][0].translation
            pos2 = qr_world_detections[b][0].translation
            offset = pos1 - pos2
            offset[0] = 0 # Snap floor height
            distances = []
            for pose_a in qr_world_detections[a]:
                for pose_b in qr_world_detections[b]:
                    distances.append(norm(pose_a.translation - pose_b.translation))
            percent_vs_truth = (norm(offset) / truth_pairs[i] - 1) * 100
            print(f"{a} - {b}: {norm(offset):.4f},"
                  f"{'+' if percent_vs_truth > 0 else ''}{percent_vs_truth:.2f}%,",
                  f"{'+' if percent_vs_truth > 0 else ''}{(norm(offset) - truth_pairs[i]) * 100.0:.2f} cm,",
                  f"(truth:{truth_pairs[i]:.5f}). (spread {np.std(distances):.4f})")
            

def pycolmap_to_batch_matrix(
    reconstruction, device="cuda", camera_type="SIMPLE_PINHOLE"
):
    """
    Convert a PyCOLMAP Reconstruction Object to batched PyTorch tensors.

    Args:
        reconstruction (pycolmap.Reconstruction): The reconstruction object from PyCOLMAP.
        device (str): The device to place the tensors on (default: "cuda").
        camera_type (str): The type of camera model used (default: "SIMPLE_PINHOLE").

    Returns:
        tuple: A tuple containing points3D, extrinsics, intrinsics, and optionally extra_params.
    """

    num_images = len(reconstruction.images)
    max_points3D_id = max(reconstruction.point3D_ids())
    points3D = np.zeros((max_points3D_id, 3))

    for point3D_id in reconstruction.points3D:
        points3D[point3D_id - 1] = reconstruction.points3D[point3D_id].xyz
    points3D = torch.from_numpy(points3D).to(device)

    extrinsics = []
    intrinsics = []

    extra_params = [] if camera_type == "SIMPLE_RADIAL" else None

    for i in range(num_images):
        # Extract and append extrinsics
        pyimg = reconstruction.images[i]
        pycam = reconstruction.cameras[pyimg.camera_id]
        matrix = pyimg.cam_from_world.matrix()
        extrinsics.append(matrix)

        # Extract and append intrinsics
        calibration_matrix = pycam.calibration_matrix()
        intrinsics.append(calibration_matrix)

        if camera_type == "SIMPLE_RADIAL":
            extra_params.append(pycam.params[-1])

    # Convert lists to torch tensors
    extrinsics = torch.from_numpy(np.stack(extrinsics)).to(device)

    intrinsics = torch.from_numpy(np.stack(intrinsics)).to(device)

    if camera_type == "SIMPLE_RADIAL":
        extra_params = torch.from_numpy(np.stack(extra_params)).to(device)
        extra_params = extra_params[:, None]

    return points3D, extrinsics, intrinsics, extra_params


class JsonFormatter(logging.Formatter):
    """Formatter to dump error message into JSON"""

    def __init__(self, domain_id, job_id, dataset_id = None, fmt = None, datefmt = None, style = "%", validate = True):
        super().__init__(fmt, datefmt, style, validate)
        self.domain_id = domain_id
        self.job_id = job_id
        self.dataset_id = dataset_id

    def format(self, record: logging.LogRecord) -> str:
        t = time.strftime(self.datefmt, time.gmtime(record.created))
        s = '%s.%09dZ' % (t, record.msecs*1e6)
        if self.dataset_id:
            record_dict = {
                "time": s,
                "level": record.levelname.lower(),
                "name": record.name,
                "tags": {
                    "domain_id": self.domain_id, 
                    "job_id": self.job_id, 
                    "dataset_id": self.dataset_id},
                "message": record.getMessage()
            }
        else: 
            record_dict = {
                "time": s,
                "level": record.levelname.lower(),
                "name": record.name,
                "tags": {
                    "domain_id": self.domain_id, 
                    "job_id": self.job_id},
                "message": record.getMessage()
            }
        return json.dumps(record_dict)


def setup_logger(name=None, log_file=None, domain_id="", job_id="", dataset_id=None, level="INFO"):
    """To setup as many loggers as you want"""

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()),)
    
    if log_file:
        logger, _ = add_file_handler(logger, log_file)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(JsonFormatter(datefmt='%Y-%m-%dT%H:%M:%S',
        domain_id=domain_id, job_id=job_id, dataset_id=dataset_id))
    logger.addHandler(console_handler)

    return logger


def add_file_handler(logger, log_file):
    file_formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s %(message)s')   
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    return logger, file_handler