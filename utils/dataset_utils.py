from typing import Iterable, List
import pycolmap
import os 
from pathlib import Path
import zipfile
import shutil
import csv
import numpy as np 
from numpy.linalg import norm
import logging
from src.ply_export import export_ply_text

from evo.main_ape import ape as evo_ape
from evo.core.trajectory import PosePath3D
from evo.core.trajectory import geometry
from evo.core.metrics import PoseRelation
from evo.core import lie_algebra as evo_lie
import matplotlib.pyplot as plt

from utils.data_utils import (
    load_qr_detections_csv, 
    mean_pose,
    mp4_to_frames,
    flatten_quaternion, 
    convert_pose_opengl_to_colmap, 
    precompute_arkit_offsets, 
    get_world_space_qr_codes,
    save_manifest_json
)
from utils.geometry_utils import align_reconstruction_chunks, run_stitching


class NoOverlapException(Exception):
    def __init__(self, message='No overlaps!'):
        # Call the base class constructor with the parameters it needs
        super(NoOverlapException, self).__init__(message)

floor_origin_portal_pose_GL = pycolmap.Rigid3d(
    pycolmap.Rotation3d(np.array([-0.7071068, 0.0, 0.0, 0.7071068])),
    np.array([0.0, 0.0, 0.0]))
p, q = convert_pose_opengl_to_colmap(np.array([0.0, 0.0, 0.0]), np.array([-0.7071068, 0.0, 0.0, 0.7071068]))
floor_origin_portal_pose = pycolmap.Rigid3d(pycolmap.Rotation3d(q), p)

def load_partial(
    unzip_folder, 
    dataset_dir, 
    dataset_group, 
    truth_portal_poses, 
    next_image_id, 
    placed_portal, 
    partial_rec_dir, 
    combined_rec, 
    timestamp_per_image, 
    arkit_precomputed, 
    detections_per_qr, 
    image_ids_per_qr, 
    chunks_image_ids, 
    all_observations=True, 
    all_poses=True, 
    gt_observations=False, 
    with_3dpoints=False
):
    experiment_name = unzip_folder.name

    dataset = unzip_folder # TODO will remove this as we can use scan_folder_path all along

    images = unzip_folder / 'Frames/'
    
    frames_mp4 = unzip_folder / 'Frames.mp4'
    print("Looking for mp4 encoded frames: ", frames_mp4)
    use_frames_from_video = False
    if frames_mp4.exists():
        print("Frames mp4 found, unpacking into", images)
        if not images.exists():
            images.mkdir()
        mp4_to_frames(frames_mp4, images, filename_prefix=experiment_name + "_")
        use_frames_from_video = True


    outputs = Path(os.path.join(dataset_dir.parent, "refined/global"))
    if dataset_group is not None:
        outputs = outputs / dataset_group

    outputs = outputs / experiment_name
    if outputs.exists():
        shutil.rmtree(outputs.as_posix())

    #--------------------
    # RGB Frames
    references = [str(p.relative_to(images)) for p in (images).iterdir()]
    references = sorted(references) # Assume file name is time stamp, to get chronological sequence

    print(len(references), "frames selected")
    if len(references) < 20:
        print("TOO FEW IMAGES! Skipping short dataset")
        return next_image_id, placed_portal, partial_rec_dir, combined_rec, timestamp_per_image, \
               arkit_precomputed, detections_per_qr, image_ids_per_qr, chunks_image_ids
    

    #--------------------
    # Frames Metadata

    frames_csv_path = dataset / "Frames.csv"
    if not frames_csv_path.exists():
        print("Dataset has no Frames.csv. SKIPPING!")
        return next_image_id, placed_portal, partial_rec_dir, combined_rec, timestamp_per_image, \
               arkit_precomputed, detections_per_qr, image_ids_per_qr, chunks_image_ids

    frames_csv_path = str(frames_csv_path)

    print("Loading image timestamps from", frames_csv_path, "...")

    # Read and process the CSV file
    timestamp_per_image_chunk = {}
    with open(frames_csv_path, newline='') as csvfile:
        frame_index = 0
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            timestamp = round(float(row[0]) * 1e9) # s to ns
            if use_frames_from_video:
                filename = f"{experiment_name}_{frame_index:06d}.jpg" # Match with how frames are unpacked to images, by mp4_to_frames
            else:
                filename = row[1]
            frame_index += 1

            timestamp_per_image_chunk[filename] = timestamp
    print(len(timestamp_per_image_chunk), "frame timestamps loaded")


    #--------------------
    # Cam Intrinsics

    cam_intrinsics_csv_path = str(dataset / "CameraIntrinsics.csv")

    print("Loading camera intrinsics from", cam_intrinsics_csv_path, "...")

    # Read and process the CSV file
    intrinsics_per_timestamp = {}
    with open(cam_intrinsics_csv_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            timestamp = round(float(row[0]) * 1e9) # s to ns
            intrinsics_per_timestamp[timestamp] = [
                float(row[1]), float(row[2]), # focal distance (fx, fy)
                float(row[3]), float(row[4]), # principal point (cx, cy)
                int(row[5]), int(row[6])      # image resolution (w, h)
            ]
    print(len(intrinsics_per_timestamp), "camera frame intrinsics loaded")


    #--------------------
    # Unrefined Poses

    ar_poses_csv_path = str(dataset / "ARposes.csv")

    print("Loading unrefined AR poses from", ar_poses_csv_path, "...")

    # Read and process the CSV file
    ar_poses_per_timestamp = {}
    with open(ar_poses_csv_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            timestamp = round(float(row[0]) * 1e9) # s to ns
            # px, py, pz, rx, ry, rz, rw
            ar_poses_per_timestamp[timestamp] = [float(val) for val in row[1:8]] 
    print(len(ar_poses_per_timestamp), "AR poses loaded")


    def arkit_world_from_cam(timestampNs) -> pycolmap.Rigid3d:
        ar_pose = ar_poses_per_timestamp[timestampNs]
        position = ar_pose[0:3]
        rotation = ar_pose[3:7]

        position, rotation = convert_pose_opengl_to_colmap(position, rotation)
        return pycolmap.Rigid3d(pycolmap.Rotation3d(rotation), position)

    #--------------------
    # QR detections
    qr_detections_csv_path = dataset / "PortalDetections.csv"
    if not qr_detections_csv_path.exists() and (dataset / "Observations.csv").exists():
        qr_detections_csv_path = dataset / "Observations.csv"
        print("WARNING: PortalDetections.csv not found, but found Observations.csv (old filename convention).")
    qr_detections_csv_path = str(qr_detections_csv_path)
    
    print("Loading QR detections from", qr_detections_csv_path, "...")

    # Read and process the CSV file
    qr_detections_per_timestamp = load_qr_detections_csv(qr_detections_csv_path)
    # unique_qr = set(value['short_id'] for value in qr_detections_per_timestamp.values())
    print(len(qr_detections_per_timestamp), "QR detections loaded")

    # Start with the known camera intrinsics and cam poses from ARKit, or from an already refined chunk.

    loaded_rec = None
    if partial_rec_dir is not None and partial_rec_dir.exists():
        loaded_rec = pycolmap.Reconstruction()
        loaded_rec.read(partial_rec_dir)
        print("Using loaded refined reconstruction from", partial_rec_dir)

    if loaded_rec is not None:
        # Recalculate QR detections' world poses using refined camera poses

        failed_timestamps = []
        for timestamp, detection in qr_detections_per_timestamp.items():
            all_timestamps_before = [t for t in timestamp_per_image_chunk.values() if t <= timestamp]
            if not all_timestamps_before:
                failed_timestamps.append(timestamp)
                continue

            nearest_image_timestamp = np.max(all_timestamps_before)
            nearest_image_names = [n for n, t in timestamp_per_image_chunk.items() if t == nearest_image_timestamp]
            if len(nearest_image_names) == 0:
                failed_timestamps.append(timestamp)
                continue

            nearest_image = [image for image in loaded_rec.images.values() if image.name == nearest_image_names[0]]
            if len(nearest_image) == 0:
                failed_timestamps.append(timestamp)
                continue
            nearest_image = nearest_image[0]

            cam_space_qr_pose = arkit_world_from_cam(nearest_image_timestamp).inverse() * detection["pose"]

            detection["pose"] = nearest_image.cam_from_world.inverse() * cam_space_qr_pose

        for failed_ts in failed_timestamps:
            qr_detections_per_timestamp.pop(failed_ts, None)

    
    #----------------------
    # Align with already placed portals
    this_chunk_detections_per_qr = {}
    for detection in qr_detections_per_timestamp.values():
        id = detection["short_id"]

        if id not in this_chunk_detections_per_qr.keys():
            this_chunk_detections_per_qr[id] = [detection["pose"]]
        else:
            this_chunk_detections_per_qr[id].append(detection["pose"])

    if gt_observations:
        this_chunk_mean_qr_poses = {qr_id: truth_portal_poses[qr_id] for qr_id, poses in this_chunk_detections_per_qr.items()}
    else:
        this_chunk_mean_qr_poses = {qr_id: mean_pose(poses) for qr_id, poses in this_chunk_detections_per_qr.items()}
    print("There are", len(this_chunk_mean_qr_poses.keys()), "of unique QR codes")
    

    # Find all overlapping portal poses
    target_poses = {
        qr_id: placed_portal[qr_id]
        for qr_id in this_chunk_mean_qr_poses.keys()
        if qr_id in placed_portal.keys()
    }
    has_overlap = len(target_poses) > 0

    is_first_chunk = len(placed_portal) == 0
    print(f"Portals already placed: {len(placed_portal)}.",
          "FIRST CHUNK -> put origin portal" if is_first_chunk else "NOT FIRST -> align based on ovelapping portals.")

    if not has_overlap and not is_first_chunk:
        raise NoOverlapException  # handled outside, to retry again after other chunks are added

    for filename, timestamp in timestamp_per_image_chunk.items():
        assert filename not in timestamp_per_image
        timestamp_per_image[filename] = timestamp

    if has_overlap:
        qr_ids = target_poses.keys()
        positions = np.array([this_chunk_mean_qr_poses[qr_id].translation for qr_id in qr_ids])
        placed_positions = np.array([target_poses[qr_id].translation for qr_id in qr_ids])

        overlap_ids = list(qr_ids)
        alignment_transforms = []
        for overlap_qr_id in overlap_ids:
            transform = target_poses[overlap_qr_id] * this_chunk_mean_qr_poses[overlap_qr_id].inverse()
            alignment_transforms.append(transform)

        alignment_transform = mean_pose(alignment_transforms)
        print(f"TRANSFORM: Aligning with overlapping QR(s): ({overlap_ids})")
        print(alignment_transform)

    elif is_first_chunk:
        origin_portal_id = list(this_chunk_mean_qr_poses.keys())[0]
        print("SET ORIGIN PORTAL:", origin_portal_id)
        alignment_transform = floor_origin_portal_pose * this_chunk_mean_qr_poses[origin_portal_id].inverse()
        print(f"TRANSFORM: Aligning origin portal to zero using single QR overlapping QR.")
        print(alignment_transform)


    for qr_id, pose in this_chunk_mean_qr_poses.items():
        if alignment_transform is not None:
            pose = alignment_transform * pose
        placed_portal[qr_id] = pose

    if alignment_transform is not None:
        for timestamp, detection in qr_detections_per_timestamp.items():
            detection["pose"] = alignment_transform * detection["pose"]


    #----------------------
    # Init unrefined Reconstruction

    selected_timestamps_per_qr = {}
    prev_qr_id = None
    for timestamp, detection in qr_detections_per_timestamp.items():
        qr_id = detection["short_id"]
        if not all_observations and qr_id == prev_qr_id:  # Not sure what this does
            continue
        if qr_id not in selected_timestamps_per_qr:
            selected_timestamps_per_qr[qr_id] = [timestamp]
        else:
            selected_timestamps_per_qr[qr_id].append(timestamp)
        prev_qr_id = qr_id

    selected_timestamps_per_qr = {qr_id: timestamps[:] for qr_id, timestamps in selected_timestamps_per_qr.items()}

    # Snap each QR detection to an image so we can re-apply the same relative offset to the refined pose of that image.
    # For each QR detection, snap to the nearest image right before.
    # Since in recorder we detect at higher FPS than we capture RGB at, we get detections "in between" RGB frames too.
    sorted_image_timestamps = sorted(list([
        timestamp_per_image[Path(ref).name] for ref in references
        if Path(ref).name in timestamp_per_image.keys()
    ]))

    timestamp_mappings_image_detection = []
    #index_of_start_timestamp = sorted_image_timestamps.index(chunk_start_time)
    for qr_id, detection_timestamps in selected_timestamps_per_qr.items():
        for detection_timestamp in detection_timestamps:

            # Only loop through timestamps of this chunk.
            # sorted_image_timestamps has all previous aligned chunks already.
            nearest_image_timestamp = 0
            for t in sorted_image_timestamps:
                if t > detection_timestamp:
                    break
                nearest_image_timestamp = t

            if nearest_image_timestamp <= 0:
                continue

            timestamp_mappings_image_detection.append({
                "image_timestamp": nearest_image_timestamp,
                "detection_timestamp": detection_timestamp
            })

    image_timestamps_with_detection = [mapping["image_timestamp"] for mapping in timestamp_mappings_image_detection]
    print("image_timestamps_with_detection: ", image_timestamps_with_detection)

    image_name_per_timestamp = {timestamp: image_name for image_name, timestamp in timestamp_per_image.items()}
    image_per_timestamp = {}

    if loaded_rec is not None and alignment_transform is not None:
        loaded_rec.transform(pycolmap.Sim3d(1.0, alignment_transform.rotation.quat, alignment_transform.translation))

    rec = pycolmap.Reconstruction()
    arkit_cam_from_world_transforms = {}
    image_id_old_to_new = {}
    for i, ref in enumerate(references):
        image_filename = Path(ref).name

        if image_filename not in timestamp_per_image.keys():
            continue

        timestampNs = timestamp_per_image[image_filename]

        if not all_poses and timestampNs not in image_timestamps_with_detection and i % 3 != 0:
            continue


        image_id = next_image_id
        camera_id = image_id # always 1-to-1 for us

        print("Camera id: ", camera_id)

        if loaded_rec:
            matching_cams = [c for c in loaded_rec.cameras.values() if loaded_rec.images[c.camera_id].name == image_filename]
            if not matching_cams:
                continue

            loaded_cam = matching_cams[0]
            cam = pycolmap.Camera(
                model=loaded_cam.model,
                width=loaded_cam.width,
                height=loaded_cam.height,
                params=loaded_cam.params,
                camera_id=camera_id
            ) # Use new camera ID when combining many scans!

            cam_to_world = loaded_rec.images[loaded_cam.camera_id].cam_from_world.inverse()

            loaded_image = loaded_rec.find_image_with_name(image_filename)
            assert loaded_image is not None
        else:
            intrinsics = intrinsics_per_timestamp[timestampNs]
            ar_pose = ar_poses_per_timestamp[timestampNs]
            fx, fy, cx, cy, w, h = intrinsics

            if fx == fy: # TODO What about simple radial?
                model = 'SIMPLE_PINHOLE'
                params = [fx, cx, cy]
            else:
                model = 'PINHOLE'
                params = [fx, fy, cx, cy]

            cam = pycolmap.Camera(model=model, width=w, height=h, params=params, camera_id=camera_id)

            position = ar_pose[0:3]
            rotation = ar_pose[3:7]

            position, rotation = convert_pose_opengl_to_colmap(position, rotation)
            cam_to_world = pycolmap.Rigid3d(pycolmap.Rotation3d(rotation), position) 
        
            if alignment_transform is not None:
                cam_to_world = alignment_transform * cam_to_world

        rec.add_camera(cam)
        combined_rec.add_camera(cam)

        cam_from_world = cam_to_world.inverse() # TODO tgus should be rename to world_to_cam?

        print(f"{timestampNs} @ Cam {camera_id}: {cam.width}x{cam.height}, {cam.model} params {cam.params} at pos=({cam_to_world.translation}) rot=({cam_to_world.rotation.quat})")

        arkit_world_from_cam_transform = arkit_world_from_cam(timestampNs)
        if alignment_transform is not None:
            arkit_world_from_cam_transform = alignment_transform * arkit_world_from_cam_transform
        arkit_cam_from_world_transforms[image_id] = arkit_world_from_cam_transform.inverse()

        if loaded_rec is None or not with_3dpoints:
            list_point_2d = []
        else:
            list_point_2d = [pycolmap.Point2D(pt2d.xy) for pt2d in loaded_image.points2D ]
        img = pycolmap.Image(image_filename, pycolmap.ListPoint2D(list_point_2d), cam_from_world, camera_id, image_id)
        image_per_timestamp[timestampNs] = img
        rec.add_image(img)
        rec.register_image(image_id)

        combined_rec.add_image(img)
        combined_rec.register_image(image_id)

        if loaded_rec is not None:
            assert image_id not in image_id_old_to_new
            image_id_old_to_new[loaded_image.image_id] = image_id

        next_image_id += 1

    if loaded_rec is not None and with_3dpoints:
        for point3D in loaded_rec.points3D.values():
            point3D_id_new = combined_rec.add_point3D(point3D.xyz, pycolmap.Track(), point3D.color)
            point3D_track = point3D.track
            for element in point3D_track.elements:
                element.image_id = image_id_old_to_new[element.image_id]
                combined_rec.add_observation(point3D_id_new, element)

    # unrefined_sfm_dir = outputs / 'unrefined_sfm'
    # Path.mkdir(unrefined_sfm_dir, parents=True, exist_ok=True)
    # rec.write(unrefined_sfm_dir) # TODO Why write to so early?


    ############################
    # PRE-PROCESSING
    ############################

    # SORT images (since order may be wrong in captured dataset)
    sorted_image_ids = list(rec.images.keys())
    sorted_image_ids.sort()

    chunks_image_ids.append(sorted_image_ids)

    # PRE-COMPUTE some offsets & gravity from the unrefined ARKit poses, which will remain constant during refinement.
    # These are used in the loss function to guide the refinement, not to diverge too far off from original.
    arkit_precomputed = precompute_arkit_offsets(sorted_image_ids, arkit_cam_from_world_transforms, arkit_precomputed) # skip first since these are offsets to previous image

    # PRE-LOAD QR DATA FOR LOOP CLOSURE
    valid_timestamps = image_per_timestamp.keys()
    for timestamp_mapping in timestamp_mappings_image_detection:
        detection = qr_detections_per_timestamp[timestamp_mapping["detection_timestamp"]]
        id = detection["short_id"]

        #print(f"QR @ {timestamp} ns, nearest image: {nearest_image}, cam space pos: {cam_space_qr_pose}")

        if id not in detections_per_qr.keys():
            detections_per_qr[id] = []
        if id not in image_ids_per_qr.keys():
            image_ids_per_qr[id] = []

        # Convert back into cam space of nearest image frame (since we skip some frames)
        all_timestamps_before = [t for t in valid_timestamps if t <= timestamp_mapping["detection_timestamp"]]
        if not all_timestamps_before:
            continue # Not sure why this can happen. Two captures crashed without this

        nearest_image_timestamp = np.max(all_timestamps_before)
        nearest_image = image_per_timestamp[nearest_image_timestamp]
        if gt_observations:
            cam_space_qr_pose = nearest_image.cam_from_world * truth_portal_poses[detection["short_id"]]
        else:
            cam_space_qr_pose = nearest_image.cam_from_world * detection["pose"]

        # Averaged observation poses test test
        # cam_space_qr_pose = nearest_image.cam_from_world * mean_pose([qr_detections_per_timestamp[ts]['pose'] for ts in selected_timestamps_per_qr[id]])

        # Offset/noisy GT poses test
        # true_pose = truth_portal_poses[detection["short_id"]]
        # cam_space_qr_pose = nearest_image.cam_from_world * pycolmap.Rigid3d(np.eye(3), np.array((0.0, 0.0, 0.05))) * true_pose
        # cam_space_qr_pose = nearest_image.cam_from_world * pycolmap.Rigid3d(np.eye(3), np.random.uniform(low=-0.01, high=0.01, size=(3,))) * true_pose

        if nearest_image.image_id in image_ids_per_qr[id]:
            print('WARNING: Double observation of the same QR code in one image!')
            continue

        detections_per_qr[id].append(cam_space_qr_pose)
        image_ids_per_qr[id].append(nearest_image.image_id)

        # Generated "GT" camera track test
        # image_id = next_image_id
        # camera_id = image_id

        # cam = combined_rec.cameras[nearest_image.camera_id]
        # cam = pycolmap.Camera(camera_id=camera_id, model=cam.model, width=cam.width, height=cam.height, params=cam.params)
        # combined_rec.add_camera(cam)

        # img = pycolmap.Image(detection["short_id"] + '_' + str(nearest_image.image_id), pycolmap.ListPoint2D([]), truth_portal_poses[detection["short_id"]].inverse(), camera_id, image_id)
        # combined_rec.add_image(img)
        # combined_rec.register_image(image_id)

        # next_image_id += 1

        # detections_per_qr[id].append(pycolmap.Rigid3d())
        # image_ids_per_qr[id].append(image_id)

    return next_image_id, placed_portal, partial_rec_dir, combined_rec, \
           timestamp_per_image, arkit_precomputed, detections_per_qr, image_ids_per_qr, chunks_image_ids


def portals_to_evo_path(pose_per_qr, flatten=False):
    positions_xyz = []
    quats_wxyz = []
    for qr_id, pose in pose_per_qr.items():
        if not isinstance(pose, pycolmap.Rigid3d):
            raise Exception(f"Wrong value type for pose of QR {qr_id}, in portals_to_evo_path. Must be pycolmap.Rigid3d, got: {pose}")

        positions_xyz.append(np.array([
            0.0 if flatten else pose.translation[0],
            pose.translation[1],
            pose.translation[2]
        ]))

        quat = np.array([
            pose.rotation.quat[3], # Evo library uses WXYZ !!!
            pose.rotation.quat[0],
            0.0 if flatten else pose.rotation.quat[1],
            0.0 if flatten else pose.rotation.quat[2]
        ])
        if flatten:
            quat /= norm(quat)

        quats_wxyz.append(quat)

    return PosePath3D(positions_xyz, quats_wxyz)


def compare_portals(initial, estimate, reference, align=False, correct_scale=False, verbose=False):

    filtered_reference = {qr_id: reference[qr_id] for qr_id in estimate.keys()}

    ini_pose_path = portals_to_evo_path(initial, flatten=True)
    est_pose_path = portals_to_evo_path(estimate, flatten=True)
    ref_pose_path = portals_to_evo_path(filtered_reference, flatten=True)

    if verbose:
        print("Initial:", ini_pose_path)
        print(", ".join(f"{qr_id}: {initial[qr_id].rotation.quat}" for qr_id in initial))
        print("Estimate:", est_pose_path)
        print(", ".join(f"{qr_id}: {estimate[qr_id].rotation.quat}" for qr_id in estimate))
        print("Reference:", ref_pose_path)
        print(", ".join(f"{qr_id}: {filtered_reference[qr_id].rotation.quat}" for qr_id in filtered_reference))
        print("")

    if align or correct_scale:
        # ONLY rotate around world up (don't rely on alignment to fix height drift)
        # Load again temporarily to flatten and compute alignment.
        # Then apply alignment on original paths which we DON'T flatten.
        # This gives a more fair measurement and also works with wall portals
        """
        def flatten(points):
            return np.array([np.array([0.0, p[1], p[2]]) for p in points])

        rotation, translation, scaling = geometry.umeyama_alignment(flatten(est_pose_path.positions_xyz).T,
                                                                    flatten(ref_pose_path.positions_xyz).T,
                                                                    correct_scale)


        #print(f"Umeyama: translation={translation},\nrotation=\n{rotation},\nscaling={scaling}")

        if correct_scale:
            est_pose_path.scale(scaling)
        if align:
            est_pose_path.transform(evo_lie.se3(rotation, translation))

            # Align again without flattening, to get also the height right (but not rotating again)
            _, translation_2, scaling_2 = geometry.umeyama_alignment(est_pose_path.positions_xyz.T,
                                                                     ref_pose_path.positions_xyz.T,
                                                                     correct_scale)

            #print(f"Umeyama 2: translation={translation_2},\nscaling={scaling_2}")
            if correct_scale:
                est_pose_path.scale(scaling_2)
            if align:
                est_pose_path.transform(evo_lie.se3(np.identity(3), translation_2))
        """


        rotation, translation, scaling = geometry.umeyama_alignment(est_pose_path.positions_xyz.T,
                                                                    ref_pose_path.positions_xyz.T,
                                                                    correct_scale)
        ini_pose_path.scale(scaling)
        ini_pose_path.transform(evo_lie.se3(rotation, translation))


    pos_comparison = evo_ape(ref_pose_path, est_pose_path, PoseRelation.point_distance,
                             align=align, correct_scale=correct_scale)

    rot_comparison = evo_ape(ref_pose_path, est_pose_path, PoseRelation.rotation_angle_deg,
                             align=align, correct_scale=correct_scale)

    if verbose:
        print(pos_comparison.pretty_str())
        print(rot_comparison.pretty_str())
        
        # fig = plt.figure()
        # traj_by_label = {
        #     "estimate": est_pose_path,
        #     "reference": ref_pose_path
        # }
        # evo_plot.trajectories(fig, traj_by_label, evo_plot.PlotMode.yz)
        

        # Scatter plot to compare portal poses
        fig = plt.figure(figsize=(14, 9))
        ax = fig.add_subplot(111)

        colors = plt.get_cmap('tab10').colors
        color_0 = np.array(colors[0]).reshape(1,-1)
        color_1 = np.array(colors[1]).reshape(1,-1)
        color_2 = np.array(colors[2]).reshape(1,-1)

        ax.scatter(ini_pose_path.positions_xyz[:, 1], ini_pose_path.positions_xyz[:, 2], label='initial',
                   c=color_0, marker="x", s=30)

        ax.scatter(est_pose_path.positions_xyz[:, 1], est_pose_path.positions_xyz[:, 2], label='optimized',
                   c=color_1, marker="x", s=15)

        ax.scatter(ref_pose_path.positions_xyz[:, 1], ref_pose_path.positions_xyz[:, 2], label='measured truth',
                   c=color_2, marker="x", s=15)

        ax.set_xlabel('Y axis')
        ax.set_ylabel('Z axis')
        ax.legend()
        plt.show()

    if verbose:
        print()
        print("Absolute Position Error (m):", pos_comparison.stats)
        print("Absolute Rotation Error (°):", rot_comparison.stats)

    print(f"Portal Accuracy (APE):",
          f" RMSE: {pos_comparison.stats['rmse']:.5f} m, {rot_comparison.stats['rmse']:.5f}°",
          f"  Max: {pos_comparison.stats['max']:.5f} m, {rot_comparison.stats['max']:.5f}°")

    return pos_comparison, rot_comparison


def stitching_helper(
    dataset_paths, 
    dataset_group, 
    group_folder, 
    truth_portal_poses, 
    all_observations=True, 
    all_poses=True, 
    use_refined_outputs=False, 
    with_3dpoints=False, 
    basic_stitch_only=False
):
    # Create and configure logger
    parent_dir = group_folder.parent
    output_path = parent_dir / "refined" / "global"
    os.makedirs(output_path, exist_ok=True)
    logging.basicConfig(filename=str(output_path) + "/global_logs",
                        format='%(asctime)s %(message)s',
                        level=logging.INFO,
                        filemode='a')

    # Creating an object
    logger = logging.getLogger()
    print(f'Working on {str(parent_dir.name)}')


    # init
    detections_per_qr = {}
    image_ids_per_qr = {}
    timestamp_per_image = {}
    arkit_precomputed = {}
    placed_portal = {}
    timestamp_per_image = {}
    chunks_image_ids = []
    combined_rec = pycolmap.Reconstruction()
    
    next_image_id = 1
    datasets_already_aligned = []
    datasets_to_align = dataset_paths.copy() # TODO Why copy? Can just loop through the list?
    consecutive_alignment_fails = 0

    refined_group_dir = parent_dir / "refined"
    os.makedirs(refined_group_dir, exist_ok=True)

    dataset_dir = parent_dir / "datasets"
    os.makedirs(dataset_dir, exist_ok=True)

    np.set_printoptions(precision=10, suppress=True, sign=' ')

    while datasets_to_align:
        dataset_path = datasets_to_align.pop(0)
        scan_name = dataset_path.stem

        if dataset_path.suffix.lower() == ".zip":
                unzip_folder = Path(os.path.join(dataset_dir, scan_name))
                if not unzip_folder.exists():
                    print(f"{unzip_folder} not existed... Unzipping dataset: {dataset_path}")
                    with zipfile.ZipFile(dataset_path, 'r') as zip_ref:
                        zip_ref.extractall(dataset_dir)
        else:
            unzip_folder = dataset_path

    
        refined_portals_csv = None
        partial_rec_dir = None

        if use_refined_outputs:
            print("Looking for local refined outputs")
            refined_scan_dir = refined_group_dir / "local" / scan_name
            refined_scan_path = refined_scan_dir / "reconstruction_refined_x1.zip"
            if refined_scan_path.exists():
                print(f"Found {str(refined_scan_path)}")
                partial_rec_dir = Path(f"/content/partial_rec/{scan_name}")
                with zipfile.ZipFile(refined_scan_path, 'r') as zip_ref:
                    zip_ref.extractall(partial_rec_dir)
            else:
                print(f"Found {str(refined_scan_dir)}")
                partial_rec_dir = refined_scan_dir / 'sfm'

        print(f"Loading partial scan: {unzip_folder}")

        try:
            next_image_id, placed_portal, partial_rec_dir, combined_rec, timestamp_per_image, \
            arkit_precomputed, detections_per_qr, image_ids_per_qr, \
            chunks_image_ids = load_partial(
                unzip_folder,
                dataset_dir,
                dataset_group,
                truth_portal_poses,
                next_image_id,
                placed_portal,
                partial_rec_dir,
                combined_rec,
                timestamp_per_image,
                arkit_precomputed,
                detections_per_qr,
                image_ids_per_qr,
                chunks_image_ids,
                all_observations=all_observations,
                all_poses=all_poses,
                with_3dpoints=with_3dpoints
            )
            print('========================================================================')
            print(f"Loaded {str(unzip_folder.stem)}")
            print('========================================================================')
            consecutive_alignment_fails = 0
            datasets_already_aligned.append(unzip_folder)
            print(f"Already aligned {len(datasets_already_aligned)} datasets, {len(datasets_to_align)} left")

        except NoOverlapException:
            # If the dataset didn't have any overlap, add back to queue and retry again later,
            # since it may overlap with other chunks which have not yet been added.
            datasets_to_align.append(dataset_path)
            print(f"NO OVERLAP! Add back to queue to retry later: {dataset_path}")

            # However, if all chunks in the queue have failed, it means none of them can be aligned
            consecutive_alignment_fails += 1
            print(f"Number of consecutive alignment fails: {consecutive_alignment_fails}")
            if consecutive_alignment_fails >= len(datasets_to_align):
                err = "One or more chunks failed to align since none of them overlap with the already placed chunks."
                print(f"ERROR! {err}")
                print(f"{len(datasets_already_aligned)} already aligned chunks:")
                print('\n'.join(str(path) for path in datasets_already_aligned))
                print(f"{len(datasets_to_align)} remaining chunks:")
                print('\n'.join(str(path) for path in datasets_to_align))
                print(f"{len(placed_portal)} QR codes already placed:")
                print('\n'.join(f"{qr_id} -> {pose}" for qr_id, pose in placed_portal.items()))
                #raise NoOverlapException(err)
                break

    def detection_position_stats(detections: Iterable[pycolmap.Rigid3d]):
        positions = np.array([det.translation for det in detections])
        pos_mean = np.mean(positions, axis=0)
        deviations = pos_mean - positions
        deviations_dist = np.linalg.norm(deviations, axis=1)
        min_dev, avg_dev, max_dev = np.min(deviations_dist), np.mean(deviations_dist), np.max(deviations_dist)
        med_dev = np.median(deviations_dist)
        rmse_dev = np.sqrt(np.mean(np.power(deviations_dist, 2)))
        return min_dev, avg_dev, med_dev, max_dev, rmse_dev

    unstitched_qr_detections = get_world_space_qr_codes(combined_rec, detections_per_qr, image_ids_per_qr)

    print('========================================================================')
    print("ALL DETECTIONS (basic stitch):")
    print('========================================================================')
    unstitched_mean_qr_poses = {qr_id: mean_pose(poses) for qr_id, poses in unstitched_qr_detections.items()}
    for qr_id, pose in unstitched_mean_qr_poses.items():
        min_dev, avg_dev, med_dev, max_dev, rmse_dev = detection_position_stats(unstitched_qr_detections[qr_id])
        print(f"{qr_id}, translation:{pose.translation}, min_dev: {min_dev:.6f}, avg_dev: {avg_dev:.6f}, med_dev: {med_dev:.6f}, max_dev: {max_dev:.6f}, rmse_dev: {rmse_dev:.6f}")

    align_reconstruction_chunks(combined_rec, chunks_image_ids, detections_per_qr, image_ids_per_qr, with_scale=False)

    unstitched_qr_detections = get_world_space_qr_codes(combined_rec, detections_per_qr, image_ids_per_qr)

    print('========================================================================')
    print("ALL DETECTIONS (se3 opt stitch):")
    print('========================================================================')
    stitched_mean_qr_poses = {qr_id: mean_pose(poses) for qr_id, poses in unstitched_qr_detections.items()}
    for qr_id, pose in stitched_mean_qr_poses.items():
        min_dev, avg_dev, med_dev, max_dev, rmse_dev = detection_position_stats(unstitched_qr_detections[qr_id])
        print(f"{qr_id}, translation:{pose.translation}, min_dev: {min_dev:.6f}, avg_dev: {avg_dev:.6f}, med_dev: {med_dev:.6f}, max_dev: {max_dev:.6f}, rmse_dev: {rmse_dev:.6f}")
    
    if with_3dpoints:
        # unrefined_sfm_dir = dataset_dir / 'outputs' / (dataset_group if dataset_group is not None else '') / 'unrefined_sfm_combined'
        unrefined_sfm_dir = refined_group_dir / 'global' / 'unrefined_sfm_combined'
        print(f"Saving unrefined sfm to: {unrefined_sfm_dir}")
        Path.mkdir(unrefined_sfm_dir, parents=True, exist_ok=True)
        combined_rec.write(unrefined_sfm_dir)
        point_cloud_path = refined_group_dir / 'global' / "UnrefinedPointCloud.ply"
        print(f"Saving unrefined point cloud as PLY to: {point_cloud_path}")
        #combined_rec.export_PLY(point_cloud_path)
        # As text for now, as mobile DMT doesn't work with binary domain data blobs
        export_ply_text(combined_rec, point_cloud_path)
        print(f'...Saved')

    if basic_stitch_only:
        print("Basic stitch only!")
        if truth_portal_poses:
            compare_portals(unstitched_mean_qr_poses, stitched_mean_qr_poses, truth_portal_poses, align=True, verbose=True, correct_scale=True)

        print('Finished Global Merge!')
        print('========================================================================')
        print('')
        print('========================================================================')

        return (
            combined_rec, unstitched_qr_detections, unstitched_mean_qr_poses,
            combined_rec, unstitched_qr_detections, stitched_mean_qr_poses,
            detections_per_qr, image_ids_per_qr
        )

    sorted_image_ids = list(combined_rec.images.keys())
    sorted_image_ids.sort()

    stitched_rec, stitched_qr_detections = run_stitching(
        detections_per_qr,
        image_ids_per_qr,
        timestamp_per_image,
        arkit_precomputed,
        combined_rec,
        sorted_image_ids,
        global_ba=True
    )

    print('========================================================================')
    print('ALL DETECTIONS (bundle adjusted):')
    print('========================================================================')
    stitched_mean_qr_poses = {qr_id: mean_pose(poses) for qr_id, poses in stitched_qr_detections.items()}
    for qr_id, pose in stitched_mean_qr_poses.items():
        deviation = np.std([det.translation for det in stitched_qr_detections[qr_id]], axis=0)
        deviation = np.mean(deviation)
        print(f"{qr_id} translation: {pose.translation}, deviation: {deviation:.10f}")


    manifest_out_path = output_path / 'refined_manifest.json'
    print(f"Saving refined manifest with {len(stitched_qr_detections)} detections, to: {manifest_out_path}")
    save_manifest_json(stitched_qr_detections, manifest_out_path, jobStatus="refined", jobProgress=100)

    if with_3dpoints:
        refined_sfm_dir = output_path / "refined_sfm_combined"
        print(f"Saving refined sfm to: {refined_sfm_dir}")
        Path.mkdir(refined_sfm_dir, parents=True, exist_ok=True)
        stitched_rec.write(refined_sfm_dir)
        point_cloud_path = output_path / "RefinedPointCloud.ply"
        print(f"Saving point cloud as PLY to: {point_cloud_path}")
        #stitched_rec.export_PLY(point_cloud_path)
        export_ply_text(stitched_rec, point_cloud_path)
        print(f'...Saved')

    if truth_portal_poses:
        compare_portals(unstitched_mean_qr_poses, stitched_mean_qr_poses, truth_portal_poses, align=True, verbose=True, correct_scale=True)

    print('========================================================================')
    print('Finished Global refinement!')
    print('========================================================================')
    print('')
    print('========================================================================')

    return (
        combined_rec, unstitched_qr_detections, unstitched_mean_qr_poses,
        stitched_rec, stitched_qr_detections, stitched_mean_qr_poses,
        detections_per_qr, image_ids_per_qr
    )