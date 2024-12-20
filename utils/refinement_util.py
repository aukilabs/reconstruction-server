from pathlib import Path
import csv
import pycolmap
import numpy as np
from numpy.linalg import norm
import logging
import shutil
import os


from utils.triangulation import triangulate_model
from utils.data_utils import (
    convert_pose_opengl_to_colmap, 
    precompute_arkit_offsets,
    get_world_space_qr_codes,
    mean_pose,
    setup_logger,
    mp4_to_frames,
    add_file_handler
)
from utils.local_bundle_adjuster import dmt_ba_solve_bundle_adjustment, prepare_ba_options


from hloc import (
    extract_features,
    match_features,
    triangulation,
    pairs_from_poses
)


def refine_dataset(
    scan_folder_path, 
    output_path,
    every_nth_image=1,
    remove_outputs=False,
    domain_id="",
    job_id="",
    log_level="INFO",
    measure_pairs=None, 
    truth_pairs=None, 
    truth_portal_poses=None
):

    ############################
    # PATHS & CONFIGS
    ############################

    # Create and configure logger
    log_path = str(output_path) + '/' + str(scan_folder_path.name)
    os.makedirs(log_path, exist_ok=True)

    # Setup paths and names
    experiment_name = Path(scan_folder_path).name
    dataset = Path(scan_folder_path)
    images = dataset / 'Frames/'
    outputs = Path(output_path) / experiment_name
    sfm_dir = outputs / 'sfm'
    sfm_dir.mkdir(exist_ok=True, parents=True)
    sfm_pairs = sfm_dir / 'pairs-sfm.txt'
    features = sfm_dir / 'features.h5'
    matches = sfm_dir / 'matches.h5'
    log_file = log_path + "/local_logs"

    # Setup Loggging
    logger = setup_logger(name="refine_dataset", log_file=log_file, 
                        domain_id=domain_id, job_id=job_id, dataset_id=experiment_name,
                        level=log_level)

    logger.info(f'Working on {str(scan_folder_path.name)}')

    # Override Hloc
    setup_logger(name="hloc", log_file=log_file,
                domain_id=domain_id, job_id=job_id, dataset_id=experiment_name,
                level=log_level)

    #feature_conf = extract_features.confs["superpoint_max"]
    #feature_conf["output"] = features
    #feature_conf["model"]["max_keypoints"] = 1024
    #feature_conf["preprocessing"]["resize_max"] = 1024

    feature_conf = {
        "output": features,
        "model": {
            "name": "aliked",
            "model_name": "aliked-n16rot",
            "max_num_keypoints": 1024,
        },
        "preprocessing": {
            "grayscale": False,
            "resize_max": 1280,
        },
    }

    """
    feature_conf = {
        "output": features,
        "model": {
            "name": "superpoint",
            "max_keypoints": 1024,
        },
        "preprocessing": {
            "grayscale": True,
            "resize_max": 1024,
        },
    }
    """

    logger.info(f"Feature conf: {feature_conf}")
    #matcher_conf = match_features.confs["superpoint+lightglue"]
    matcher_conf = match_features.confs["aliked+lightglue"]

    ############################
    # LOAD DATASET
    ############################

    #--------------------
    # RGB Frames


    frames_mp4 = dataset / 'Frames.mp4'
    logger.info(f"Looking for mp4 encoded frames: {frames_mp4}")
    use_frames_from_video = False
    if frames_mp4.exists():
        logger.info(f"Frames mp4 found, unpacking into {images}")
        if not images.exists():
            images.mkdir()
        mp4_to_frames(frames_mp4, images, filename_prefix=experiment_name + "_")
        use_frames_from_video = True

    references = [str(p.relative_to(images)) for p in (images).iterdir()]
    original_image_count = len(references)
    references = sorted(references) # Assume file name is time stamp, to get chronological sequence

    references = references[0:-1:every_nth_image]

    logger.info(f'{len(references)}, frames selected, out of, {original_image_count}')


    #--------------------
    # Frames Metadata

    frames_csv_path = str(dataset / "Frames.csv")

    logger.info(f'Loading image timestamps from, {frames_csv_path} ...')

    # Initialize the dictionary
    timestamps_per_image = {}

    # Read and process the CSV file
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
            timestamps_per_image[filename] = timestamp

    if len(timestamps_per_image) != original_image_count:
        raise Exception("Mismatching number of Frames and Timestamps. "
                        f"{original_image_count} images {len(timestamps_per_image)} timestamps")
    # Display the result
    logger.info(f'{len(timestamps_per_image)}, frame timestamps loaded')


    #--------------------
    # Cam Intrinsics

    cam_intrinsics_csv_path = str(dataset / "CameraIntrinsics.csv")

    logger.info(f'Loading camera intrinsics from, {cam_intrinsics_csv_path}, ...')

    # Initialize the dictionary
    intrinsics_per_timestamp = {}

    # Read and process the CSV file
    with open(cam_intrinsics_csv_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            timestamp = round(float(row[0]) * 1e9) # s to ns
            intrinsics_per_timestamp[timestamp] = [
                float(row[1]), float(row[2]), # focal distance (fx, fy)
                float(row[3]), float(row[4]), # principal point (cx, cy)
                int(row[5]), int(row[6])      # resolution (w, h)
            ]

    if len(intrinsics_per_timestamp) != original_image_count:
        raise Exception("Mismatching number of Frames and Camera Intrinsics. "
                        f"{original_image_count} images {len(intrinsics_per_timestamp)} intrinsics")
    # Display the result
    logger.info(f'{len(intrinsics_per_timestamp)}, camera frame intrinsics loaded')


    #--------------------
    # Unrefined Poses

    ar_poses_csv_path = str(dataset / "ARposes.csv")

    logger.info(f'Loading unrefined AR poses from", {ar_poses_csv_path}, ...')

    # Initialize the dictionary
    ar_poses_per_timestamp = {}

    # Read and process the CSV file
    with open(ar_poses_csv_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            timestamp = round(float(row[0]) * 1e9) # s to ns
            ar_poses_per_timestamp[timestamp] = [float(val) for val in row[1:8]] # px, py, pz, rx, ry, rz, rw

    if len(ar_poses_per_timestamp) != original_image_count:
        raise Exception("Mismatching number of Frames and Poses. "
                        f"{original_image_count} images {len(ar_poses_per_timestamp)} poses")
    # Display the result
    logger.info(f'{len(ar_poses_per_timestamp)}, AR poses loaded')


    #--------------------
    # QR detections

    qr_detections_csv_path = dataset / "PortalDetections.csv"
    if not qr_detections_csv_path.exists() and (dataset / "Observations.csv").exists():
        qr_detections_csv_path = dataset / "Observations.csv"
        logger.info("WARNING: PortalDetections.csv not found, but found Observations.csv (old filename convention).")
    qr_detections_csv_path = str(qr_detections_csv_path)

    logger.info(f'Loading QR detections from, {qr_detections_csv_path}, ...')
    # Initialize the dictionary
    qr_detections_per_timestamp = {}

    # Read and process the CSV file
    with open(qr_detections_csv_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            timestamp = round(float(row[0]) * 1e9) # s to ns
            pose_values = [float(val) for val in row[2:9]] # px, py, pz, rx, ry, rz, rw
            pos = pose_values[:3]
            quat = pose_values[3:]

            pos, quat = convert_pose_opengl_to_colmap(pos, quat)

            qr_pose = pycolmap.Rigid3d(
                pycolmap.Rotation3d(np.array(quat)),
                np.array(pos)
            )

            qr_detections_per_timestamp[timestamp] = {
                "pose": qr_pose,
                "short_id": row[1]
            }

    # Display the result
    logger.info(f'{len(qr_detections_per_timestamp)}, QR detections loaded')

    #----------------------
    # Check if each qr detection has at least one image within [references] array
    this_chunk_detections_per_qr = {}
    for ts, detection in qr_detections_per_timestamp.items():
        id = detection["short_id"]

        if id not in this_chunk_detections_per_qr.keys():
            this_chunk_detections_per_qr[id] = [ts]
        else:
            this_chunk_detections_per_qr[id].append(ts)

    image_ts_list = list(timestamps_per_image.values())
    reference_ts = [ timestamps_per_image[ref] for ref in references]
    logger.debug(f"timestamps length: {len(image_ts_list)}")
    logger.debug(f"references length: { len(references)}")
    for qr_id, timestamps in this_chunk_detections_per_qr.items():

        in_ref = [a in reference_ts for a in timestamps]
        logger.debug(qr_id)
        logger.debug(in_ref)
        logger.debug(timestamps)

        # If the reference ts contain any qr detection image, skip
        if any(in_ref):
            continue

        all_timestamps_before = [t for t in image_ts_list if t <= timestamps[0]]
        nearest_image_timestamp = np.max(all_timestamps_before)

        for filename, ts in timestamps_per_image.items():
            if ts == nearest_image_timestamp and filename not in references:
                logger.debug(filename)
                references.append(filename)

    references = sorted(references)
    logger.debug(len(references))

    #----------------------
    # Init unrefined Reconstruction

    # Start with the known camera intrinsics and cam poses from ARKit.
    rec = pycolmap.Reconstruction()
    camera_id = 1  # increment for each new camera
    image_id = 1  # increment for each new image
    arkit_cam_from_world_transforms = {}
    for ref in references:
        image_filename = Path(ref).name
        timestampNs = timestamps_per_image[image_filename]

        intrinsics = intrinsics_per_timestamp[timestampNs]
        ar_pose = ar_poses_per_timestamp[timestampNs]
        fx, fy, cx, cy, w, h = intrinsics

        if fx == fy:
            model = 'SIMPLE_PINHOLE'
            params = [fx, cx, cy]
        else:
            model = 'PINHOLE'
            params = [fx, fy, cx, cy]
        cam = pycolmap.Camera(
            model=model, width=w, height=h, params=params, camera_id=camera_id
        )
        position = ar_pose[0:3]
        rotation = ar_pose[3:7]

        position, rotation = convert_pose_opengl_to_colmap(position, rotation)

        rec.add_camera(cam)

        # logger.info(f"{timestampNs} @ Cam {camera_id}: {w}x{h}, {model} params {params} at pos=({position}) rot=({rotation})")

        cam_to_world = pycolmap.Rigid3d(pycolmap.Rotation3d(rotation), position)

        cam_from_world = cam_to_world.inverse()
        arkit_cam_from_world_transforms[image_id] = cam_from_world
        #print("INV: pos:", world_to_cam.translation, "rot:", world_to_cam.rotation)
        img = pycolmap.Image(
            image_filename, pycolmap.ListPoint2D([]), cam_from_world, camera_id, image_id
        )
        rec.add_image(img)
        rec.register_image(image_id)

        camera_id += 1
        image_id += 1

    colmap_rec_path = outputs / 'colmap_rec'
    Path.mkdir(colmap_rec_path, parents=True, exist_ok=True)
    rec.write(colmap_rec_path)

    refined_rec = pycolmap.Reconstruction()
    refined_rec.read(colmap_rec_path)
    refined_rec.write(sfm_dir)

    ############################
    # IMAGE PAIRS
    ############################
    logger.info("Pairs from poses")
    pairs_from_poses.main(colmap_rec_path, sfm_pairs, 20, rotation_threshold=360)

    ############################
    # FEATURE POINTS
    ############################

    # features.unlink(missing_ok=True)
    logger.info("Extracting features")
    features = extract_features.main(
        feature_conf, 
        images, 
        outputs, 
        feature_path=features, 
        as_half=True,
        image_list=references
    )

    # Feature Matching
    logger.info("Feature matching")
    match_features.main(matcher_conf, sfm_pairs, features=features, matches=matches)

    ############################
    # PRE-PROCESSING
    ############################

    # Load features and pairs from above
    refined_rec.read(sfm_dir)

    # SORT images (since order may be wrong in captured dataset)
    sorted_image_ids = list(refined_rec.images.keys())
    sorted_image_ids.sort()

    # PRE-COMPUTE some offsets & gravity from the unrefined ARKit poses, which will remain constant during refinement.
    # These are used in the loss function to guide the refinement, not to diverge too far off from original.
    arkit_precomputed = precompute_arkit_offsets(
        sorted_image_ids, arkit_cam_from_world_transforms
    ) # skip first since these are offsets to previous image

    # PRE-LOAD QR DATA FOR LOOP CLOSURE
    image_per_timestamp = {}
    for img in refined_rec.images.values():
        timestamp = timestamps_per_image[img.name]
        image_per_timestamp[timestamp] = img
        

    valid_timestamps = image_per_timestamp.keys()

    detections_per_qr = {}
    image_ids_per_qr = {}  # Only store the ID here. Still gotta use the latest image from the reconstruction at each iteration with the latest pose
    logger.info(f"valid timestamps: {len(valid_timestamps)}")
    logger.info(f"count of qr detections: {len(qr_detections_per_timestamp)}")
    for timestamp, detection in qr_detections_per_timestamp.items():
        id = detection["short_id"]

        if id not in detections_per_qr.keys():
            detections_per_qr[id] = []
            image_ids_per_qr[id] = []

        # print(f"QR position for {id}:", detection["pose"])

        # Convert back into cam space of nearest image frame (since we skip some frames)
        valid_nearest_timestamps = [t for t in valid_timestamps if t <= timestamp]
        if valid_nearest_timestamps:
            nearest_image_timestamp = np.max(valid_nearest_timestamps)
        else:
            continue
        nearest_image = image_per_timestamp[nearest_image_timestamp]
        cam_space_qr_pose = nearest_image.cam_from_world * detection["pose"] #T_RC = T_WC*T_RW

        logger.info(f"QR code {id} @ {timestamp} ns, nearest image: {nearest_image}, cam space pos: {cam_space_qr_pose}")

        detections_per_qr[id].append(cam_space_qr_pose)
        image_ids_per_qr[id].append(nearest_image.image_id)

    # Add more truth measurements if available.
    # Uses portal poses from JSON.
    # Only if portal poses are manually measured very carefully.
    if truth_portal_poses is not None:
        measure_pairs = []
        truth_pairs = []
        all_detected_qr_ids = list(detections_per_qr.keys())

        #First to last
        measure_pairs.append([all_detected_qr_ids[0], all_detected_qr_ids[-1]])
        truth_pairs.append(
            norm(truth_portal_poses[all_detected_qr_ids[0]].translation - truth_portal_poses[all_detected_qr_ids[-1]].translation)
        )

        # From each to next in order of first scan.
        for i, short_id in enumerate(all_detected_qr_ids[1:]):
            prev_short_id = all_detected_qr_ids[i - 1]
            measure_pairs.append([prev_short_id, short_id])
            truth_pairs.append(
                norm(truth_portal_poses[short_id].translation - truth_portal_poses[prev_short_id].translation)
            )


    logger.info("Start triangulation")
    refined_rec = triangulate_model(
        sfm_dir, 
        colmap_rec_path, 
        images, 
        sfm_pairs, 
        features, 
        matches,
        skip_geometric_verification=True,
        verbose=True,
        timestamp_per_image=timestamps_per_image,
        arkit_precomputed=arkit_precomputed
    )
    refined_rec.write(sfm_dir)
    logger.info("Finished triangulation")
    reproj_error = refined_rec.compute_mean_reprojection_error()
    logger.info(f'After triangulation, the mean reprojection error is {reproj_error}')

    logger.info("Now save adjusted QR code poses")
    stitched_qr_detections = get_world_space_qr_codes(refined_rec, detections_per_qr, image_ids_per_qr)
    stitched_mean_qr_poses = {qr_id: mean_pose(poses) for qr_id, poses in stitched_qr_detections.items() if poses}
    for qr_id, pose in stitched_mean_qr_poses.items():
        deviation = np.std([det.translation for det in stitched_qr_detections[qr_id]], axis=0)
        deviation = np.mean(deviation)
        logger.info(f'QR code id: {qr_id}, pose translation {pose.translation}, deviation: {deviation:.5f}')

    if remove_outputs:
        logger.info('Remove output directory')
        shutil.rmtree(outputs)
    
    logger.info('Finished local refinement!')
    logger.info('========================================================================')
    logger.info('')
    logger.info('========================================================================')

    return refined_rec, rec



def tri_ba_iteration(refined_rec, 
                     sorted_image_ids, 
                     detections_per_qr,
                     image_ids_per_qr,
                     timestamps_per_image,
                     arkit_precomputed,
                     ba_options,
                     sfm_dir,
                     images,
                     sfm_pairs,
                     features,
                     matches,
                     reproj_error_history,
                     skip_geometric_verification=True,
                     refinement_config={}):
    # Avoid degeneracies in bundle adjustment
    refined_rec.filter_observations_with_negative_depth()


    # Configure bundle adjustment
    ba_config = pycolmap.BundleAdjustmentConfig()

    for image_id in sorted_image_ids:
        ba_config.add_image(image_id)

    # Fix 7-DOFs of the bundle adjustment problem
    ba_config.set_constant_cam_pose(sorted_image_ids[0])
    ba_config.set_constant_cam_positions(sorted_image_ids[1], [0])

    print("Start Global Bundle Adjustment")
    summary, loss_details = dmt_ba_solve_bundle_adjustment(detections_per_qr,
                                                            image_ids_per_qr,
                                                            timestamps_per_image,
                                                            arkit_precomputed,
                                                            refined_rec,
                                                            ba_options,
                                                            ba_config,
                                                            refinement_config)

    ##print("\n".join(summary.BriefReport().split(",")))
    print("\n".join(summary.FullReport().split(",")))

    refined_rec.write(sfm_dir)

    refined_rec = triangulation.main(sfm_dir, 
                                     sfm_dir, 
                                     images, 
                                     sfm_pairs, 
                                     features, matches, 
                                     skip_geometric_verification=skip_geometric_verification,
                                     verbose=True)


    reproj_error_history.append(refined_rec.compute_mean_reprojection_error())
    print(f"Mean reprojection error {len(reproj_error_history)} = {reproj_error_history[-1]}")

    return refined_rec, loss_details, reproj_error_history


def triangulator(
        reconstruction, 
        sfm_dir,
        BA_iters=3,
        max_reprojection_err=4.0, 
        min_triangulation_angle=2.0
    ):

    mapper = pycolmap.IncrementalMapper(pycolmap.DatabaseCache())
    mapper.begin_reconstruction(reconstruction)

    for BA_iter in range(BA_iters):
        # Initial BA
        ba_options = prepare_ba_options()
        pycolmap.bundle_adjustment(reconstruction, ba_options)

        # Filter reconstrucion
        mapper.observation_manager.filter_all_points3D(max_reprojection_err, min_triangulation_angle)

    mapper.end_reconstruction(False)

    reconstruction.write(sfm_dir)

    return reconstruction
    