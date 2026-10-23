from pathlib import Path
import pycolmap
import numpy as np
import shutil
from datetime import datetime
from typing import NamedTuple

from reconstruction.pipeline.local.triangulation import process_features_and_matching, triangulate_model
from reconstruction.geometry.pose import convert_pose_opengl_to_colmap, mean_pose, rectify_portal_pose
from reconstruction.io.scan import load_scan
from reconstruction.pipeline.local.qr import precompute_arkit_offsets, get_world_space_qr_codes
from reconstruction.log import LOCAL_REFINEMENT, setup_logger
from reconstruction.io.portals import save_portal_csv
from reconstruction.preprocess.frames import process_frames

class RefinementPaths(NamedTuple):
    """Container for all paths used in refinement."""
    scan_folder: Path
    output: Path
    images: Path
    sfm_dir: Path
    colmap_rec: Path
    features: Path
    global_features: Path
    matches: Path
    sfm_pairs: Path
    log_path: Path


def setup_refinement_paths(layout, scan):
    """
    Setup and create necessary directories for refinement.
    
    Args:
        layout: JobLayout of the job
        scan: Name of the scan folder under datasets/
        
    Returns:
        RefinementPaths object containing all necessary paths
    """
    scan_folder_path = layout.dataset(scan)
    sfm_dir = layout.refined_sfm(scan)
    paths = RefinementPaths(
        scan_folder=scan_folder_path,
        output=layout.refined_scan(scan),
        images=scan_folder_path / 'Frames/',
        sfm_dir=sfm_dir,
        colmap_rec=layout.refined_scan(scan) / 'colmap_rec',
        features=sfm_dir / 'features.h5',
        global_features=sfm_dir / 'global_features.h5',
        matches=sfm_dir / 'matches.h5',
        sfm_pairs=sfm_dir / 'pairs-sfm.txt',
        log_path=layout.refined_scan(scan)
    )

    # Create necessary directories
    for path in [paths.output, paths.sfm_dir, paths.colmap_rec, paths.log_path]:
        path.mkdir(parents=True, exist_ok=True)

    return paths


def initialize_reconstruction(image_references, scan_data):
    rec = pycolmap.Reconstruction()

    cam_from_world_transforms = {}

    for i, ref in enumerate(image_references):
        image_filename = Path(ref).name
        timestampNs = scan_data.timestamps_per_image[image_filename]
        
        # Add camera
        intrinsics = scan_data.intrinsics_per_timestamp[timestampNs]
        fx, fy, cx, cy, w, h = intrinsics
        
        if fx == fy:
            model, params = 'SIMPLE_PINHOLE', [fx, cx, cy]
        else:
            model, params = 'PINHOLE', [fx, fy, cx, cy]
        
        id = i + 1

        cam = pycolmap.Camera(
            model=model,
            width=w,
            height=h,
            params=params,
            camera_id=id
        )
        rec.add_camera(cam)

        rig = pycolmap.Rig()
        rig.rig_id = id

        sensor = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=id)
        rig.add_ref_sensor(sensor)
        rec.add_rig(rig)
        
        # Add image
        ar_pose = scan_data.ar_poses_per_timestamp[timestampNs]
        position, rotation = convert_pose_opengl_to_colmap(ar_pose[0:3], ar_pose[3:7])
        cam_to_world = pycolmap.Rigid3d(pycolmap.Rotation3d(rotation), position)
        cam_from_world = cam_to_world.inverse()
        cam_from_world_transforms[id] = cam_from_world

        frame = pycolmap.Frame(
            rig_id = id,
            rig_from_world = cam_from_world,
            frame_id = id
        )
        frame.add_data_id(pycolmap.data_t(
            sensor_id=sensor,
            id=id
        ))
        rec.add_frame(frame)

        img = pycolmap.Image(image_filename, pycolmap.Point2DList([]), id, id)
        img.frame_id = id
        rec.add_image(img)
        rec.register_frame(id)

    return rec, cam_from_world_transforms


def prepare_data_for_loop_closure(
    refined_rec, 
    arkit_cam_from_world_transforms,
    scan_data,
    logger
):
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
        timestamp = scan_data.timestamps_per_image[img.name]
        image_per_timestamp[timestamp] = img

    valid_timestamps = image_per_timestamp.keys()

    detections_per_qr = {}
    image_ids_per_qr = {}  # Only store the ID here. Still gotta use the latest image from the reconstruction at each iteration with the latest pose
    corners_per_qr = {}
    logger.info(f"valid timestamps: {len(valid_timestamps)}")
    logger.info(f"count of qr detections: {len(scan_data.qr_detections_per_timestamp)}")
    for timestamp, detection in scan_data.qr_detections_per_timestamp.items():
        if timestamp not in valid_timestamps:
            continue
        id = detection["short_id"]

        if id not in detections_per_qr.keys():
            detections_per_qr[id] = []
            image_ids_per_qr[id] = []
            corners_per_qr[id] = []

        # Convert back into cam space of nearest image frame (since we skip some frames)
        valid_nearest_timestamps = [t for t in valid_timestamps if t <= timestamp]
        if valid_nearest_timestamps:
            nearest_image_timestamp = np.max(valid_nearest_timestamps)
        else:
            continue
        nearest_image = image_per_timestamp[nearest_image_timestamp]
        cam_space_qr_pose = nearest_image.cam_from_world() * detection["pose"] #T_RC = T_WC*T_RW

        logger.debug(f"QR code {id} @ {timestamp} ns, nearest image: {nearest_image}, cam space pos: {cam_space_qr_pose}")

        detections_per_qr[id].append(cam_space_qr_pose)
        image_ids_per_qr[id].append(nearest_image.image_id)
        corners_per_qr[id].append(detection["portal_corners"])
    
    return arkit_precomputed, detections_per_qr, image_ids_per_qr, corners_per_qr


def process_QR(
    refined_rec, 
    detections_per_qr, 
    image_ids_per_qr, 
    paths, 
    scan_data, 
    corners_per_qr, 
    logger
):
    logger.info("Now save adjusted QR code poses")
    stitched_qr_detections = get_world_space_qr_codes(
        refined_rec, 
        detections_per_qr, 
        image_ids_per_qr,
        logger=logger
    )
    stitched_mean_qr_poses = {
        qr_id: mean_pose(poses) for qr_id, poses in stitched_qr_detections.items() if poses
    }
    for qr_id, pose in stitched_mean_qr_poses.items():
        deviation = np.std([det.translation for det in stitched_qr_detections[qr_id]], axis=0)
        deviation = np.mean(deviation)
        logger.info(f'QR code id: {qr_id}, pose translation {pose.translation}, deviation: {deviation:.5f}')

    stitched_qr_detections = {qr_id: [rectify_portal_pose(p) for p in poses] for qr_id, poses in stitched_qr_detections.items()}

    stitched_qr_csv_path = paths.sfm_dir / "portals.csv"
    save_portal_csv(
        stitched_qr_detections, 
        stitched_qr_csv_path, 
        image_ids_per_qr, 
        scan_data.portal_sizes, 
        corners_per_qr
    )

def refine_dataset_part_two(
    paths,
    arkit_cam_from_world_transforms,
    scan_data,
    logger,
    colmap_rec_path,
    remove_outputs,
    start_time
):
    # Prepare data for loop closure
    refined_rec = pycolmap.Reconstruction()
    refined_rec.read(paths.sfm_dir)
    arkit_precomputed, detections_per_qr, image_ids_per_qr, corners_per_qr = prepare_data_for_loop_closure(
        refined_rec, 
        arkit_cam_from_world_transforms,
        scan_data,
        logger
    )

    # Triangulation
    logger.info("Start triangulation")
    refined_rec = triangulate_model(
        paths.sfm_dir, 
        colmap_rec_path, 
        paths.images, 
        paths.sfm_pairs, 
        paths.features, 
        paths.matches,
        skip_geometric_verification=True,
        verbose=True,
        timestamp_per_image=scan_data.timestamps_per_image,
        arkit_precomputed=arkit_precomputed,
        detections_per_qr=detections_per_qr,
        image_ids_per_qr=image_ids_per_qr
    )
    refined_rec.write(paths.sfm_dir)
    logger.info("Finished triangulation")
    reproj_error = refined_rec.compute_mean_reprojection_error()
    logger.info(f'After triangulation, the mean reprojection error is {reproj_error}')

    # Process QR codes
    process_QR(
        refined_rec, 
        detections_per_qr, 
        image_ids_per_qr, 
        paths, 
        scan_data, 
        corners_per_qr, 
        logger
    )

    if remove_outputs:
        logger.info('Remove output directory')
        shutil.rmtree(paths.output)
    
    duration = datetime.now() - start_time
    logger.info(f"Local refinement completed in {duration}")
    logger.info('========================================================================')
    logger.info('')
    logger.info('========================================================================')


def refine_dataset(
    layout,
    scan,
    every_nth_image=1,
    remove_outputs=False,
    domain_id="",
    job_id="",
    log_level="INFO",
    log_format="json",
    pool_executor=None
):
    """
    Refine a dataset using Structure from Motion techniques.
    
    Args:
        layout: JobLayout of the job; the scan is read from datasets/<scan> and refined
            into refined/local/<scan>
        scan: Name of the scan folder
        every_nth_image: Process every nth image
        remove_outputs: Whether to remove existing outputs
        domain_id: Domain identifier
        job_id: Job identifier
        log_level: Logging level
        log_format: Logging format
        pool_executor: ThreadPoolExecutor instance for parallel processing
    Returns:
        Future object if pool_executor is provided, otherwise None
    """
    start_time = datetime.now()

    # Setup paths and logging
    paths = setup_refinement_paths(layout, scan)

    # Setup Logging
    logger = setup_logger(
        name=LOCAL_REFINEMENT,
        log_file=str(paths.log_path / "local_logs"), 
        domain_id=domain_id, 
        job_id=job_id, 
        dataset_id=scan,
        level=log_level,
        log_format=log_format
    )
    logger.info(f'Starting local refinement of {scan}')

    # Process frames and load data
    references, use_frames_from_video, original_image_count = process_frames(
        paths, every_nth_image, logger
    )

    # Load scan from folder
    scan_data = load_scan(
        paths,  
        use_frames_from_video, 
        original_image_count, 
        logger
    )

    # Initialize reconstruction
    rec, cam_from_world_transforms = initialize_reconstruction(references, scan_data)
    # Save initial reconstruction
    colmap_rec_path = paths.output / 'colmap_rec'
    colmap_rec_path.mkdir(parents=True, exist_ok=True)
    rec.write(colmap_rec_path)
    rec.write(paths.sfm_dir)

    # Process features and matching
    process_features_and_matching(
        references, 
        colmap_rec_path, 
        paths,
        logger
    )
    
    if pool_executor:
        future = pool_executor.submit(
            refine_dataset_part_two,
            paths,
            cam_from_world_transforms,
            scan_data,
            logger,
            colmap_rec_path,
            remove_outputs,
            start_time
        )
        return future
    else:
        refine_dataset_part_two(
            paths,
            cam_from_world_transforms,
            scan_data,
            logger,
            colmap_rec_path,
            remove_outputs,
            start_time
        )
        return None
