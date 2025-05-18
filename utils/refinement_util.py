from pathlib import Path
import pycolmap
import numpy as np
import shutil
from datetime import datetime
from typing import NamedTuple

from utils.triangulation import process_features_and_matching, triangulate_model
from utils.data_utils import (
    convert_pose_opengl_to_colmap, 
    precompute_arkit_offsets,
    get_world_space_qr_codes,
    mean_pose,
    setup_logger,
    save_portal_csv, 
    process_frames,
    load_dataset_metadata,
    rectify_portal_pose
)
from utils.local_bundle_adjuster import dmt_ba_solve_bundle_adjustment, prepare_ba_options


from hloc import triangulation

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


def setup_refinement_paths(scan_folder_path, output_path):
    """
    Setup and create necessary directories for refinement.
    
    Args:
        scan_folder_path: Path to scan folder
        output_path: Base output path
        
    Returns:
        RefinementPaths object containing all necessary paths
    """
    experiment_name = scan_folder_path.name
    paths = RefinementPaths(
        scan_folder=scan_folder_path,
        output=output_path / experiment_name,
        images=scan_folder_path / 'Frames/',
        sfm_dir=output_path / experiment_name / 'sfm',
        colmap_rec=output_path / experiment_name / 'colmap_rec',
        features=output_path / experiment_name / 'sfm/features.h5',
        global_features=output_path / experiment_name / 'sfm/global_features.h5',
        matches=output_path / experiment_name / 'sfm/matches.h5',
        sfm_pairs=output_path / experiment_name / 'sfm/pairs-sfm.txt',
        log_path=output_path / experiment_name
    )

    # Create necessary directories
    for path in [paths.output, paths.sfm_dir, paths.colmap_rec, paths.log_path]:
        path.mkdir(parents=True, exist_ok=True)

    return paths


def initialize_reconstruction(references, metadata):
    """
    Initialize reconstruction with camera intrinsics and poses.
    
    Returns:
        Tuple of (reconstruction, arkit_transforms)
    """
    rec = pycolmap.Reconstruction()
    arkit_cam_from_world_transforms = {}
    camera_id = image_id = 1

    for ref in references:
        image_filename = Path(ref).name
        timestampNs = metadata.timestamps_per_image[image_filename]
        
        # Add camera
        intrinsics = metadata.intrinsics_per_timestamp[timestampNs]
        fx, fy, cx, cy, w, h = intrinsics
        
        if fx == fy:
            model, params = 'SIMPLE_RADIAL', [fx, cx, cy, 0.0]
        else:
            model, params = 'RADIAL', [fx, fy, cx, cy, 0.0]
            
        cam = pycolmap.Camera(
            model=model,
            width=w,
            height=h,
            params=params,
            camera_id=camera_id
        )
        rec.add_camera(cam)
        
        # Add image
        ar_pose = metadata.ar_poses_per_timestamp[timestampNs]
        position, rotation = convert_pose_opengl_to_colmap(ar_pose[0:3], ar_pose[3:7])
        cam_to_world = pycolmap.Rigid3d(pycolmap.Rotation3d(rotation), position)
        cam_from_world = cam_to_world.inverse()
        arkit_cam_from_world_transforms[image_id] = cam_from_world
        
        img = pycolmap.Image(
            image_filename,
            pycolmap.ListPoint2D([]),
            cam_from_world,
            camera_id,
            image_id
        )
        rec.add_image(img)
        rec.register_image(image_id)
        
        camera_id += 1
        image_id += 1

    return rec, arkit_cam_from_world_transforms


def prepare_data_for_loop_closure(
    refined_rec, 
    arkit_cam_from_world_transforms,
    metadata,
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
        timestamp = metadata.timestamps_per_image[img.name]
        image_per_timestamp[timestamp] = img

    valid_timestamps = image_per_timestamp.keys()

    detections_per_qr = {}
    image_ids_per_qr = {}  # Only store the ID here. Still gotta use the latest image from the reconstruction at each iteration with the latest pose
    corners_per_qr = {}
    logger.info(f"valid timestamps: {len(valid_timestamps)}")
    logger.info(f"count of qr detections: {len(metadata.qr_detections_per_timestamp)}")
    for timestamp, detection in metadata.qr_detections_per_timestamp.items():
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
        cam_space_qr_pose = nearest_image.cam_from_world * detection["pose"] #T_RC = T_WC*T_RW

        logger.info(f"QR code {id} @ {timestamp} ns, nearest image: {nearest_image}, cam space pos: {cam_space_qr_pose}")

        detections_per_qr[id].append(cam_space_qr_pose)
        image_ids_per_qr[id].append(nearest_image.image_id)
        corners_per_qr[id].append(detection["portal_corners"])
    
    return arkit_precomputed, detections_per_qr, image_ids_per_qr, corners_per_qr


def process_QR(
    refined_rec, 
    detections_per_qr, 
    image_ids_per_qr, 
    paths, 
    metadata, 
    corners_per_qr, 
    logger
):
    logger.info("Now save adjusted QR code poses")
    stitched_qr_detections = get_world_space_qr_codes(
        refined_rec, 
        detections_per_qr, 
        image_ids_per_qr
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
        metadata.portal_sizes, 
        corners_per_qr
    )

def refine_dataset_part_two(
    paths,
    arkit_cam_from_world_transforms,
    metadata,
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
        metadata,
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
        timestamp_per_image=metadata.timestamps_per_image,
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
        metadata, 
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
    scan_folder_path, 
    output_path,
    every_nth_image=1,
    remove_outputs=False,
    domain_id="",
    job_id="",
    log_level="INFO",
    pool_executor=None
):
    """
    Refine a dataset using Structure from Motion techniques.
    
    Args:
        scan_folder_path: Path to the scan folder
        output_path: Path for output files
        every_nth_image: Process every nth image
        remove_outputs: Whether to remove existing outputs
        domain_id: Domain identifier
        job_id: Job identifier
        log_level: Logging level
        pool_executor: ThreadPoolExecutor instance for parallel processing
    Returns:
        Future object if pool_executor is provided, otherwise None
    """
    start_time = datetime.now()

    # Setup paths and logging
    paths = setup_refinement_paths(
        scan_folder_path, output_path
    )

    # Setup Logging
    logger = setup_logger(
        name="refine_dataset", 
        log_file=str(paths.log_path / "local_logs"), 
        domain_id=domain_id, 
        job_id=job_id, 
        dataset_id=scan_folder_path.name,
        level=log_level
    )
    logger.info(f'Starting local refinement of {scan_folder_path.name}')

    # Process frames and load data
    references, use_frames_from_video, original_image_count = process_frames(
        paths, every_nth_image, logger
    )

    # Load dataset metadata
    metadata = load_dataset_metadata(
        paths,  
        use_frames_from_video, 
        original_image_count, 
        logger
    )

    # Initialize reconstruction
    rec, arkit_cam_from_world_transforms = initialize_reconstruction(references, metadata)

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
            arkit_cam_from_world_transforms,
            metadata,
            logger,
            colmap_rec_path,
            remove_outputs,
            start_time
        )
        return future
    else:
        refine_dataset_part_two(
            paths,
            arkit_cam_from_world_transforms,
            metadata,
            logger,
            colmap_rec_path,
            remove_outputs,
            start_time
        )
        return None

def tri_ba_iteration(
    refined_rec, 
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
    refinement_config={}
):
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
    summary, loss_details = dmt_ba_solve_bundle_adjustment(
        detections_per_qr,
        image_ids_per_qr,
        timestamps_per_image,
        arkit_precomputed,
        refined_rec,
        ba_options,
        ba_config,
        refinement_config
    )

    ##print("\n".join(summary.BriefReport().split(",")))
    print("\n".join(summary.FullReport().split(",")))

    refined_rec.write(sfm_dir)

    refined_rec = triangulation.main(
        sfm_dir, 
        sfm_dir, 
        images, 
        sfm_pairs, 
        features, matches, 
        skip_geometric_verification=skip_geometric_verification,
        verbose=True
    )


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
    