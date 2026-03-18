from typing import Dict, List, Optional, Set
import time
import pycolmap
import os 
from pathlib import Path
import zipfile
import shutil
import csv
import numpy as np 
from numpy.linalg import norm
import logging
from dataclasses import dataclass

from sklearn import logger

from utils.data_utils import (
    convert_pose_colmap_to_opengl,
    mean_pose,
    convert_pose_opengl_to_colmap, 
    precompute_arkit_offsets, 
    get_world_space_qr_codes,
    save_manifest_json,
    export_rec_as_ply,
    parse_portals_from_manifest
)
from utils.geometry_utils import align_reconstruction_chunks
from utils.io import Model, read_portal_csv, read_model, merge_models, write_model, apply_similarity_to_new_model, validate_model_consistency
from utils.voxel_raycast_utils import carve_outdated_reference_geometry


class NoOverlapException(Exception):
    def __init__(self, message='No overlaps!'):
        # Call the base class constructor with the parameters it needs
        super(NoOverlapException, self).__init__(message)

floor_origin_portal_pose_GL = pycolmap.Rigid3d(
    pycolmap.Rotation3d(np.array([-0.7071068, 0.0, 0.0, 0.7071068])),
    np.array([0.0, 0.0, 0.0]))
p, q = convert_pose_opengl_to_colmap(np.array([0.0, 0.0, 0.0]), np.array([-0.7071068, 0.0, 0.0, 0.7071068]))
floor_origin_portal_pose = pycolmap.Rigid3d(pycolmap.Rotation3d(q), p)


@dataclass
class StitchingData:
    detections_per_qr: Dict[str, List[pycolmap.Rigid3d]] = None
    image_ids_per_qr: Dict[str, List[int]] = None
    timestamp_per_image: Dict[str, int] = None
    arkit_precomputed: Dict = None
    placed_portal: Dict[str, pycolmap.Rigid3d] = None
    chunks_image_ids: List[List[int]] = None
    combined_rec: pycolmap.Reconstruction = None
    next_image_id: int = 1
    portal_sizes: Dict[str, float] = None

    def __post_init__(self):
        self.detections_per_qr = self.detections_per_qr or {}
        self.image_ids_per_qr = self.image_ids_per_qr or {}
        self.timestamp_per_image = self.timestamp_per_image or {}
        self.arkit_precomputed = self.arkit_precomputed or {}
        self.placed_portal = self.placed_portal or {}
        self.chunks_image_ids = self.chunks_image_ids or []
        self.combined_rec = self.combined_rec or pycolmap.Reconstruction()
        self.portal_sizes = self.portal_sizes or {}

@dataclass
class StitchingResult:
    basic_rec: pycolmap.Reconstruction
    basic_detections: Dict[str, List[pycolmap.Rigid3d]]
    basic_poses: Dict[str, pycolmap.Rigid3d]
    refined_rec: pycolmap.Reconstruction
    refined_detections: Dict[str, List[pycolmap.Rigid3d]]
    refined_poses: Dict[str, List[pycolmap.Rigid3d]]
    detections_per_qr: Dict[str, List[pycolmap.Rigid3d]]
    image_ids_per_qr: Dict[str, List[int]]

@dataclass
class StitchResults:
    rec: pycolmap.Reconstruction
    detections: Dict[str, List[pycolmap.Rigid3d]]
    poses: Dict[str, pycolmap.Rigid3d]

@dataclass
class Paths:
    parent_dir: Path
    output_path: Path
    dataset_dir: Path
    refined_group_dir: Path
    reference_path: Path    # Path for reference reconstruction (global refinement that set as canonical)

def stitching_helper(
    dataset_paths: List[Path],
    group_folder: Path,
    truth_portal_poses: Dict[str, pycolmap.Rigid3d],
    use_refined_outputs: bool = False,
    with_3dpoints: bool = False,
    basic_stitch_only: bool = False,
    logger_name: Optional[str] = None
) -> StitchingResult:
    """Main function to stitch multiple reconstructions together.
    
    Args:
        dataset_paths: List of paths to datasets to stitch
        dataset_group: Name of dataset group
        group_folder: Path to group folder
        truth_portal_poses: Ground truth portal poses if available
        all_observations: Whether to use all observations
        all_poses: Whether to use all poses 
        use_refined_outputs: Whether to use refined outputs
        with_3dpoints: Whether to include 3D points
        basic_stitch_only: Whether to only do basic stitching
        logger_name: Name of logger to use

    Returns:
        StitchingResult containing basic and refined reconstructions
    """
    logger = logging.getLogger(logger_name)

    # Initialize paths and data
    paths = _initialize_paths(group_folder)
    stitch_data = StitchingData()

    # Process datasets
    aligned_datasets = _process_datasets(
        dataset_paths,
        paths,
        truth_portal_poses,
        stitch_data,
        use_refined_outputs,
        with_3dpoints,
        logger
    )

    if not aligned_datasets:
        logger.error("Failed to align any datasets")
        return None
    
    # Get basic stitch results
    logger.info("Getting basic stitch results...")
    basic_results = _get_basic_stitch_results(
        stitch_data,
        truth_portal_poses,
        paths,
        with_3dpoints and basic_stitch_only, # No need to export the unrefined ply if global refinement is enabled.
        logger
    )
    logger.info("DONE")

    if basic_stitch_only:
        return _handle_basic_stitch(
            basic_results,
            stitch_data,
            paths,
            truth_portal_poses,
            with_3dpoints,
            logger
        )
    
    # Get refined results
    logger.info("Getting refined results...")
    refined_results = _get_refined_results(
        stitch_data,
        basic_results,
        paths,
        truth_portal_poses,
        with_3dpoints,
        logger
    )
    logger.info("DONE")

    return StitchingResult(
        basic_rec=basic_results.rec,
        basic_detections=basic_results.detections,
        basic_poses=basic_results.poses,
        refined_rec=refined_results.rec,
        refined_detections=refined_results.detections,
        refined_poses=refined_results.poses,
        detections_per_qr=stitch_data.detections_per_qr,
        image_ids_per_qr=stitch_data.image_ids_per_qr
    )


def update_helper(
    dataset_paths: List[Path],
    group_folder: Path,
    logger_name: Optional[str] = None
) -> StitchingResult:
    """Main function to stitch multiple reconstructions together.
    
    Args:
        dataset_paths: List of paths to datasets to stitch
        dataset_group: Name of dataset group
        group_folder: Path to group folder
        truth_portal_poses: Ground truth portal poses if available
        all_observations: Whether to use all observations
        all_poses: Whether to use all poses 
        use_refined_outputs: Whether to use refined outputs
        with_3dpoints: Whether to include 3D points
        basic_stitch_only: Whether to only do basic stitching
        logger_name: Name of logger to use

    Returns:
        StitchingResult containing basic and refined reconstructions
    """

    logger = logging.getLogger(logger_name)

    # Initialize paths and data
    paths = _initialize_paths(group_folder, "update_helper")
    stitch_data = StitchingData()

    pending_update_rec = []


    dataset_rec_paths = [
        _get_refined_rec_dir(
            True,
            paths.refined_group_dir,
            scan_name,
            logger
        )for scan_name in [path.stem for path in dataset_paths]]

    if len(dataset_rec_paths) > 1:
        logger.info("Multiple dataset paths found. Proceeding with stitching.")

        # TODO: Replace this function with bundle scans and perform basic stitch.
        pending_update_rec.extend(dataset_rec_paths)

    elif len(dataset_rec_paths) == 1:
        pending_update_rec.append(dataset_rec_paths[0])
        logger.info("Only one dataset path found. Skipping stitching and preparing for update refinement.")
    else:
        logger.error("No dataset paths found. Exiting.")
        return None
    
    # Loading the reference model that will be refined. This should be the model that set to be canonical, which is the latest colmap model of the domain.
    cams_r, imgs_r, pts_r = read_model(paths.reference_path / "refined_sfm_combined", ".bin",logger=logger)
    portal_r = parse_portals_from_manifest(paths.reference_path / "refined_manifest.json")
    portal_r = {pid: pycolmap.Rigid3d(pycolmap.Rotation3d(portal_r[pid][0]), portal_r[pid][1]) for pid in portal_r.keys()}

    # Process datasets
    for pending_update_rec_dir in pending_update_rec:
        logger.info(f"Processing dataset for update refinement: {pending_update_rec_dir}")

        # Loading the new reconstruction that contains the new geometry to be merged in. 
        # This should be the local refined reconstruction of the new scan that will be merged in.
        cams_u, imgs_u, pts_u = read_model(pending_update_rec_dir, ".bin", logger=logger)
        portals_u = load_qr_detections_from_local_refinement(pending_update_rec_dir, logger)
        
        # Align the new reconstruction to the reference model using the detected QR code portals as anchors. 
        # This gives us a rough alignment that is good enough for culling out outdated geometry from the reference model.
        alignment_mat = _calculate_alignment_transform(portals_u, portal_r, logger)
        logger.info(f"Calculated alignment transform for update refinement: \n{alignment_mat.matrix()}")
        cams_u_aligned, imgs_u_aligned, pts_u_aligned = apply_similarity_to_new_model(cams_u, imgs_u, pts_u, alignment_mat.matrix())

        # Pruning the reference model by carving out points that violate the new free-space constraints observed in the new scan.
        pruned_imgs_r, pruned_pts_r = carve_outdated_reference_geometry(
            ref_imgs=imgs_r,
            ref_pts=pts_r,
            new_imgs=imgs_u_aligned,
            new_pts=pts_u_aligned,
            voxel_size=0.15,         # Adjust based on scene scale (e.g. 10cm)
            clearance_margin=0.1,   # Stop 10cm before the target to avoid false collisions
            min_surviving_points=50, # Drop old images with < 50 valid points left
            logger=logger
        )
        logger.info(f"Pruned reference model has {len(pruned_imgs_r)} images and {len(pruned_pts_r)} points (out of original {len(imgs_r)} images and {len(pts_r)} points).")
        if logger.level <= logging.DEBUG:
            validate_model_consistency(cams_r, pruned_imgs_r, pruned_pts_r, logger=logger)
            os.makedirs(paths.output_path / f"pruned_update_{pending_update_rec_dir.parent.name}", exist_ok=True)
            write_model(cams_r, pruned_imgs_r, pruned_pts_r, paths.output_path / f"pruned_update_{pending_update_rec_dir.parent.name}")
            logger.debug(f"Exported pruned reference model to {paths.output_path / f'pruned_update_{pending_update_rec_dir.parent.name}'}. Model contains {len(cams_r)} cameras, {len(pruned_imgs_r)} images, and {len(pruned_pts_r)} points.")

        # Merging the pruned reference model with the new aligned reconstruction to produce the updated reconstruction.
        cams_r, imgs_r, pts_r, _ = merge_models(
            (cams_r, pruned_imgs_r, pruned_pts_r), # Use the freshly carved reference map
            (cams_u_aligned, imgs_u_aligned, pts_u_aligned)
        )
        if logger.level <= logging.DEBUG:
            validate_model_consistency(cams_r, imgs_r, pts_r, logger=logger)
            os.makedirs(paths.output_path / f"merged_update_{pending_update_rec_dir.parent.name}", exist_ok=True)
            write_model(cams_r, imgs_r, pts_r, paths.output_path / f"merged_update_{pending_update_rec_dir.parent.name}")
            logger.debug(f"Exported merged model to {paths.output_path / f'merged_update_{pending_update_rec_dir.parent.name}'}. Model contains {len(cams_r)} cameras, {len(imgs_r)} images, and {len(pts_r)} points.")


    # Export the merged model for inspection
    os.makedirs(paths.output_path / "updated_sfm", exist_ok=True)
    write_model(cams_r, imgs_r, pts_r, paths.output_path / "updated_sfm")
    logger.debug(f"Exported updated reconstruction to {paths.output_path / 'updated_sfm'}. Model contains {len(cams_r)} cameras, {len(imgs_r)} images, and {len(pts_r)} points.")
    validate_model_consistency(cams_r, imgs_r, pts_r, logger=logger)

    manifest_path = paths.output_path / 'refined_manifest.json'
    portals = {pid: [pose] for pid, pose in portal_r.items()}
    save_manifest_json(
        portals,
        manifest_path,
        paths.parent_dir,
        job_status="refined",
        job_progress=100
    )

    ply_path = paths.refined_group_dir / 'updated' / "RefinedPointCloud.ply"
    rec = pycolmap.Reconstruction()
    for point in pts_r.values():
        x,y,z = point.xyz
        _ = rec.add_point3D(np.array([x,y,z]), pycolmap.Track(), point.rgb)
    export_rec_as_ply(rec, ply_path) # Outputs binary PLY in openCV coords. We convert it to OpenGL in the post_process_ply

    return True

def load_qr_detections_from_local_refinement(rec_dir: Path, logger) -> List[Dict]:
    portals_u_dict = read_portal_csv(rec_dir / "portals.csv")
    portals_u_list = []
    for portal in portals_u_dict.values():
        gl_tvec, gl_qvec = convert_pose_colmap_to_opengl(portal.tvec, portal.qvec)
        portals_u_list.append({
            "short_id": portal.short_id, 
            "tvec": gl_tvec,
            "qvec": gl_qvec,
            "image_id": portal.image_id, 
            "size": portal.size, 
            "corners": portal.corners,
            "pose": pycolmap.Rigid3d(pycolmap.Rotation3d(np.array(gl_qvec)), np.array(gl_tvec))
        })
    chunk_detections_per_qr = _group_detections_by_qr(portals_u_list)
    return _calculate_mean_qr_poses(chunk_detections_per_qr)

def load_partial(
    unzip_folder: Path,
    truth_portal_poses: Dict[str, pycolmap.Rigid3d],
    stitch_data: StitchingData,
    partial_rec_dir: Optional[Path],
    gt_observations: bool = False,
    with_3dpoints: bool = False,
    logger_name: Optional[str] = None
) -> StitchingData:
    """Load and process a partial reconstruction for stitching.
    
    Args:
        unzip_folder: Path to the unzipped dataset folder
        dataset_dir: Path to the datasets directory
        dataset_group: Name of the dataset group
        truth_portal_poses: Ground truth portal poses if available
        stitch_data: Current stitching state data
        partial_rec_dir: Directory containing partial reconstruction
        all_observations: Whether to use all observations
        all_poses: Whether to use all poses
        gt_observations: Whether to use ground truth observations
        with_3dpoints: Whether to include 3D points
        logger_name: Name of logger to use
    
    Returns:
        Updated StitchingData
    """
    logger = logging.getLogger(logger_name)

    # Load refined reconstruction
    loaded_rec = _load_refined_reconstruction(partial_rec_dir, logger)
    if loaded_rec is None:
        return stitch_data
    
    # Extract and process QR code detections
    qr_detections = _process_qr_detections(loaded_rec)

    # Calculate mean QR poses 
    chunk_detections_per_qr = _group_detections_by_qr(qr_detections)
    chunk_mean_qr_poses = _calculate_mean_qr_poses(
        chunk_detections_per_qr, 
        truth_portal_poses if gt_observations else None
    )


    # Find overlapping portals and calculate alignment
    rigid_alignment_transform = _calculate_alignment_transform(
        chunk_mean_qr_poses,
        stitch_data.placed_portal,
        logger,
    )

    alignment_transform = pycolmap.Sim3d(
        1.0,
        rigid_alignment_transform.rotation.quat,
        rigid_alignment_transform.translation
    )

    # Update placed portals
    _update_placed_portals(
        chunk_mean_qr_poses,
        alignment_transform,
        stitch_data.placed_portal,
        logger
    )

    # Process reconstruction
    _process_reconstruction(
        loaded_rec,
        alignment_transform,
        qr_detections,
        stitch_data,
        with_3dpoints,
        logger
    )

    return stitch_data

def _load_refined_reconstruction(partial_rec_dir: Path, logger=None) -> Optional[Model]:
    if logger is None:
        logger = logging.getLogger()

    if not (partial_rec_dir and partial_rec_dir.exists()):
        logger.error(f"No refined data found at: {partial_rec_dir}")
        return None
        
    loaded_rec = Model()
    loaded_rec.read_model(partial_rec_dir, logger=logger)
    logger.info(f"Loaded refined reconstruction from {partial_rec_dir}")
    return loaded_rec

def _load_frame_timestamps(dataset: Path, logger) -> Dict[str, int]:
    frames_csv = dataset / "Frames.csv"
    if not frames_csv.exists():
        logger.info("No Frames.csv found. Skipping!")
        return {}

    use_frames_from_video = (dataset / 'Frames.mp4').exists()
    experiment_name = dataset.name
    
    timestamps = {}
    with open(frames_csv, newline='') as csvfile:
        for i, row in enumerate(csv.reader(csvfile)):
            timestamp = round(float(row[0]) * 1e9)
            if use_frames_from_video:
                filename = f"{experiment_name}_{i:06d}.jpg"
            else:
                filename = row[1]
            timestamps[filename] = timestamp
            
    logger.info(f"Loaded {len(timestamps)} frame timestamps")
    return timestamps

def _process_qr_detections(loaded_rec: Model) -> List[Dict]:
    """Process QR code detections from loaded reconstruction."""
    qr_detections = loaded_rec.get_portals()
    for detection in qr_detections:
        detection["pose"] = pycolmap.Rigid3d(
            pycolmap.Rotation3d(np.array(detection["qvec"])),
            detection["tvec"]
        )
    return qr_detections

def _group_detections_by_qr(qr_detections: List[Dict]) -> Dict[str, List[pycolmap.Rigid3d]]:
    """Group QR detections by QR ID."""
    detections_per_qr = {}
    for detection in qr_detections:
        qr_id = detection["short_id"]
        if qr_id not in detections_per_qr:
            detections_per_qr[qr_id] = [detection["pose"]]
        else:
            detections_per_qr[qr_id].append(detection["pose"])
    return detections_per_qr

def _calculate_mean_qr_poses(
    detections_per_qr: Dict[str, List[pycolmap.Rigid3d]],
    truth_poses: Optional[Dict[str, pycolmap.Rigid3d]] = None
) -> Dict[str, pycolmap.Rigid3d]:
    """Calculate mean poses for each QR code."""
    if truth_poses:
        return {qr_id: truth_poses[qr_id] 
                for qr_id, poses in detections_per_qr.items()}
    return {qr_id: mean_pose(poses) 
            for qr_id, poses in detections_per_qr.items()}


def _calculate_alignment_transform(
    mean_qr_poses: Dict[str, pycolmap.Rigid3d],
    placed_portal: Dict[str, pycolmap.Rigid3d],
    logger
) -> pycolmap.Rigid3d:
    """Calculate alignment transform between current and placed portals."""
    target_poses = {
        qr_id: placed_portal[qr_id]
        for qr_id in mean_qr_poses.keys()
        if qr_id in placed_portal.keys()
    }
    
    has_overlap = len(target_poses) > 0
    is_first_chunk = len(placed_portal) == 0

    if not has_overlap and not is_first_chunk:
        raise NoOverlapException()

    if has_overlap:
        alignment_transforms = [
            target_poses[qr_id] * mean_qr_poses[qr_id].inverse()
            for qr_id in target_poses.keys()
        ]
        return mean_pose(alignment_transforms)

    if is_first_chunk:
        origin_portal_id = list(mean_qr_poses.keys())[0]
        return floor_origin_portal_pose * mean_qr_poses[origin_portal_id].inverse()


def _update_placed_portals(
    mean_qr_poses: Dict[str, pycolmap.Rigid3d],
    alignment_transform: Optional[pycolmap.Sim3d],
    placed_portal: Dict[str, pycolmap.Rigid3d],
    logger
) -> None:
    """Update placed portal positions."""

    for qr_id, pose in mean_qr_poses.items():
        if alignment_transform is not None:
            pose = transform_with_scale(alignment_transform, pose)
        placed_portal[qr_id] = pose
        logger.info(f"Portal: {qr_id} Pose: {pose}")


def transform_with_scale(alignment_transform: pycolmap.Sim3d, pose: pycolmap.Rigid3d) -> pycolmap.Rigid3d:
    pose = pycolmap.Sim3d(1.0, pose.rotation, pose.translation)
    pose = alignment_transform * pose
    return pycolmap.Rigid3d(pose.rotation, pose.translation)

def _process_reconstruction(
    loaded_rec: Model,
    alignment_transform: Optional[pycolmap.Sim3d],
    qr_detections: List[Dict],
    stitch_data: StitchingData,
    with_3dpoints: bool,
    logger
) -> None:
    # Step 1: Load reconstruction and apply alignment
    pycolmap_rec = pycolmap.Reconstruction()
    pycolmap_rec.read(loaded_rec.get_path())
    if alignment_transform is not None:
        pycolmap_rec.transform(alignment_transform)
        for detection in qr_detections:
            detection["pose"] = transform_with_scale(alignment_transform, detection["pose"])

    # Step 2: Copy cameras and images but with incremented IDs
    image_id_old_to_new = {}
    for old_img_id in pycolmap_rec.reg_image_ids():
        old_img = pycolmap_rec.images[old_img_id]
        old_cam_id = old_img.camera_id
        old_cam = pycolmap_rec.cameras[old_cam_id]

        new_id = stitch_data.next_image_id
        
        new_cam = pycolmap.Camera(
            model=old_cam.model,
            width=old_cam.width,
            height=old_cam.height,
            params=old_cam.params,
            camera_id=new_id
        )
        stitch_data.combined_rec.add_camera(new_cam)

        new_rig = pycolmap.Rig()
        new_rig.rig_id = new_id

        sensor = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=new_id)
        new_rig.add_ref_sensor(sensor)
        stitch_data.combined_rec.add_rig(new_rig)

        new_frame = pycolmap.Frame(
            rig_id = new_id,
            rig_from_world = old_img.frame.rig_from_world,
            frame_id = new_id
        )
        new_frame.add_data_id(pycolmap.data_t(
            sensor_id=sensor,
            id=new_id
        ))
        stitch_data.combined_rec.add_frame(new_frame)

        list_point_2d = [pycolmap.Point2D(pt2d.xy) for pt2d in old_img.points2D]
        new_img = pycolmap.Image(
            old_img.name,
            pycolmap.Point2DList(list_point_2d),
            new_id,
            new_id
        )
        new_img.frame_id = new_id
        
        image_id_old_to_new[old_img_id] = new_id

        stitch_data.combined_rec.add_image(new_img)
        stitch_data.combined_rec.register_frame(new_id)
        stitch_data.next_image_id += 1

    # Step 3: Copy 3D points if requested
    if with_3dpoints:
        for point3D in pycolmap_rec.points3D.values():
            point3D_id_new = stitch_data.combined_rec.add_point3D(
                point3D.xyz, 
                pycolmap.Track(), 
                point3D.color
            )
            point3D_track = point3D.track
            for element in point3D_track.elements:
                element.image_id = image_id_old_to_new[element.image_id]
                stitch_data.combined_rec.add_observation(point3D_id_new, element)

    # Process sorted image IDs
    sorted_new_image_ids = sorted(list(image_id_old_to_new.values()))
    stitch_data.chunks_image_ids.append(sorted_new_image_ids)

    # Process QR detections
    for detection in qr_detections:
        qr_id = detection["short_id"]
        if qr_id not in stitch_data.detections_per_qr:
            stitch_data.detections_per_qr[qr_id] = []
        if qr_id not in stitch_data.image_ids_per_qr:
            stitch_data.image_ids_per_qr[qr_id] = []

        cam_space_qr_pose = (
            pycolmap_rec.images[detection["image_id"]].cam_from_world() * 
            detection["pose"]
        )
        stitch_data.detections_per_qr[qr_id].append(cam_space_qr_pose)
        stitch_data.image_ids_per_qr[qr_id].append(
            image_id_old_to_new[detection["image_id"]]
        )

def _initialize_paths(group_folder: Path, function: str = "stitching_helper") -> Paths:
    """Initialize all required paths."""
    parent_dir = group_folder.parent
    
    if function == "stitching_helper":
        output_path = parent_dir / "refined" / "global"
    elif function == "update_helper":
        output_path = parent_dir / "refined" / "updated"
        reference_path = parent_dir / "refined" / "global"
    else:
        raise ValueError(f"Unknown function: {function}")
    
    
    dataset_dir = parent_dir / "datasets"
    refined_group_dir = parent_dir / "refined"

    os.makedirs(refined_group_dir, exist_ok=True)
    os.makedirs(dataset_dir, exist_ok=True)

    return Paths(parent_dir, output_path, dataset_dir, refined_group_dir, reference_path if function == "update_helper" else None)

def _process_datasets(
    dataset_paths: List[Path],
    paths: Paths,
    truth_portal_poses: Dict[str, pycolmap.Rigid3d],
    stitch_data: StitchingData,
    use_refined_outputs: bool,
    with_3dpoints: bool,
    logger

) -> Set[Path]:
    """Process all datasets and attempt to align them."""
    datasets_already_aligned = set()
    datasets_to_align = dataset_paths.copy()
    consecutive_alignment_fails = 0

    stitch_data.portal_sizes = {}
    portal_ids_per_dataset = {}
    while datasets_to_align:
        dataset_path = datasets_to_align.pop(0)
        scan_name = dataset_path.stem

        try:
            unzip_folder = _prepare_dataset(dataset_path, paths.dataset_dir, logger)
            partial_rec_dir = _get_refined_rec_dir(
                use_refined_outputs,
                paths.refined_group_dir,
                scan_name,
                logger
            )

            # Quick overlap check before loading reconstruction, to not waste time
            scanned_portal_ids = portal_ids_per_dataset.get(scan_name, None)
            if scanned_portal_ids is None:
                portals = read_portal_csv(os.path.join(partial_rec_dir, "portals.csv"))
                
                for portal in portals.values():
                    if portal.short_id not in stitch_data.portal_sizes:
                        stitch_data.portal_sizes[portal.short_id] = portal.size

                scanned_portal_ids = [
                    portal.short_id for portal in portals.values()
                ]
                portal_ids_per_dataset[scan_name] = scanned_portal_ids

            has_overlap = len(set(scanned_portal_ids) & set(stitch_data.placed_portal.keys())) > 0
            is_first_chunk = len(stitch_data.placed_portal) == 0
            if not has_overlap and not is_first_chunk:
                raise NoOverlapException()

            stitch_data = load_partial(
                unzip_folder,
                truth_portal_poses,
                stitch_data,
                partial_rec_dir,
                with_3dpoints=with_3dpoints,
                logger_name=logger.name
            )

            consecutive_alignment_fails = 0
            datasets_already_aligned.add(unzip_folder)
            logger.info(f"Aligned {len(datasets_already_aligned)} datasets, {len(datasets_to_align)} remaining")

        except NoOverlapException:
            datasets_to_align.append(dataset_path)
            consecutive_alignment_fails += 1
            
            if consecutive_alignment_fails >= len(datasets_to_align):
                logger.error("Failed to align remaining chunks - no overlaps found")
                break

    return datasets_already_aligned

def _prepare_dataset(dataset_path: Path, dataset_dir: Path, logger) -> Path:
    """Prepare dataset by unzipping if necessary."""
    if dataset_path.suffix.lower() == ".zip":
        unzip_folder = dataset_dir / dataset_path.stem
        if not unzip_folder.exists():
            logger.info(f"Unzipping dataset: {dataset_path}")
            with zipfile.ZipFile(dataset_path, 'r') as zip_ref:
                zip_ref.extractall(dataset_dir)
        return unzip_folder
    return dataset_path

def _get_refined_rec_dir(
    use_refined_outputs: bool,
    refined_group_dir: Path,
    scan_name: str,
    logger
) -> Optional[Path]:
    """Get directory containing refined reconstruction if it exists."""
    if not use_refined_outputs:
        return None

    refined_scan_dir = refined_group_dir / "local" / scan_name
    refined_scan_path = refined_scan_dir / "reconstruction_refined_x1.zip"
    
    if refined_scan_path.exists():
        logger.info(f"Found refined reconstruction: {refined_scan_path}")
        partial_rec_dir = Path(f"/content/partial_rec/{scan_name}")
        with zipfile.ZipFile(refined_scan_path, 'r') as zip_ref:
            zip_ref.extractall(partial_rec_dir)
        return partial_rec_dir
    
    return refined_scan_dir / 'sfm'

def _get_basic_stitch_results(
    stitch_data: StitchingData,
    truth_portal_poses: Dict[str, pycolmap.Rigid3d],
    paths: Paths,
    with_3dpoints: bool,
    logger
) -> StitchResults:
    """Get results from basic stitching."""
    qr_detections = get_world_space_qr_codes(
        stitch_data.combined_rec,
        stitch_data.detections_per_qr,
        stitch_data.image_ids_per_qr
    )
    

    mean_qr_poses = {qr_id: mean_pose(poses) 
                    for qr_id, poses in qr_detections.items()}

    if with_3dpoints:
        ply_path = paths.refined_group_dir / 'global' / "BasicStitchPointCloud.ply"
        export_rec_as_ply(stitch_data.combined_rec, ply_path, logger.name)

    return StitchResults(
        rec=stitch_data.combined_rec,
        detections=qr_detections,
        poses=mean_qr_poses
    )

def _handle_basic_stitch(
    basic_results: StitchResults,
    stitch_data: StitchingData,
    paths: Paths,
    truth_portal_poses: Dict[str, pycolmap.Rigid3d],
    with_3dpoints: bool,
    logger
) -> StitchingResult:
    """Handle case when only basic stitching is requested."""
    #if truth_portal_poses:
    #    compare_portals(
    #        basic_results.poses,
    #        basic_results.poses,
    #        truth_portal_poses,
    #        align=True,
    #        verbose=True,
    #        correct_scale=True
    #    )

    if with_3dpoints:
        src_ply = paths.refined_group_dir / 'global' / "BasicStitchPointCloud.ply"
        dst_ply = paths.refined_group_dir / 'global' / "RefinedPointCloud.ply"
        shutil.copy(src_ply, dst_ply)

        # Save basic stitch sfm
        sfm_dir = paths.output_path / "basic_sfm_combined"
        os.makedirs(sfm_dir, exist_ok=True)
        basic_results.rec.write(sfm_dir)


    manifest_path = paths.output_path / 'refined_manifest.json'
    save_manifest_json(
        {qr_id: [pose] for qr_id, pose in basic_results.poses.items()},
        manifest_path,
        paths.parent_dir,
        job_status="refined",
        job_progress=100,
        portal_sizes=stitch_data.portal_sizes
    )

    return StitchingResult(
        basic_rec=basic_results.rec,
        basic_detections=basic_results.detections,
        basic_poses=basic_results.poses,
        refined_rec=basic_results.rec,
        refined_detections=basic_results.detections,
        refined_poses={qr_id: [pose] for qr_id, pose in basic_results.poses.items()},
        detections_per_qr=stitch_data.detections_per_qr,
        image_ids_per_qr=stitch_data.image_ids_per_qr
    )

def _get_refined_results(
    stitch_data: StitchingData,
    basic_results: StitchResults,
    paths: Paths,
    truth_portal_poses: Dict[str, pycolmap.Rigid3d],
    with_3dpoints: bool,
    logger
) -> StitchResults:
    """Get results from refined stitching."""
    # Align reconstruction chunks
    align_reconstruction_chunks(
        stitch_data.combined_rec,
        stitch_data.chunks_image_ids,
        stitch_data.detections_per_qr,
        stitch_data.image_ids_per_qr,
        with_scale=True,
        logger=logger
    )
    
    # World space QR detections using globally refined camera poses
    refined_detections = get_world_space_qr_codes(
        stitch_data.combined_rec,
        stitch_data.detections_per_qr,
        stitch_data.image_ids_per_qr
    )

    # Calculate mean pose per QR code
    mean_poses = {qr_id: [mean_pose(poses)] 
                 for qr_id, poses in refined_detections.items()}

    # Save results
    manifest_path = paths.output_path / 'refined_manifest.json'
    save_manifest_json(
        mean_poses,
        manifest_path,
        paths.parent_dir,
        job_status="refined",
        job_progress=100,
        portal_sizes=stitch_data.portal_sizes
    )

    if with_3dpoints:
        sfm_dir = paths.output_path / "refined_sfm_combined"
        os.makedirs(sfm_dir, exist_ok=True)
        stitch_data.combined_rec.write(sfm_dir)
        
        ply_path = paths.refined_group_dir / 'global' / "RefinedPointCloud.ply"
        export_rec_as_ply(stitch_data.combined_rec, ply_path) # Outputs binary PLY in openCV coords. We convert it to OpenGL in the post_process_ply

    #if truth_portal_poses:
    #    compare_portals(
    #        basic_results.poses,
    #        {qr_id: poses[0] for qr_id, poses in mean_poses.items()},
    #        truth_portal_poses,
    #        align=True,
    #        verbose=True,
    #        correct_scale=True
    #    )

    return StitchResults(rec=stitch_data.combined_rec, detections=refined_detections, poses=mean_poses)

