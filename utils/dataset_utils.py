from typing import Dict, List, Optional, Tuple
import pycolmap
import os 
from pathlib import Path
import zipfile
import csv
import numpy as np 
import logging
from dataclasses import dataclass

from utils.data_utils import (
    mean_pose,
    convert_pose_opengl_to_colmap,
    convert_pose_colmap_to_opengl,
    save_manifest_json,
    export_rec_as_ply,
    parse_info_from_manifest
)
from utils.io import Model, read_portal_csv, read_model, write_model

# These are Sam's additions to io.py that will be merged there separately.
# Import them so update_helper can use them.
from utils.io import merge_models, apply_similarity_to_new_model, validate_model_consistency
from utils.voxel_raycast_utils import carve_outdated_reference_geometry


# TODO remove in favor of scan_alignment.py
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
class Paths:
    parent_dir: Path
    output_path: Path
    dataset_dir: Path
    refined_group_dir: Path
    reference_path: Optional[Path] = None  # Path for reference reconstruction (global refinement set as canonical); only used in update_helper


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


# TODO remove in favor of scan_alignment.py
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


def transform_with_scale(alignment_transform: pycolmap.Sim3d, pose: pycolmap.Rigid3d) -> pycolmap.Rigid3d:
    pose = pycolmap.Sim3d(1.0, pose.rotation, pose.translation)
    pose = alignment_transform * pose
    return pycolmap.Rigid3d(pose.rotation, pose.translation)


def _initialize_paths(job_root_path: Path, function: str = "stitching_helper") -> Paths:
    """Initialize all required paths.
    
    When called as _initialize_paths(group_folder) with a single arg (legacy/develop API),
    parent_dir is group_folder.parent and output is under refined/global.
    
    When called with function="update_helper", job_root_path IS the parent_dir,
    and output goes to refined/update with a reference_path at refined/global.
    """
    if function == "stitching_helper":
        # Legacy behavior: group_folder.parent is the root
        parent_dir = job_root_path.parent
        output_path = parent_dir / "refined" / "global"
        reference_path = None
    elif function == "update_helper":
        parent_dir = job_root_path
        output_path = parent_dir / "refined" / "update"
        reference_path = parent_dir / "refined" / "global"
    else:
        raise ValueError(f"Unknown function: {function}")

    dataset_dir = parent_dir / "datasets"
    refined_group_dir = parent_dir / "refined"

    os.makedirs(refined_group_dir, exist_ok=True)
    os.makedirs(dataset_dir, exist_ok=True)

    return Paths(parent_dir, output_path, dataset_dir, refined_group_dir, reference_path)


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


def load_qr_detections_from_local_refinement(rec_dir: Path, logger) -> Tuple[Dict[str, pycolmap.Rigid3d], Dict[str, float]]:
    """Load QR portal detections from a local refinement's portals.csv.
    
    Reads the portal CSV, converts poses to OpenGL convention, groups by QR ID,
    and returns mean poses per QR along with portal sizes.
    
    Args:
        rec_dir: Path to the local refined reconstruction directory containing portals.csv
        logger: Logger instance
        
    Returns:
        Tuple of (mean_qr_poses, portal_sizes) where mean_qr_poses maps QR short_id
        to mean Rigid3d pose and portal_sizes maps QR short_id to size float.
    """
    # develop's read_portal_csv returns List[Portal], not Dict[int, Portal]
    portals_list = read_portal_csv(rec_dir / "portals.csv")
    portals_u_list = []
    portal_sizes = {}
    for portal in portals_list:
        portal_sizes[portal.short_id] = portal.size
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
    return _calculate_mean_qr_poses(chunk_detections_per_qr), portal_sizes


def update_helper(
    dataset_paths: List[Path],
    job_root_path: Path,
    logger_name: Optional[str] = None
) -> bool:
    """Main function to merge new scan reconstructions into an existing reference model.
    
    For each new dataset/scan:
    1. Loads the locally-refined reconstruction and its portal detections
    2. Aligns it to the reference model via shared QR portals
    3. Carves outdated geometry from the reference using new free-space constraints
    4. Merges the aligned new reconstruction into the (pruned) reference
    5. Adds any new portal poses (existing portals are preserved for stability)
    
    Finally exports the updated reconstruction, manifest, and point cloud.
    
    Args:
        dataset_paths: List of paths to datasets (zip files or directories) to merge
        job_root_path: Path to the root folder for the update job
        logger_name: Name of logger to use

    Returns:
        True on success, False if no dataset paths found.
    """

    logger = logging.getLogger(logger_name)

    # Initialize paths and data
    paths = _initialize_paths(job_root_path, "update_helper")

    pending_update_rec = []

    dataset_rec_paths = [
        _get_refined_rec_dir(
            True,
            paths.refined_group_dir,
            scan_name,
            logger
        ) for scan_name in [path.stem for path in dataset_paths]]

    if len(dataset_rec_paths) > 1:
        logger.info("Multiple dataset paths found. Proceeding with stitching.")
        # TODO: Replace this function with bundle scans and perform basic stitch.
        pending_update_rec.extend(dataset_rec_paths)
    elif len(dataset_rec_paths) == 1:
        pending_update_rec.append(dataset_rec_paths[0])
        logger.info("Only one dataset path found. Skipping stitching and preparing for update refinement.")
    else:
        logger.error("No dataset paths found. Exiting.")
        return False
    
    # Loading the reference model that will be refined. This should be the model
    # set to be canonical, which is the latest colmap model of the domain.
    cams_r, imgs_r, pts_r = read_model(paths.reference_path / "refined_sfm_combined", ".bin", logger=logger)
    portal_r, refined_files_r = parse_info_from_manifest(paths.reference_path / "refined_manifest.json")  # return a dict of portal_id -> (R, t, size)
    portal_sizes = {pid: portal[2] for pid, portal in portal_r.items()}
    portal_r = {pid: pycolmap.Rigid3d(pycolmap.Rotation3d(portal_r[pid][0]), portal_r[pid][1]) for pid in portal_r.keys()}

    # Process datasets
    for pending_update_rec_dir in pending_update_rec:
        logger.info(f"Processing dataset for update refinement: {pending_update_rec_dir}")

        # Loading the new reconstruction that contains the new geometry to be merged in. 
        # This should be the local refined reconstruction of the new scan.
        cams_u, imgs_u, pts_u = read_model(pending_update_rec_dir, ".bin", logger=logger)
        portals_u, portal_sizes_u = load_qr_detections_from_local_refinement(pending_update_rec_dir, logger)
        
        # Align the new reconstruction to the reference model using detected QR code portals as anchors.
        # This gives a rough alignment good enough for culling outdated geometry from the reference model.
        alignment_mat = _calculate_alignment_transform(portals_u, portal_r, logger)
        logger.info(f"Calculated alignment transform for update refinement: \n{alignment_mat.matrix()}")
        cams_u_aligned, imgs_u_aligned, pts_u_aligned = apply_similarity_to_new_model(cams_u, imgs_u, pts_u, alignment_mat.matrix())

        # Prune the reference model by carving out points that violate new free-space constraints.
        pruned_imgs_r, pruned_pts_r = carve_outdated_reference_geometry(
            ref_imgs=imgs_r,
            ref_pts=pts_r,
            new_imgs=imgs_u_aligned,
            new_pts=pts_u_aligned,
            voxel_size=0.15,         # Adjust based on scene scale (e.g. 10cm)
            clearance_margin=0.1,    # Stop 10cm before the target to avoid false collisions
            min_surviving_points=50, # Drop old images with < 50 valid points left
            logger=logger
        )
        logger.info(f"Pruned reference model has {len(pruned_imgs_r)} images and {len(pruned_pts_r)} points (out of original {len(imgs_r)} images and {len(pts_r)} points).")
        if logger.level <= logging.DEBUG:
            validate_model_consistency(cams_r, pruned_imgs_r, pruned_pts_r, logger=logger)
            os.makedirs(paths.output_path / f"pruned_update_{pending_update_rec_dir.parent.name}", exist_ok=True)
            write_model(cams_r, pruned_imgs_r, pruned_pts_r, paths.output_path / f"pruned_update_{pending_update_rec_dir.parent.name}")
            logger.debug(f"Exported pruned reference model to {paths.output_path / f'pruned_update_{pending_update_rec_dir.parent.name}'}. Model contains {len(cams_r)} cameras, {len(pruned_imgs_r)} images, and {len(pruned_pts_r)} points.")

        # Merge the pruned reference model with the new aligned reconstruction.
        cams_r, imgs_r, pts_r, _ = merge_models(
            (cams_r, pruned_imgs_r, pruned_pts_r),  # Use the freshly carved reference map
            (cams_u_aligned, imgs_u_aligned, pts_u_aligned)
        )
        if logger.level <= logging.DEBUG:
            validate_model_consistency(cams_r, imgs_r, pts_r, logger=logger)
            os.makedirs(paths.output_path / f"merged_update_{pending_update_rec_dir.parent.name}", exist_ok=True)
            write_model(cams_r, imgs_r, pts_r, paths.output_path / f"merged_update_{pending_update_rec_dir.parent.name}")
            logger.debug(f"Exported merged model to {paths.output_path / f'merged_update_{pending_update_rec_dir.parent.name}'}. Model contains {len(cams_r)} cameras, {len(imgs_r)} images, and {len(pts_r)} points.")

        # Transform and Merge Portals.
        # Only add new portals; do not modify existing portals in the reference model
        # to avoid instability of portal poses across updates.
        for pid, portal in portals_u.items():
            if pid in portal_r:
                logger.info(f"Portal {pid} already exists in reference model. Skipping.")
                continue
            alignment_sim3d = pycolmap.Sim3d(1.0, alignment_mat.rotation, alignment_mat.translation)
            transformed_portal = transform_with_scale(alignment_sim3d, portal)
            portal_r[pid] = transformed_portal
            portal_sizes[pid] = portal_sizes_u[pid]

    # Export the merged model for inspection
    os.makedirs(paths.output_path / "refined_sfm_combined", exist_ok=True)
    write_model(cams_r, imgs_r, pts_r, paths.output_path / "refined_sfm_combined")
    logger.debug(f"Exported updated reconstruction to {paths.output_path / 'refined_sfm_combined'}. Model contains {len(cams_r)} cameras, {len(imgs_r)} images, and {len(pts_r)} points.")
    validate_model_consistency(cams_r, imgs_r, pts_r, logger=logger)

    manifest_path = paths.output_path / 'refined_manifest.json'
    portals_opengl = {pid: convert_pose_colmap_to_opengl(portal.translation, portal.rotation.quat) for pid, portal in portal_r.items()}
    portals = {pid: [pycolmap.Rigid3d(pycolmap.Rotation3d(np.array(pose[1])), np.array(pose[0]))] for pid, pose in portals_opengl.items()}
    save_manifest_json(
        portals,
        manifest_path,
        paths.parent_dir,
        job_status="refined",
        job_progress=100,
        portal_sizes=portal_sizes,
        previous_scan_files=refined_files_r
    )

    ply_path = paths.refined_group_dir / 'update' / "RefinedPointCloud.ply"
    rec = pycolmap.Reconstruction()
    for point in pts_r.values():
        x, y, z = point.xyz
        _ = rec.add_point3D(np.array([x, y, z]), pycolmap.Track(), point.rgb)
    export_rec_as_ply(rec, ply_path)  # Outputs binary PLY in openCV coords. We convert it to OpenGL in the post_process_ply

    return True
