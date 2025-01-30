from typing import Dict, List, Optional, Set
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

from evo.main_ape import ape as evo_ape
from evo.core.trajectory import PosePath3D
from evo.core.trajectory import geometry
from evo.core.metrics import PoseRelation
from evo.core import lie_algebra as evo_lie
import matplotlib.pyplot as plt

from utils.data_utils import (
    mean_pose,
    convert_pose_opengl_to_colmap, 
    precompute_arkit_offsets, 
    get_world_space_qr_codes,
    save_manifest_json,
    export_rec_as_ply,
    flatten_quaternion,
    flatten_portal_rotation
)
from utils.geometry_utils import align_reconstruction_chunks, run_stitching
from utils.io import Model


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

    def __post_init__(self):
        self.detections_per_qr = self.detections_per_qr or {}
        self.image_ids_per_qr = self.image_ids_per_qr or {}
        self.timestamp_per_image = self.timestamp_per_image or {}
        self.arkit_precomputed = self.arkit_precomputed or {}
        self.placed_portal = self.placed_portal or {}
        self.chunks_image_ids = self.chunks_image_ids or []
        self.combined_rec = self.combined_rec or pycolmap.Reconstruction()

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
    basic_results = _get_basic_stitch_results(
        stitch_data,
        truth_portal_poses,
        paths,
        with_3dpoints,
        logger
    )

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
    refined_results = _get_refined_results(
        stitch_data,
        basic_results,
        paths,
        truth_portal_poses,
        with_3dpoints,
        logger
    )

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
    
    # Load frame timestamps
    timestamp_chunk = _load_frame_timestamps(unzip_folder, logger)
    if not timestamp_chunk:
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
    alignment_transform = _calculate_alignment_transform(
        chunk_mean_qr_poses,
        stitch_data.placed_portal,
        logger,
        rectify_portals=False
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
        timestamp_chunk,
        stitch_data,
        with_3dpoints,
        logger
    )

    return stitch_data

def _load_refined_reconstruction(partial_rec_dir: Path, logger) -> Optional[Model]:
    if not (partial_rec_dir and partial_rec_dir.exists()):
        logger.error(f"No refined data found at: {partial_rec_dir}")
        return None
        
    loaded_rec = Model()
    loaded_rec.read_model(partial_rec_dir)
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
    logger,
    rectify_portals: bool = False
) -> pycolmap.Rigid3d:
    """Calculate alignment transform between current and placed portals."""
    target_poses = {
        qr_id: placed_portal[qr_id]
        for qr_id in mean_qr_poses.keys()
        if qr_id in placed_portal.keys()
    }
    
    has_overlap = len(target_poses) > 0
    is_first_chunk = len(placed_portal) == 0

    if has_overlap:
        alignment_transforms = []
        for qr_id in target_poses.keys():
            from_pose = mean_qr_poses[qr_id]
            to_pose = target_poses[qr_id]
            if rectify_portals:
                from_pose.rotation = pycolmap.Rotation3d(flatten_portal_rotation(from_pose.rotation.matrix()))
                to_pose.rotation = pycolmap.Rotation3d(flatten_portal_rotation(to_pose.rotation.matrix()))
            
            transform = to_pose * from_pose.inverse()
            if rectify_portals:
                transform.rotation.quat = flatten_quaternion(transform.rotation.quat)
            alignment_transforms.append(transform)

        
        alignment_transform = mean_pose(alignment_transforms)
        if rectify_portals:
            alignment_transform.rotation.quat = flatten_quaternion(alignment_transform.rotation.quat)
        
        return alignment_transform
    
    if is_first_chunk:
        origin_id = list(mean_qr_poses.keys())[0]
        return floor_origin_portal_pose * mean_qr_poses[origin_id].inverse()
    
    raise NoOverlapException()

def _update_placed_portals(
    mean_qr_poses: Dict[str, pycolmap.Rigid3d],
    alignment_transform: Optional[pycolmap.Rigid3d],
    placed_portal: Dict[str, pycolmap.Rigid3d],
    logger
) -> None:
    """Update placed portal positions."""
    for qr_id, pose in mean_qr_poses.items():
        if alignment_transform is not None:
            pose = alignment_transform * pose
        placed_portal[qr_id] = pose
        logger.info(f"Portal: {qr_id} Pose: {pose}")

def _process_reconstruction(
    loaded_rec: Model,
    alignment_transform: Optional[pycolmap.Rigid3d],
    qr_detections: List[Dict],
    timestamp_chunk: Dict[str, int],
    stitch_data: StitchingData,
    with_3dpoints: bool,
    logger
) -> None:
    """Process and combine reconstructions."""
    pycolmap_rec = pycolmap.Reconstruction()
    pycolmap_rec.read(loaded_rec.get_path())
    if alignment_transform is not None:
        pycolmap_rec.transform(pycolmap.Sim3d(
            1.0, 
            alignment_transform.rotation.quat, 
            alignment_transform.translation
        ))
        for detection in qr_detections:
            detection["pose"] = alignment_transform * detection["pose"]

    image_id_old_to_new = {}
    arkit_cam_from_world_transforms = {}
    rec2 = pycolmap.Reconstruction()

    # Process images
    for i in range(1, pycolmap_rec.num_images() + 1):
        image_id = stitch_data.next_image_id
        camera_id = image_id
        
        # Add camera
        cam = pycolmap_rec.cameras[i]
        cam2 = pycolmap.Camera(
            model=cam.model,
            width=cam.width,
            height=cam.height,
            params=cam.params,
            camera_id=camera_id
        )
        stitch_data.combined_rec.add_camera(cam2)
        rec2.add_camera(cam2)

        # Add image
        img = pycolmap_rec.images[i]
        list_point_2d = [pycolmap.Point2D(pt2d.xy) for pt2d in img.points2D]
        img2 = pycolmap.Image(
            img.name,
            pycolmap.ListPoint2D(list_point_2d),
            img.cam_from_world,
            camera_id,
            image_id
        )
        stitch_data.combined_rec.add_image(img2)
        stitch_data.combined_rec.register_image(image_id)
        rec2.add_image(img2)
        rec2.register_image(image_id)

        image_id_old_to_new[i] = image_id
        arkit_cam_from_world_transforms[image_id] = img.cam_from_world.inverse()
        stitch_data.next_image_id += 1

    # Add 3D points if requested
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

    # Update timestamps
    for filename, timestamp in timestamp_chunk.items():
        assert filename not in stitch_data.timestamp_per_image
        stitch_data.timestamp_per_image[filename] = timestamp

    # Process sorted image IDs
    sorted_image_ids = sorted(list(rec2.images.keys()))
    stitch_data.chunks_image_ids.append(sorted_image_ids)

    # Precompute ARKit offsets
    stitch_data.arkit_precomputed = precompute_arkit_offsets(
        sorted_image_ids,
        arkit_cam_from_world_transforms,
        stitch_data.arkit_precomputed
    )

    # Process QR detections
    for detection in qr_detections:
        qr_id = detection["short_id"]
        if qr_id not in stitch_data.detections_per_qr:
            stitch_data.detections_per_qr[qr_id] = []
        if qr_id not in stitch_data.image_ids_per_qr:
            stitch_data.image_ids_per_qr[qr_id] = []

        cam_space_qr_pose = (
            pycolmap_rec.images[detection["image_id"]].cam_from_world * 
            detection["pose"]
        )
        stitch_data.detections_per_qr[qr_id].append(cam_space_qr_pose)
        stitch_data.image_ids_per_qr[qr_id].append(
            image_id_old_to_new[detection["image_id"]]
        )

def _initialize_paths(group_folder: Path) -> Paths:
    """Initialize all required paths."""
    parent_dir = group_folder.parent
    output_path = parent_dir / "refined" / "global"
    dataset_dir = parent_dir / "datasets"
    refined_group_dir = parent_dir / "refined"

    os.makedirs(refined_group_dir, exist_ok=True)
    os.makedirs(dataset_dir, exist_ok=True)

    return Paths(parent_dir, output_path, dataset_dir, refined_group_dir)

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
    if truth_portal_poses:
        compare_portals(
            basic_results.poses,
            basic_results.poses,
            truth_portal_poses,
            align=True,
            verbose=True,
            correct_scale=True
        )

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
        job_progress=100
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
        with_scale=False
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
        job_progress=100
    )

    if with_3dpoints:
        sfm_dir = paths.output_path / "refined_sfm_combined"
        os.makedirs(sfm_dir, exist_ok=True)
        stitch_data.combined_rec.write(sfm_dir)
        
        ply_path = paths.refined_group_dir / 'global' / "RefinedPointCloud.ply"
        export_rec_as_ply(stitch_data.combined_rec, ply_path)

    if truth_portal_poses:
        compare_portals(
            basic_results.poses,
            {qr_id: poses[0] for qr_id, poses in mean_poses.items()},
            truth_portal_poses,
            align=True,
            verbose=True,
            correct_scale=True
        )

    return StitchResults(rec=stitch_data.combined_rec, detections=refined_detections, poses=mean_poses)


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

