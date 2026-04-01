from typing import Dict, List, Optional
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
    convert_pose_opengl_to_colmap
)
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
class Paths:
    parent_dir: Path
    output_path: Path
    dataset_dir: Path
    refined_group_dir: Path


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


def _initialize_paths(group_folder: Path) -> Paths:
    """Initialize all required paths."""
    parent_dir = group_folder.parent
    output_path = parent_dir / "refined" / "global"
    dataset_dir = parent_dir / "datasets"
    refined_group_dir = parent_dir / "refined"

    os.makedirs(refined_group_dir, exist_ok=True)
    os.makedirs(dataset_dir, exist_ok=True)

    return Paths(parent_dir, output_path, dataset_dir, refined_group_dir)


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
