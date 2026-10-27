"""Initial alignment of scans through shared portals."""
import pycolmap
import numpy as np
import logging
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass
from reconstruction.geometry.pose import transform_with_scale
from reconstruction.geometry.frames import COLMAP_WORLD, OPENGL, convert_pose_convention
from reconstruction.geometry.pose import flatten_portal_rotation, is_portal_almost_flat, mean_pose
from reconstruction.io.portals import portalPose, read_portal_csv
from reconstruction.layout import JobLayout


@dataclass
class AlignedScans:
    scan_ids: List[str]
    alignment_transforms: Dict[str, pycolmap.Sim3d]
    raw_portal_detections: Dict[str, Dict[str, List[pycolmap.Rigid3d]]]
    aligned_portal_detections: Dict[str, Dict[str, List[pycolmap.Rigid3d]]]


class NoOverlapException(Exception):
    def __init__(self, message='No overlaps!'):
        # Call the base class constructor with the parameters it needs
        super(NoOverlapException, self).__init__(message)


floor_origin_portal_pose_GL = pycolmap.Rigid3d(
    pycolmap.Rotation3d(np.array([-0.7071068, 0.0, 0.0, 0.7071068])),
    np.array([0.0, 0.0, 0.0]))


p, q = convert_pose_convention(np.array([0.0, 0.0, 0.0]), np.array([-0.7071068, 0.0, 0.0, 0.7071068]), OPENGL, COLMAP_WORLD)


floor_origin_portal_pose = pycolmap.Rigid3d(pycolmap.Rotation3d(q), p)


def rigid_to_sim3(transform: pycolmap.Rigid3d) -> pycolmap.Sim3d:
    return pycolmap.Sim3d(1.0, transform.rotation, transform.translation)


def calculate_alignment_transform(
    scanned_poses: Dict[str, pycolmap.Rigid3d],
    placed_poses: Dict[str, pycolmap.Rigid3d],
    logger
) -> pycolmap.Rigid3d:
    """Calculate alignment transform between current and placed portals."""
    scanned_ids = list(scanned_poses.keys())

    target_poses = {
        qr_id: placed_poses[qr_id]
        for qr_id in scanned_ids
        if qr_id in placed_poses.keys()
    }

    has_overlap = len(target_poses) > 0
    is_first_chunk = len(placed_poses) == 0

    if not has_overlap and not is_first_chunk:
        # All chunks except for the first must overlap
        raise NoOverlapException()

    if has_overlap:
        alignment_transforms = [
            target_poses[qr_id] * scanned_poses[qr_id].inverse()
            for qr_id in target_poses.keys()
        ]
        return mean_pose(alignment_transforms)

    if is_first_chunk:
        first_floor_portal_id = None
        for qr_id, pose in scanned_poses.items():
            if is_portal_almost_flat(pose.rotation.matrix()):
                first_floor_portal_id = qr_id
                break
            logger.info(f"Skip non-horizontal origin portal candi: {qr_id}")

        if first_floor_portal_id is None:
            # First scan must not align using a wall portal, as that would tilt the entire reconstruction!
            logger.info("No floor portal found for origin alignment")
            raise NoOverlapException("No floor portal found for origin alignment")

        logger.info(f"Using {first_floor_portal_id} as origin portal to align the reconstruction. Pose: R={scanned_poses[first_floor_portal_id].rotation.matrix()}, t={scanned_poses[first_floor_portal_id].translation}")

        # Align only rotating around UP (x in our colmap space), if floor portal is not completely flat don't tilt the entire scan.
        scanned_pose = scanned_poses[first_floor_portal_id]
        rot = scanned_pose.rotation.matrix()
        rot = flatten_portal_rotation(rot)
        scanned_pose_flat = pycolmap.Rigid3d(pycolmap.Rotation3d(rot), scanned_pose.translation)
        alignment_transform = floor_origin_portal_pose * scanned_pose_flat.inverse()

        return alignment_transform

    raise NoOverlapException()


def align_scans(
    scan_ids: List[str],
    job_root_path: Path,
    logger: Optional[logging.Logger] = None
):
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("Basic scan alignment from overlapping portals...")

    layout = JobLayout(job_root_path)

    scans_already_aligned: List[str] = []
    scans_to_align: List[str] = scan_ids.copy()
    consecutive_alignment_fails = 0
    placed_portal_poses: Dict[str, pycolmap.Rigid3d] = {}

    alignment_transforms = {}
    raw_portal_detections_by_scan = {}
    aligned_portal_detections_by_scan = {}
    while scans_to_align:
        try:

            scanID = scans_to_align.pop(0)
            refined_scan_path = layout.refined_sfm(scanID)
            portals_csv_path = refined_scan_path / "portals.csv"
            if not portals_csv_path.exists():
                logger.error(f"Portals CSV file not found for scan {scanID} (path: {portals_csv_path})")
                continue

            portal_rows = read_portal_csv(portals_csv_path)
            detected_portal_poses: Dict[str, List[pycolmap.Rigid3d]] = {}
            for portal in portal_rows:
                if portal.short_id not in detected_portal_poses:
                    detected_portal_poses[portal.short_id] = []
                detected_portal_poses[portal.short_id].append(portalPose(portal))

            detected_mean_portal_poses: Dict[str, pycolmap.Rigid3d] = {
                qr_id: mean_pose(poses)
                for qr_id, poses in detected_portal_poses.items()
            }

            rigid_alignment_transform = calculate_alignment_transform(
                detected_mean_portal_poses,
                placed_portal_poses,
                logger,
            )

            # Alignement found! Add to results (don't apply transform yet)
            alignment_transforms[scanID] = rigid_to_sim3(rigid_alignment_transform)
            raw_portal_detections_by_scan[scanID] = detected_portal_poses
            aligned_portal_detections_by_scan[scanID] = {
                qr_id: [
                    transform_with_scale(alignment_transforms[scanID], pose)
                    for pose in poses
                ]
                for qr_id, poses in detected_portal_poses.items()
            }
            scans_already_aligned.append(scanID)
            consecutive_alignment_fails = 0
            logger.info(f"Alignment transform for scan {scanID}: {alignment_transforms[scanID]}")

            for qr_id, poses in aligned_portal_detections_by_scan[scanID].items():
                placed_portal_poses[qr_id] = mean_pose(poses)

        except NoOverlapException:
            consecutive_alignment_fails += 1
            if consecutive_alignment_fails > len(scans_to_align):
                # All failed, no more overlappin scans.
                logger.info(f"All remaining {len(scans_to_align)} scans have no overlap with the previous {len(scans_already_aligned)} scans. STOPPING!")
                raise

            # Add back to queue to retry again at the end after all others (may have more overlap after align other scans first)
            logger.info(f"No overlap with the previous {len(scans_already_aligned)} scans. Adding back to queue to retry again at the end after all others (may have more overlap after align other scans first)")
            scans_to_align.append(scanID)

    logger.info(f"Alignment transforms found for {len(alignment_transforms)} scans: {alignment_transforms}")

    return AlignedScans(
        scan_ids=scans_already_aligned,
        alignment_transforms=alignment_transforms,
        raw_portal_detections=raw_portal_detections_by_scan,
        aligned_portal_detections=aligned_portal_detections_by_scan,
    )
