import itertools
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import pycolmap
import pyceres
import numpy as np
from utils.io import read_portal_csv, portalPose
from utils.data_utils import mean_pose, convert_pose_opengl_to_colmap
from utils.dataset_utils import transform_with_scale
from utils.geometry_utils import QuaternionNormalizationCostFunction
from src.cost_functions import RelativeTransformationSim3CostFunction # Custom ceres cost implementated in C++
from src.reconstruction_merge import append_reconstruction

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
p, q = convert_pose_opengl_to_colmap(np.array([0.0, 0.0, 0.0]), np.array([-0.7071068, 0.0, 0.0, 0.7071068]))
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
        origin_portal_id = list(scanned_poses.keys())[0]
        return floor_origin_portal_pose * scanned_poses[origin_portal_id].inverse()

def align_scans(
    scan_ids: List[str],
    job_root_path: Path,
    logger: Optional[logging.Logger] = None
):
    if logger is None:
        logger = logging.getLogger("scan_alignment")
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(sys.stdout))

    logger.info("Basic scan alignment from overlapping portals...")

    local_refined_dir = job_root_path / "refined" / "local"

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
            refined_scan_path = local_refined_dir / scanID / "sfm"
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


def refine_alignment(
    aligned_scans: AlignedScans,
    job_root_path: Path,
    logger: Optional[logging.Logger] = None
) -> AlignedScans:
    if logger is None:
        logger = logging.getLogger("scan_alignment")
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.info("Going to optimize chunk alignment...")
    scan_ids = aligned_scans.scan_ids
    t_local_scan_quat = [pycolmap.Rigid3d().rotation.quat for _ in range(len(scan_ids))]
    t_local_scan_translation = [pycolmap.Rigid3d().translation for _ in range(len(scan_ids))]
    problem = pyceres.Problem()

    #loss = pyceres.HuberLoss(0.1)
    loss = None

    qr_ids_per_scan = [set() for _ in range(len(scan_ids))]
    connected_scans = [set() for _ in range(len(scan_ids))]
    detections_per_qr: Dict[str, List[tuple[int, pycolmap.Rigid3d]]] = {}
    for scan_idx, scan_id in enumerate(scan_ids):
        for qr_id, aligned_qr_poses in aligned_scans.aligned_portal_detections[scan_id].items():
            if qr_id not in detections_per_qr:
                detections_per_qr[qr_id] = []
            detections_per_qr[qr_id].extend(
                (scan_idx, aligned_qr_pose)
                for aligned_qr_pose in aligned_qr_poses
            )

    for qr_id, world_space_detections in detections_per_qr.items():
        scan_pairs = set(itertools.combinations(world_space_detections, 2))
        for (scan_idx_ref, t_refworld_qr), (scan_idx_tgt, t_tgtworld_qr) in scan_pairs:
            if scan_idx_ref == scan_idx_tgt:
                continue

            cov = np.eye(6)
            cov[3:, 3:] *= 0.01

            # First relative to second
            cost_1 = RelativeTransformationSim3CostFunction(t_refworld_qr.rotation.quat,
                                                          t_refworld_qr.translation,
                                                          t_tgtworld_qr.rotation.quat,
                                                          t_tgtworld_qr.translation, cov)

            params_1 = [
                t_local_scan_quat[scan_idx_tgt],
                t_local_scan_translation[scan_idx_tgt],
                t_local_scan_quat[scan_idx_ref],
                t_local_scan_translation[scan_idx_ref]
            ]

            problem.add_residual_block(cost_1, loss, params_1)

            # Second relative to first (to ensure scale impacts in both ways symetrically)
            cost_2 = RelativeTransformationSim3CostFunction(t_tgtworld_qr.rotation.quat,
                                                          t_tgtworld_qr.translation,
                                                          t_refworld_qr.rotation.quat,
                                                          t_refworld_qr.translation, cov)
            params_2 = [
                t_local_scan_quat[scan_idx_ref],
                t_local_scan_translation[scan_idx_ref],
                t_local_scan_quat[scan_idx_tgt],
                t_local_scan_translation[scan_idx_tgt]
            ]
            problem.add_residual_block(cost_2, loss, params_2)

            qr_ids_per_scan[scan_idx_ref].add(qr_id)
            qr_ids_per_scan[scan_idx_tgt].add(qr_id)
            connected_scans[scan_idx_ref].add(scan_idx_tgt)
            connected_scans[scan_idx_tgt].add(scan_idx_ref)

    for scan_idx in range(len(scan_ids)):
        if len(qr_ids_per_scan[scan_idx]) < 2:
            quat = t_local_scan_quat[scan_idx]
            logger.info(f"Scan {scan_idx} has less than 2 correspondences, fixing scale")
            if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
                problem.set_manifold(quat, pyceres.QuaternionManifold())
        else:
            # Keep scale close to 1.0 while still allowing the solver
            # to absorb the residual scan-to-scan scale drift.
            weight = 5000.0
            scale_cost = QuaternionNormalizationCostFunction(weight=weight)
            params = [t_local_scan_quat[scan_idx]]
            problem.add_residual_block(scale_cost, None, params)

    solver_options = pyceres.SolverOptions()
    solver_options.linear_solver_type = pyceres.LinearSolverType.SPARSE_NORMAL_CHOLESKY
    solver_options.minimizer_progress_to_stdout = False
    solver_options.function_tolerance = 0.0
    solver_options.gradient_tolerance = 0.0
    solver_options.parameter_tolerance = 0.0
    solver_options.max_num_iterations = 100
    solver_options.logging_type = pyceres.LoggingType.SILENT

    summary = pyceres.SolverSummary()
    pyceres.solve(solver_options, problem, summary)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"{summary.FullReport()}")
    else:
        logger.info(f"{summary.BriefReport()}")

    t_local_scans = [pycolmap.Sim3d(pycolmap.Rotation3d(quat).norm()**2, pycolmap.Rotation3d(quat), translation) for quat, translation in zip(t_local_scan_quat, t_local_scan_translation)]
    for t_local_scan in t_local_scans:
        t_local_scan.rotation.normalize()

    logger.debug('Refined Sim3 transforms:')
    for scan_idx, t_local_scan in enumerate(t_local_scans):
        logger.debug(f'Scan {scan_idx} ({scan_ids[scan_idx]}): {t_local_scan}')

    refined_alignment_transforms = {
        scan_id: t_local_scans[scan_idx] * aligned_scans.alignment_transforms[scan_id]
        for scan_idx, scan_id in enumerate(scan_ids)
    }
    refined_aligned_portal_detections = {
        scan_id: {
            qr_id: [
                transform_with_scale(refined_alignment_transforms[scan_id], pose)
                for pose in poses
            ]
            for qr_id, poses in aligned_scans.raw_portal_detections[scan_id].items()
        }
        for scan_id in scan_ids
    }

    logger.info("Scan alignment refined successfully!")
    return AlignedScans(
        scan_ids=scan_ids,
        alignment_transforms=refined_alignment_transforms,
        raw_portal_detections=aligned_scans.raw_portal_detections,
        aligned_portal_detections=refined_aligned_portal_detections,
    )


def merge_aligned_scans(
    aligned_scans: AlignedScans,
    job_root_path: Path,
    logger: Optional[logging.Logger] = None
) -> pycolmap.Reconstruction:
    if logger is None:
        logger = logging.getLogger("scan_alignment")
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(sys.stdout))

    combined_rec = pycolmap.Reconstruction()

    for i, scan_id in enumerate(aligned_scans.scan_ids):
        scan_path = job_root_path / "refined" / "local" / scan_id / "sfm"
        rec = pycolmap.Reconstruction()
        logger.info(f"Align and merge scan {i} / {len(aligned_scans.scan_ids)}: {scan_id}")
        logger.debug(f"-- Reading ...")
        rec.read(scan_path)
        logger.debug(f"-- Aligning ...")
        rec.transform(aligned_scans.alignment_transforms[scan_id])
        logger.debug(f"-- Merging ...")
        append_reconstruction(combined_rec, rec)

        logger.debug(f"-- MERGED! Combined size: {combined_rec.num_images()} images, "
            f"{combined_rec.num_frames()} frames, {combined_rec.num_rigs()} rigs, "
            f"{combined_rec.num_cameras()} cameras, {combined_rec.num_points3D()} points3D")

    logger.info(f"Merged {len(aligned_scans.scan_ids)} scans successfully: {combined_rec}")
    return combined_rec


def print_alignment_comparison(
    aligned_scans: AlignedScans,
    refined_aligned_scans: AlignedScans,
    logger: logging.Logger
) -> Tuple[Dict[str, float], Dict[str, float], List[str]]:
    """Print comparison of alignment transforms between aligned and refined aligned scans."""
    move_deltas = {} # cm
    rotation_deltas = {} # degrees
    for scan_id in aligned_scans.scan_ids:
        aligned_transform = aligned_scans.alignment_transforms[scan_id]
        refined_transform = refined_aligned_scans.alignment_transforms[scan_id]
        rel_pose = aligned_transform.inverse() * refined_transform
        rel_cm = np.linalg.norm(rel_pose.translation) * 100
        rel_deg = np.rad2deg(np.arccos(rel_pose.rotation.quat.dot(np.array([0.0, 0.0, 0.0, 1.0]))))
        move_deltas[scan_id] = rel_cm
        rotation_deltas[scan_id] = rel_deg
        logger.debug(f"Scan {scan_id} moved by {rel_cm:.2f} cm, turned by {rel_deg:.2f} degrees")

    logger.info(f"Refined alignment compared to basic alignment:")
    move_vals = list(move_deltas.values())
    rot_vals = list(rotation_deltas.values())

    logger.info(
        f"Position change (cm): "
        f"mean={np.mean(move_vals):.2f}, "
        f"max={np.max(move_vals):.2f}, "
        f"90%={np.percentile(move_vals, 90):.2f}"
    )
    logger.info(
        f"Rotation change (deg): "
        f"mean={np.mean(rot_vals):.2f}, "
        f"max={np.max(rot_vals):.2f}, "
        f"90%={np.percentile(rot_vals, 90):.2f}"
    )

    # Find and warn for any scans which were moved or rotated significantly by refinement.
    # If we see any weird setup accuracy issues checking these logs manually can help us figure out why.
    # Relative AND absolute thresholds must be met, so we catch outliers but avoid false warnings.
    high_move_rel = move_vals > np.mean(move_vals) + 3 * np.std(move_vals)
    high_move_abs = np.array(move_vals) > 100.0
    high_rot_rel = rot_vals > np.mean(rot_vals) + 3 * np.std(rot_vals)
    high_rot_abs = np.array(rot_vals) > 10
    high_indices = np.where(
        (high_move_rel & high_move_abs) | (high_rot_rel & high_rot_abs)
    )[0]

    high_scan_ids = []
    for index in high_indices:
        high_scan_ids.append(aligned_scans.scan_ids[index])
        logger.warning(
            f"WARNING: Scan {aligned_scans.scan_ids[index]} changed significantly by alignment refining, "
            f"which may indicate accuracy issues. Please check if your reconstruction seems off. "
            f"Changed by: {move_vals[index]:.2f} cm, {rot_vals[index]:.2f} deg")

    return move_deltas, rotation_deltas, high_scan_ids