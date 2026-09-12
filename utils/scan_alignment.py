import itertools
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
import pycolmap
import pyceres
import numpy as np
from utils.io import read_portal_csv, portalPose
from utils.data_utils import mean_pose, convert_pose_opengl_to_colmap, is_portal_almost_flat, flatten_portal_rotation
from utils.dataset_utils import transform_with_scale
from utils.geometry_utils import QuaternionNormalizationCostFunction
from src.cost_functions import RelativeTransformationSim3CostFunction  # Custom ceres costs implemented in C++
from src.reconstruction_merge import append_reconstruction

@dataclass
class AlignedScans:
    scan_ids: List[str]
    alignment_transforms: Dict[str, pycolmap.Sim3d]
    raw_portal_detections: Dict[str, Dict[str, List[pycolmap.Rigid3d]]]
    aligned_portal_detections: Dict[str, Dict[str, List[pycolmap.Rigid3d]]]


@dataclass(frozen=True)
class PortalSpreadMetrics:
    position_spread_m: float
    rotation_spread_deg: float
    detection_count: int


@dataclass(frozen=True)
class PortalOverlapEdge:
    scan_a: str
    scan_b: str
    portal_id: str
    dp_m: float
    dr_deg: float


@dataclass(frozen=True)
class PortalSpreadRmse:
    position_m: float
    rotation_deg: float


@dataclass(frozen=True)
class PortalOverlapGap:
    position_gap_m: float
    rotation_gap_deg: float


@dataclass(frozen=True)
class PortalAlignmentPortalRecord:
    portal_id: str
    detection_count: int
    before: PortalSpreadMetrics
    after: PortalSpreadMetrics


@dataclass(frozen=True)
class PortalAlignmentOverlapRecord:
    scan_a: str
    scan_b: str
    portal_id: str
    before: PortalOverlapGap
    after: PortalOverlapGap


@dataclass(frozen=True)
class DownweightedPortalEdge:
    scan_a: str
    scan_b: str
    portal_id: str
    position_gap_m: float
    rotation_gap_deg: float


@dataclass(frozen=True)
class PortalAlignmentMetrics:
    applied: bool
    accepted: bool
    cov_scale: int
    rmse_before: PortalSpreadRmse
    rmse_after: PortalSpreadRmse
    rmse_trusted_before: PortalSpreadRmse
    rmse_trusted_after: PortalSpreadRmse
    portals: Tuple[PortalAlignmentPortalRecord, ...]
    overlaps: Tuple[PortalAlignmentOverlapRecord, ...]
    downweighted_edges: Tuple[DownweightedPortalEdge, ...]


# Live knobs (2026-09-12): stronger downweight + stricter outlier gate, plus gravity prior.
PORTAL_DOWNWEIGHT_COV_SCALE = 10_000
PORTAL_OUTLIER_SIGMA_MULT = 3.0
# Soft gravity prior: keep each scan's local-refine up (COLMAP -X) after Sim3 delta.
PORTAL_GRAVITY_PRIOR_WEIGHT = 20.0
# Huber scale a ≈ weight * 2*sin(θ/2); a=1.0 ≈ firm below ~3° tilt, linear above.
PORTAL_GRAVITY_PRIOR_HUBER_SCALE = 1.0
# Local-refine frames are ARKit/gravity aligned → down is COLMAP -X.
_PORTAL_LOCAL_GRAVITY = np.array([-1.0, 0.0, 0.0], dtype=float)


def logical_portal_edge_key(scan_a: str, scan_b: str, portal_id: str) -> Tuple[str, str, str]:
    """Canonical key for a scan-pair portal overlap (scan_a <= scan_b)."""
    sa, sb = sorted([scan_a, scan_b])
    return (sa, sb, portal_id)


def portal_edge_cov_multiplier(
    scan_id_ref: str,
    scan_id_tgt: str,
    portal_id: str,
    downweighted_edges: Optional[Set[Tuple[str, str, str]]],
    cov_scale: int = PORTAL_DOWNWEIGHT_COV_SCALE,
) -> float:
    """Return cov multiplier (1.0 or cov_scale) for a detection-pair residual block."""
    if not downweighted_edges:
        return 1.0
    key = logical_portal_edge_key(scan_id_ref, scan_id_tgt, portal_id)
    return float(cov_scale) if key in downweighted_edges else 1.0


def _edges_by_key(edges: List[PortalOverlapEdge]) -> Dict[Tuple[str, str, str], PortalOverlapEdge]:
    return {
        logical_portal_edge_key(edge.scan_a, edge.scan_b, edge.portal_id): edge
        for edge in edges
    }


def build_portal_alignment_metrics(
    before_scans: AlignedScans,
    after_scans: AlignedScans,
    flagged_edges: List[PortalOverlapEdge],
    applied: bool,
    accepted: bool,
    cov_scale: int = PORTAL_DOWNWEIGHT_COV_SCALE,
) -> PortalAlignmentMetrics:
    """Assemble portal_alignment_metrics payload for manifest wiring (S3)."""
    metrics_before = compute_portal_spread_metrics(before_scans)
    metrics_after = compute_portal_spread_metrics(after_scans)
    rmse_before = compute_portal_spread_rmse(metrics_before)
    rmse_after = compute_portal_spread_rmse(metrics_after)

    downweighted_keys: Set[Tuple[str, str, str]] = {
        logical_portal_edge_key(edge.scan_a, edge.scan_b, edge.portal_id)
        for edge in flagged_edges
    }
    edges_before_list = build_portal_overlap_edges(before_scans)
    edges_after_list = build_portal_overlap_edges(after_scans)
    rmse_trusted_before = compute_overlap_rmse(
        edges_before_list,
        exclude_keys=downweighted_keys if downweighted_keys else None,
    )
    rmse_trusted_after = compute_overlap_rmse(
        edges_after_list,
        exclude_keys=downweighted_keys if downweighted_keys else None,
    )

    all_portal_ids = sorted(set(metrics_before.keys()) | set(metrics_after.keys()))
    portal_records: List[PortalAlignmentPortalRecord] = []
    for portal_id in all_portal_ids:
        before_m = metrics_before.get(portal_id)
        after_m = metrics_after.get(portal_id)
        detection_count = (
            after_m.detection_count if after_m is not None
            else before_m.detection_count if before_m is not None
            else 0
        )
        zero = PortalSpreadMetrics(0.0, 0.0, detection_count)
        portal_records.append(
            PortalAlignmentPortalRecord(
                portal_id=portal_id,
                detection_count=detection_count,
                before=before_m if before_m is not None else zero,
                after=after_m if after_m is not None else zero,
            )
        )

    edges_before = _edges_by_key(build_portal_overlap_edges(before_scans))
    edges_after = _edges_by_key(build_portal_overlap_edges(after_scans))
    all_edge_keys = sorted(set(edges_before.keys()) | set(edges_after.keys()))
    overlap_records: List[PortalAlignmentOverlapRecord] = []
    for key in all_edge_keys:
        scan_a, scan_b, portal_id = key
        before_e = edges_before.get(key)
        after_e = edges_after.get(key)
        zero_gap = PortalOverlapGap(0.0, 0.0)
        overlap_records.append(
            PortalAlignmentOverlapRecord(
                scan_a=scan_a,
                scan_b=scan_b,
                portal_id=portal_id,
                before=PortalOverlapGap(before_e.dp_m, before_e.dr_deg) if before_e else zero_gap,
                after=PortalOverlapGap(after_e.dp_m, after_e.dr_deg) if after_e else zero_gap,
            )
        )

    downweighted = tuple(
        DownweightedPortalEdge(
            scan_a=edge.scan_a,
            scan_b=edge.scan_b,
            portal_id=edge.portal_id,
            position_gap_m=edge.dp_m,
            rotation_gap_deg=edge.dr_deg,
        )
        for edge in flagged_edges
    )

    return PortalAlignmentMetrics(
        applied=applied,
        accepted=accepted,
        cov_scale=cov_scale,
        rmse_before=rmse_before,
        rmse_after=rmse_after,
        rmse_trusted_before=rmse_trusted_before,
        rmse_trusted_after=rmse_trusted_after,
        portals=tuple(portal_records),
        overlaps=tuple(overlap_records),
        downweighted_edges=downweighted,
    )


def portal_alignment_metrics_to_dict(metrics: PortalAlignmentMetrics) -> dict:
    """Serialize PortalAlignmentMetrics for refined_manifest.json (top-level portal_alignment_metrics)."""

    def _spread(spread: PortalSpreadMetrics) -> dict:
        return {
            "position_spread_m": spread.position_spread_m,
            "rotation_spread_deg": spread.rotation_spread_deg,
        }

    def _gap(gap: PortalOverlapGap) -> dict:
        return {
            "position_gap_m": gap.position_gap_m,
            "rotation_gap_deg": gap.rotation_gap_deg,
        }

    def _rmse(rmse: PortalSpreadRmse) -> dict:
        return {
            "position_m": rmse.position_m,
            "rotation_deg": rmse.rotation_deg,
        }

    return {
        "applied": metrics.applied,
        "accepted": metrics.accepted,
        "cov_scale": metrics.cov_scale,
        "rmse_before": _rmse(metrics.rmse_before),
        "rmse_after": _rmse(metrics.rmse_after),
        "rmse_trusted_before": _rmse(metrics.rmse_trusted_before),
        "rmse_trusted_after": _rmse(metrics.rmse_trusted_after),
        "portals": [
            {
                "portal_id": portal.portal_id,
                "detection_count": portal.detection_count,
                "before": _spread(portal.before),
                "after": _spread(portal.after),
            }
            for portal in metrics.portals
        ],
        "overlaps": [
            {
                "scan_a": overlap.scan_a,
                "scan_b": overlap.scan_b,
                "portal_id": overlap.portal_id,
                "before": _gap(overlap.before),
                "after": _gap(overlap.after),
            }
            for overlap in metrics.overlaps
        ],
        "downweighted_edges": [
            {
                "scan_a": edge.scan_a,
                "scan_b": edge.scan_b,
                "portal_id": edge.portal_id,
                "position_gap_m": edge.position_gap_m,
                "rotation_gap_deg": edge.rotation_gap_deg,
            }
            for edge in metrics.downweighted_edges
        ],
    }


def _rotation_angle_deg(quat_a: np.ndarray, quat_b: np.ndarray) -> float:
    dot = abs(float(np.dot(quat_a, quat_b)))
    dot = min(1.0, max(-1.0, dot))
    return float(np.degrees(2.0 * np.arccos(dot)))


def compute_portal_spread_metrics(aligned_scans: AlignedScans) -> Dict[str, PortalSpreadMetrics]:
    """Per-portal RMS position/rotation spread across all aligned detections."""
    detections_by_portal: Dict[str, List[pycolmap.Rigid3d]] = {}

    if not aligned_scans.aligned_portal_detections:
        return {}

    for scan_portals in aligned_scans.aligned_portal_detections.values():
        for portal_id, poses in scan_portals.items():
            detections_by_portal.setdefault(portal_id, []).extend(poses)

    metrics: Dict[str, PortalSpreadMetrics] = {}
    for portal_id, detections in detections_by_portal.items():
        count = len(detections)
        if count == 0:
            continue
        if count == 1:
            metrics[portal_id] = PortalSpreadMetrics(0.0, 0.0, 1)
            continue

        mean = mean_pose(detections)
        positions = np.array([pose.translation for pose in detections])
        position_delta = positions - mean.translation
        position_spread_m = float(np.sqrt(np.mean(np.sum(position_delta * position_delta, axis=1))))

        mean_quat = mean.rotation.quat
        angle_deg = [
            _rotation_angle_deg(pose.rotation.quat, mean_quat)
            for pose in detections
        ]
        rotation_spread_deg = float(np.sqrt(np.mean(np.square(angle_deg))))

        metrics[portal_id] = PortalSpreadMetrics(
            position_spread_m=position_spread_m,
            rotation_spread_deg=rotation_spread_deg,
            detection_count=count,
        )

    return metrics


def build_portal_overlap_edges(aligned_scans: AlignedScans) -> List[PortalOverlapEdge]:
    """Logical scan-pair overlap edges from per-scan mean portal poses (world frame)."""
    portal_scan_means: Dict[str, Dict[str, pycolmap.Rigid3d]] = {}

    for scan_id, scan_portals in aligned_scans.aligned_portal_detections.items():
        for portal_id, poses in scan_portals.items():
            if not poses:
                continue
            portal_scan_means.setdefault(portal_id, {})[scan_id] = mean_pose(poses)

    edges: List[PortalOverlapEdge] = []
    for portal_id, scan_means in portal_scan_means.items():
        for scan_a, scan_b in itertools.combinations(sorted(scan_means.keys()), 2):
            pose_a = scan_means[scan_a]
            pose_b = scan_means[scan_b]
            dp_m = float(np.linalg.norm(pose_a.translation - pose_b.translation))
            dr_deg = _rotation_angle_deg(pose_a.rotation.quat, pose_b.rotation.quat)
            edges.append(
                PortalOverlapEdge(
                    scan_a=scan_a,
                    scan_b=scan_b,
                    portal_id=portal_id,
                    dp_m=dp_m,
                    dr_deg=dr_deg,
                )
            )
    return edges


def flag_outlier_portal_edges(edges: List[PortalOverlapEdge]) -> List[PortalOverlapEdge]:
    """Flag edges where dp or dr exceeds median + k·σ (sample stdev, ddof=1)."""
    if len(edges) < 2:
        return []

    k = PORTAL_OUTLIER_SIGMA_MULT
    dp_values = np.array([edge.dp_m for edge in edges], dtype=float)
    dr_values = np.array([edge.dr_deg for edge in edges], dtype=float)

    thr_p = float(np.median(dp_values) + k * np.std(dp_values, ddof=1))
    thr_r = float(np.median(dr_values) + k * np.std(dr_values, ddof=1))

    return [
        edge
        for edge in edges
        if edge.dp_m > thr_p or edge.dr_deg > thr_r
    ]


def compute_portal_spread_rmse(
    metrics: Dict[str, PortalSpreadMetrics],
) -> PortalSpreadRmse:
    """Job-level portal-spread RMSE over portals with detection_count >= 2."""
    eligible = [m for m in metrics.values() if m.detection_count >= 2]
    if not eligible:
        return PortalSpreadRmse(position_m=0.0, rotation_deg=0.0)

    pos_sq = [m.position_spread_m ** 2 for m in eligible]
    rot_sq = [m.rotation_spread_deg ** 2 for m in eligible]
    return PortalSpreadRmse(
        position_m=float(np.sqrt(np.mean(pos_sq))),
        rotation_deg=float(np.sqrt(np.mean(rot_sq))),
    )


def compute_overlap_rmse(
    edges: List[PortalOverlapEdge],
    exclude_keys: Optional[Set[Tuple[str, str, str]]] = None,
) -> PortalSpreadRmse:
    """Job-level overlap RMSE over logical edges, optionally excluding downweighted keys."""
    trusted = [
        edge
        for edge in edges
        if exclude_keys is None
        or logical_portal_edge_key(edge.scan_a, edge.scan_b, edge.portal_id) not in exclude_keys
    ]
    if not trusted:
        return PortalSpreadRmse(position_m=0.0, rotation_deg=0.0)

    pos_sq = [edge.dp_m ** 2 for edge in trusted]
    rot_sq = [edge.dr_deg ** 2 for edge in trusted]
    return PortalSpreadRmse(
        position_m=float(np.sqrt(np.mean(pos_sq))),
        rotation_deg=float(np.sqrt(np.mean(rot_sq))),
    )


_PORTAL_ACCEPT_POS_ABS_MAX_M = 0.20
_PORTAL_ACCEPT_ROT_ABS_MAX_DEG = 5.0
_PORTAL_ACCEPT_WORSEN_RATIO = 2.0


def _portal_axis_reject(before: float, after: float, abs_max: float) -> bool:
    """True if after is catastrophically worse than before on one RMSE axis."""
    if after <= before:
        return False
    if before > 0.0 and after >= _PORTAL_ACCEPT_WORSEN_RATIO * before:
        return True
    if (after - before) >= abs_max:
        return True
    return False


def accept_portal_spread_rmse(before: PortalSpreadRmse, after: PortalSpreadRmse) -> bool:
    """Accept after solve unless trusted RMSE worsens catastrophically on either axis."""
    if _portal_axis_reject(
        before.position_m, after.position_m, _PORTAL_ACCEPT_POS_ABS_MAX_M
    ):
        return False
    if _portal_axis_reject(
        before.rotation_deg, after.rotation_deg, _PORTAL_ACCEPT_ROT_ABS_MAX_DEG
    ):
        return False
    return True


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


def _run_ceres_alignment_solve(
    aligned_scans: AlignedScans,
    logger: logging.Logger,
    downweighted_edges: Optional[Set[Tuple[str, str, str]]] = None,
) -> AlignedScans:
    """Build Ceres problem from aligned scans, solve, return refined AlignedScans."""
    scan_ids = aligned_scans.scan_ids
    t_local_scan_quat = [pycolmap.Rigid3d().rotation.quat for _ in range(len(scan_ids))]
    t_local_scan_translation = [pycolmap.Rigid3d().translation for _ in range(len(scan_ids))]
    problem = pyceres.Problem()

    loss = None

    qr_ids_per_scan = [set() for _ in range(len(scan_ids))]
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

            cov_multiplier = portal_edge_cov_multiplier(
                scan_ids[scan_idx_ref],
                scan_ids[scan_idx_tgt],
                qr_id,
                downweighted_edges,
            )
            cov = np.eye(6)
            cov[3:, 3:] *= 0.01
            cov *= cov_multiplier

            cost_1 = RelativeTransformationSim3CostFunction(
                t_refworld_qr.rotation.quat,
                t_refworld_qr.translation,
                t_tgtworld_qr.rotation.quat,
                t_tgtworld_qr.translation,
                cov,
            )
            params_1 = [
                t_local_scan_quat[scan_idx_tgt],
                t_local_scan_translation[scan_idx_tgt],
                t_local_scan_quat[scan_idx_ref],
                t_local_scan_translation[scan_idx_ref],
            ]
            problem.add_residual_block(cost_1, loss, params_1)

            cost_2 = RelativeTransformationSim3CostFunction(
                t_tgtworld_qr.rotation.quat,
                t_tgtworld_qr.translation,
                t_refworld_qr.rotation.quat,
                t_refworld_qr.translation,
                cov,
            )
            params_2 = [
                t_local_scan_quat[scan_idx_ref],
                t_local_scan_translation[scan_idx_ref],
                t_local_scan_quat[scan_idx_tgt],
                t_local_scan_translation[scan_idx_tgt],
            ]
            problem.add_residual_block(cost_2, loss, params_2)

            qr_ids_per_scan[scan_idx_ref].add(qr_id)
            qr_ids_per_scan[scan_idx_tgt].add(qr_id)

    # Soft gravity prior: trust local-refinement up (COLMAP -X) per scan.
    # residual = w * (R_delta * R_stitch * local_g - world_g), Huber-robustified.
    from src.cost_functions import GravityDirectionPriorSim3DeltaCostFunction

    gravity_loss = pyceres.HuberLoss(PORTAL_GRAVITY_PRIOR_HUBER_SCALE)
    gravity_costs = []
    for scan_idx, scan_id in enumerate(scan_ids):
        stitch = aligned_scans.alignment_transforms[scan_id]
        cost = GravityDirectionPriorSim3DeltaCostFunction(
            np.asarray(stitch.rotation.quat, dtype=float),
            _PORTAL_LOCAL_GRAVITY,
            PORTAL_GRAVITY_PRIOR_WEIGHT,
        )
        quat = t_local_scan_quat[scan_idx]
        gravity_costs.append((scan_id, cost, quat))
        problem.add_residual_block(cost, gravity_loss, [quat])
        if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
            problem.set_manifold(quat, pyceres.QuaternionManifold())

    def _gravity_cost_stats(label: str) -> Tuple[float, float]:
        """Return (sum raw ||r||^2, sum Huber rho) and log per-pass contribution."""
        raw_sq = []
        huber_vals = []
        tilts_deg = []
        world_down = _PORTAL_LOCAL_GRAVITY
        for scan_id, cost, quat in gravity_costs:
            residuals, _ = cost.evaluate(quat)
            r = np.asarray(residuals, dtype=float).reshape(-1)
            s = float(np.dot(r, r))
            raw_sq.append(s)
            # Ceres LossFunction.evaluate(s) -> [rho, ...]
            huber_vals.append(float(gravity_loss.evaluate(s)[0]))
            # Effective world-down tilt after delta*stitch (independent of weight).
            delta = pycolmap.Rotation3d(quat)
            stitch_R = aligned_scans.alignment_transforms[scan_id].rotation
            mapped = (delta * stitch_R).matrix() @ world_down
            c = float(np.clip(np.dot(mapped, world_down), -1.0, 1.0))
            tilts_deg.append(float(np.degrees(np.arccos(c))))
        sum_raw = float(np.sum(raw_sq))
        sum_huber = float(np.sum(huber_vals))
        logger.info(
            f"Gravity prior [{label}]: n={len(gravity_costs)} "
            f"raw_cost={sum_raw:.3f} huber_cost={sum_huber:.3f} "
            f"(w={PORTAL_GRAVITY_PRIOR_WEIGHT}, Huber a={PORTAL_GRAVITY_PRIOR_HUBER_SCALE}); "
            f"tilt_deg mean={np.mean(tilts_deg):.2f} max={np.max(tilts_deg):.2f} "
            f"p90={np.percentile(tilts_deg, 90):.2f}"
        )
        return sum_raw, sum_huber

    grav_raw0, grav_huber0 = _gravity_cost_stats("pre-solve")

    for scan_idx in range(len(scan_ids)):
        if len(qr_ids_per_scan[scan_idx]) < 2:
            quat = t_local_scan_quat[scan_idx]
            logger.info(f"Scan {scan_idx} has less than 2 correspondences, fixing scale")
            if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
                problem.set_manifold(quat, pyceres.QuaternionManifold())
        else:
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

    grav_raw1, grav_huber1 = _gravity_cost_stats("post-solve")
    # Ceres reports 0.5 * sum rho; compare gravity huber sum on same footing.
    initial = float(summary.initial_cost)
    final = float(summary.final_cost)
    # Our huber_cost is sum rho; Ceres cost is typically 1/2 sum rho.
    grav_share0 = (0.5 * grav_huber0 / initial) if initial > 0 else 0.0
    grav_share1 = (0.5 * grav_huber1 / final) if final > 0 else 0.0
    logger.info(
        f"Gravity prior vs total Ceres cost: "
        f"pre {0.5*grav_huber0:.3f}/{initial:.3f} ({100*grav_share0:.1f}%), "
        f"post {0.5*grav_huber1:.3f}/{final:.3f} ({100*grav_share1:.1f}%)"
    )

    t_local_scans = [
        pycolmap.Sim3d(
            pycolmap.Rotation3d(quat).norm() ** 2,
            pycolmap.Rotation3d(quat),
            translation,
        )
        for quat, translation in zip(t_local_scan_quat, t_local_scan_translation)
    ]
    for t_local_scan in t_local_scans:
        t_local_scan.rotation.normalize()

    logger.debug("Refined Sim3 transforms:")
    for scan_idx, t_local_scan in enumerate(t_local_scans):
        logger.debug(f"Scan {scan_idx} ({scan_ids[scan_idx]}): {t_local_scan}")

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

    return AlignedScans(
        scan_ids=scan_ids,
        alignment_transforms=refined_alignment_transforms,
        raw_portal_detections=aligned_scans.raw_portal_detections,
        aligned_portal_detections=refined_aligned_portal_detections,
    )


def refine_alignment(
    aligned_scans: AlignedScans,
    job_root_path: Path,
    logger: Optional[logging.Logger] = None,
    enable_portal_reweight: bool = True,
) -> Tuple[AlignedScans, PortalAlignmentMetrics]:
    if logger is None:
        logger = logging.getLogger("scan_alignment")
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.info("Going to optimize chunk alignment...")

    first_result = _run_ceres_alignment_solve(aligned_scans, logger)

    if not enable_portal_reweight:
        logger.info("Portal reweight pass disabled; using first solve only.")
        metrics = build_portal_alignment_metrics(
            first_result,
            first_result,
            flagged_edges=[],
            applied=False,
            accepted=False,
        )
        logger.info("Scan alignment refined successfully!")
        return first_result, metrics

    overlap_edges = build_portal_overlap_edges(first_result)
    flagged_edges = flag_outlier_portal_edges(overlap_edges)

    if not flagged_edges:
        logger.info("No outlier portal overlap edges flagged; skipping reweight pass.")
        metrics = build_portal_alignment_metrics(
            first_result,
            first_result,
            flagged_edges=[],
            applied=False,
            accepted=False,
        )
        logger.info("Scan alignment refined successfully!")
        return first_result, metrics

    downweighted_keys: Set[Tuple[str, str, str]] = {
        logical_portal_edge_key(edge.scan_a, edge.scan_b, edge.portal_id)
        for edge in flagged_edges
    }
    logger.info(
        "Outlier portal overlap edges flagged (%d); running reweight pass (cov×%d).",
        len(flagged_edges),
        PORTAL_DOWNWEIGHT_COV_SCALE,
    )
    for edge in flagged_edges:
        logger.info(
            "  downweight %s↔%s portal %s (Δp=%.4f m, Δr=%.2f deg)",
            edge.scan_a,
            edge.scan_b,
            edge.portal_id,
            edge.dp_m,
            edge.dr_deg,
        )

    second_result = _run_ceres_alignment_solve(
        aligned_scans,
        logger,
        downweighted_edges=downweighted_keys,
    )

    spread_rmse_before = compute_portal_spread_rmse(compute_portal_spread_metrics(first_result))
    spread_rmse_after = compute_portal_spread_rmse(compute_portal_spread_metrics(second_result))
    edges_before = build_portal_overlap_edges(first_result)
    edges_after = build_portal_overlap_edges(second_result)
    trusted_before = compute_overlap_rmse(edges_before, exclude_keys=downweighted_keys)
    trusted_after = compute_overlap_rmse(edges_after, exclude_keys=downweighted_keys)
    accepted = accept_portal_spread_rmse(trusted_before, trusted_after)

    if accepted:
        logger.info(
            "Reweight pass accepted (trusted RMSE pos %.6f→%.6f m, rot %.6f→%.6f deg; "
            "portal-spread pos %.6f→%.6f m, rot %.6f→%.6f deg).",
            trusted_before.position_m,
            trusted_after.position_m,
            trusted_before.rotation_deg,
            trusted_after.rotation_deg,
            spread_rmse_before.position_m,
            spread_rmse_after.position_m,
            spread_rmse_before.rotation_deg,
            spread_rmse_after.rotation_deg,
        )
        final_result = second_result
    else:
        logger.info(
            "Reweight pass reverted (trusted RMSE pos %.6f→%.6f m, rot %.6f→%.6f deg; "
            "portal-spread pos %.6f→%.6f m, rot %.6f→%.6f deg).",
            trusted_before.position_m,
            trusted_after.position_m,
            trusted_before.rotation_deg,
            trusted_after.rotation_deg,
            spread_rmse_before.position_m,
            spread_rmse_after.position_m,
            spread_rmse_before.rotation_deg,
            spread_rmse_after.rotation_deg,
        )
        final_result = first_result

    metrics = build_portal_alignment_metrics(
        first_result,
        second_result,
        flagged_edges=flagged_edges,
        applied=True,
        accepted=accepted,
    )
    logger.info("Scan alignment refined successfully!")
    return final_result, metrics


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