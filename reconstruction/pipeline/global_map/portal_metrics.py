"""Portal spread and overlap metrics, used to accept or downweight portal constraints."""
import pycolmap
import numpy as np
from typing import List, Dict, Tuple, Optional, Set
import itertools
from dataclasses import dataclass
from reconstruction.geometry.pose import mean_pose
from reconstruction.pipeline.global_map.alignment import AlignedScans


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
