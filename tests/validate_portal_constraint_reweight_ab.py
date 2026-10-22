"""
Synthetic A/B validation for portal constraint reweight (plan §5 item 3).

Loads the multi-scan job fixture, runs refine with/without reweight on clean and
corrupted portal overlap data, and prints RMSE comparison.

The 4-scan fixture has only six logical overlap edges; after Ceres L2 the first
solve scales edge gaps uniformly so median+2σ rarely fires on raw detection
corruption alone. The corrupt path therefore adds two phantom scans (copies of a
stable reference scan) to supply low-gap edges, then skews one scan's aligned
portal constraint while leaving raw detections unchanged — mimicking a bad overlap
constraint that survives post-solve edge scoring.

Run (host without pycolmap):
  docker run --rm -v "$PWD:/app" -w /app -e PYTHONPATH=/app \\
    --entrypoint python3 reconstruction-node:local \\
    tests/validate_portal_constraint_reweight_ab.py

Or as unittest (skips if fixture missing):
  PYTHONPATH=. python -m pytest tests/validate_portal_constraint_reweight_ab.py -v
"""

from __future__ import annotations

import logging
import sys
import unittest
from pathlib import Path
from typing import Tuple

import numpy as np
import pycolmap

from reconstruction.geometry.pose import transform_with_scale
from reconstruction.pipeline.global_.alignment import AlignedScans, align_scans
from reconstruction.pipeline.global_.portal_metrics import PortalAlignmentMetrics, PortalSpreadRmse, compute_portal_spread_metrics, compute_portal_spread_rmse
from reconstruction.pipeline.global_.refine import refine_alignment

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = (
    REPO_ROOT
    / "_local"
    / "job-artifacts"
    / "20260904-214520-job_50ca9b6a-dcc2-4bf6-a278-439b1ffea8ff-complete"
)

PHANTOM_REFERENCE_SCAN_ID = "2026-04-02_13-42-53"
PHANTOM_COUNT = 2

CORRUPT_SCAN_ID = "2026-04-02_13-41-55"
CORRUPT_PORTAL_ID = "HBORRXLUOD6"
CORRUPT_ALIGNED_TRANSLATION_M = np.array([2.0, 0.0, 0.0])


def _copy_pose(pose: pycolmap.Rigid3d) -> pycolmap.Rigid3d:
    return pycolmap.Rigid3d(
        pycolmap.Rotation3d(pose.rotation.quat.copy()),
        pose.translation.copy(),
    )


def _recompute_aligned_portal_detections(
    aligned: AlignedScans,
) -> dict[str, dict[str, list[pycolmap.Rigid3d]]]:
    return {
        scan_id: {
            portal_id: [
                transform_with_scale(aligned.alignment_transforms[scan_id], pose)
                for pose in poses
            ]
            for portal_id, poses in portals.items()
        }
        for scan_id, portals in aligned.raw_portal_detections.items()
    }


def copy_aligned_scans(aligned: AlignedScans) -> AlignedScans:
    raw_portal_detections = {
        scan_id: {
            portal_id: [_copy_pose(pose) for pose in poses]
            for portal_id, poses in portals.items()
        }
        for scan_id, portals in aligned.raw_portal_detections.items()
    }
    copied = AlignedScans(
        scan_ids=list(aligned.scan_ids),
        alignment_transforms=dict(aligned.alignment_transforms),
        raw_portal_detections=raw_portal_detections,
        aligned_portal_detections={},
    )
    copied.aligned_portal_detections = _recompute_aligned_portal_detections(copied)
    return copied


def add_phantom_scans(
    aligned: AlignedScans,
    reference_scan_id: str = PHANTOM_REFERENCE_SCAN_ID,
    phantom_count: int = PHANTOM_COUNT,
) -> AlignedScans:
    """Duplicate a stable scan so median+2σ has enough low-gap edges to flag one outlier."""
    augmented = copy_aligned_scans(aligned)
    reference_raw = augmented.raw_portal_detections[reference_scan_id]
    reference_transform = augmented.alignment_transforms[reference_scan_id]

    for index in range(phantom_count):
        phantom_id = f"phantom_{index:02d}"
        augmented.scan_ids.append(phantom_id)
        augmented.alignment_transforms[phantom_id] = reference_transform
        augmented.raw_portal_detections[phantom_id] = {
            portal_id: [_copy_pose(pose) for pose in poses]
            for portal_id, poses in reference_raw.items()
        }

    augmented.aligned_portal_detections = _recompute_aligned_portal_detections(augmented)
    return augmented


def corrupt_aligned_portal_constraint(
    aligned: AlignedScans,
    scan_id: str,
    portal_id: str,
    translation_offset: np.ndarray,
) -> AlignedScans:
    """Skew one aligned portal pose used by Ceres while keeping raw detections intact."""
    corrupted = copy_aligned_scans(aligned)
    pose = corrupted.aligned_portal_detections[scan_id][portal_id][0]
    corrupted.aligned_portal_detections[scan_id][portal_id][0] = pycolmap.Rigid3d(
        pycolmap.Rotation3d(pose.rotation.quat.copy()),
        pose.translation + translation_offset,
    )
    return corrupted


def rmse_for_result(result: AlignedScans) -> PortalSpreadRmse:
    return compute_portal_spread_rmse(compute_portal_spread_metrics(result))


def run_refine(
    aligned: AlignedScans,
    job_root: Path,
    enable_portal_reweight: bool,
    logger: logging.Logger,
) -> Tuple[AlignedScans, PortalAlignmentMetrics, PortalSpreadRmse]:
    refined, metrics = refine_alignment(
        aligned,
        job_root,
        logger=logger,
        enable_portal_reweight=enable_portal_reweight,
    )
    return refined, metrics, rmse_for_result(refined)


def build_corrupted_aligned(aligned: AlignedScans) -> AlignedScans:
    with_phantoms = add_phantom_scans(aligned)
    return corrupt_aligned_portal_constraint(
        with_phantoms,
        CORRUPT_SCAN_ID,
        CORRUPT_PORTAL_ID,
        CORRUPT_ALIGNED_TRANSLATION_M,
    )


def run_ab(fixture_path: Path) -> dict:
    logger = logging.getLogger("portal_reweight_ab")
    logger.setLevel(logging.WARNING)

    scan_ids = sorted(
        p.parent.parent.name
        for p in (fixture_path / "refined" / "local").glob("*/sfm/portals.csv")
    )
    aligned = align_scans(scan_ids, fixture_path, logger=logger)

    _, clean_metrics, rmse_clean_on = run_refine(
        aligned, fixture_path, True, logger
    )
    _, _, rmse_clean_off = run_refine(aligned, fixture_path, False, logger)

    corrupted = build_corrupted_aligned(aligned)
    _, _, rmse_corrupt_off = run_refine(
        corrupted, fixture_path, False, logger
    )
    _, corrupt_on_metrics, rmse_corrupt_on = run_refine(
        corrupted, fixture_path, True, logger
    )

    return {
        "fixture": str(fixture_path),
        "scan_count": len(scan_ids),
        "phantom_count": PHANTOM_COUNT,
        "corrupt": {
            "scan_id": CORRUPT_SCAN_ID,
            "portal_id": CORRUPT_PORTAL_ID,
            "mode": "aligned_constraint_skew_m",
            "translation_m": CORRUPT_ALIGNED_TRANSLATION_M.tolist(),
        },
        "rmse_clean_on": _rmse_dict(rmse_clean_on),
        "rmse_clean_off": _rmse_dict(rmse_clean_off),
        "rmse_corrupt_off": _rmse_dict(rmse_corrupt_off),
        "rmse_corrupt_on": _rmse_dict(rmse_corrupt_on),
        "clean_reweight_applied": clean_metrics.applied,
        "corrupt_reweight_applied": corrupt_on_metrics.applied,
        "corrupt_reweight_accepted": corrupt_on_metrics.accepted,
        "downweighted_edges": [
            {
                "scan_a": e.scan_a,
                "scan_b": e.scan_b,
                "portal_id": e.portal_id,
                "position_gap_m": e.position_gap_m,
                "rotation_gap_deg": e.rotation_gap_deg,
            }
            for e in corrupt_on_metrics.downweighted_edges
        ],
        "checks": {
            "healthy_no_reweight": _rmse_close(rmse_clean_on, rmse_clean_off),
            "corrupt_off_worse_than_clean": _rmse_worse(rmse_corrupt_off, rmse_clean_on),
            "corrupt_on_better_than_off": _rmse_better_or_equal(rmse_corrupt_on, rmse_corrupt_off)
            and _rmse_strictly_better_on_at_least_one_axis(rmse_corrupt_on, rmse_corrupt_off),
            "corrupt_reweight_ran": corrupt_on_metrics.applied,
            "corrupt_edges_flagged": len(corrupt_on_metrics.downweighted_edges) > 0,
            "corrupt_reweight_accepted": corrupt_on_metrics.accepted,
        },
    }


def _rmse_dict(rmse: PortalSpreadRmse) -> dict:
    return {"position_m": rmse.position_m, "rotation_deg": rmse.rotation_deg}


def _rmse_close(a: PortalSpreadRmse, b: PortalSpreadRmse, tol: float = 1e-4) -> bool:
    return (
        abs(a.position_m - b.position_m) <= tol
        and abs(a.rotation_deg - b.rotation_deg) <= tol
    )


def _rmse_worse(a: PortalSpreadRmse, b: PortalSpreadRmse) -> bool:
    return a.position_m > b.position_m or a.rotation_deg > b.rotation_deg


def _rmse_better_or_equal(a: PortalSpreadRmse, b: PortalSpreadRmse) -> bool:
    return a.position_m <= b.position_m and a.rotation_deg <= b.rotation_deg


def _rmse_strictly_better_on_at_least_one_axis(
    a: PortalSpreadRmse,
    b: PortalSpreadRmse,
) -> bool:
    return a.position_m < b.position_m or a.rotation_deg < b.rotation_deg


def _format_results(results: dict) -> str:
    lines = [
        f"Fixture: {results['fixture']}",
        f"Scans: {results['scan_count']} (+{results['phantom_count']} phantom scans on corrupt path)",
        (
            f"Corruption: scan={results['corrupt']['scan_id']} "
            f"portal={results['corrupt']['portal_id']} "
            f"mode={results['corrupt']['mode']} "
            f"offset={results['corrupt']['translation_m']}"
        ),
        "",
        "Portal-spread RMSE (position_m, rotation_deg):",
        f"  clean +reweight:   {results['rmse_clean_on']}",
        f"  clean -reweight:   {results['rmse_clean_off']}",
        f"  corrupt -reweight: {results['rmse_corrupt_off']}",
        f"  corrupt +reweight: {results['rmse_corrupt_on']}",
        "",
        f"Clean reweight applied: {results['clean_reweight_applied']}",
        (
            f"Corrupt reweight applied={results['corrupt_reweight_applied']} "
            f"accepted={results['corrupt_reweight_accepted']}"
        ),
        f"Downweighted edges ({len(results['downweighted_edges'])}):",
    ]
    for edge in results["downweighted_edges"]:
        lines.append(
            f"  {edge['scan_a']}↔{edge['scan_b']} {edge['portal_id']} "
            f"Δp={edge['position_gap_m']:.4f}m Δr={edge['rotation_gap_deg']:.2f}°"
        )
    lines.append("")
    lines.append("Checks:")
    for name, passed in results["checks"].items():
        lines.append(f"  {'PASS' if passed else 'FAIL'} {name}")
    return "\n".join(lines)


class PortalConstraintReweightAbTests(unittest.TestCase):
    @unittest.skipUnless(DEFAULT_FIXTURE.exists(), "job fixture not available")
    def test_synthetic_ab_on_fixture(self):
        results = run_ab(DEFAULT_FIXTURE)
        for name, passed in results["checks"].items():
            self.assertTrue(passed, f"check failed: {name}\n{_format_results(results)}")


def main() -> int:
    fixture = DEFAULT_FIXTURE
    if len(sys.argv) > 1:
        fixture = Path(sys.argv[1])

    if not fixture.exists():
        print(f"SKIP: fixture not found: {fixture}", file=sys.stderr)
        return 0

    results = run_ab(fixture)
    print(_format_results(results))
    failed = [name for name, passed in results["checks"].items() if not passed]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
