import json
import math
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pycolmap

from reconstruction.output.manifest import save_manifest_json
from reconstruction.pipeline.global_map.alignment import AlignedScans
from reconstruction.pipeline.global_map.portal_metrics import PORTAL_DOWNWEIGHT_COV_SCALE, PortalOverlapEdge, PortalSpreadMetrics, PortalSpreadRmse, accept_portal_spread_rmse, build_portal_alignment_metrics, build_portal_overlap_edges, portal_alignment_metrics_to_dict, compute_overlap_rmse, compute_portal_spread_rmse, flag_outlier_portal_edges, logical_portal_edge_key, portal_edge_cov_multiplier


def _pose(x: float, y: float, z: float, quat=None) -> pycolmap.Rigid3d:
    if quat is None:
        quat = np.array([-0.7071068, 0.0, 0.0, 0.7071068])
    return pycolmap.Rigid3d(pycolmap.Rotation3d(quat), np.array([x, y, z]))


def _multi_scan_aligned(portal_layout: dict) -> AlignedScans:
    """portal_layout: {scan_id: {portal_id: [poses]}}"""
    return AlignedScans(
        scan_ids=sorted(portal_layout.keys()),
        alignment_transforms={},
        raw_portal_detections={},
        aligned_portal_detections=portal_layout,
    )


class BuildPortalOverlapEdgesTests(unittest.TestCase):
    def test_hand_computed_position_and_rotation_gaps(self):
        base = np.array([-0.7071068, 0.0, 0.0, 0.7071068])
        tilt = np.array([-0.700, 0.08, 0.0, 0.7071068])
        aligned = _multi_scan_aligned(
            {
                "scan_a": {"P1": [_pose(0.0, 0.0, 0.0, base)]},
                "scan_b": {"P1": [_pose(3.0, 4.0, 0.0, tilt)]},
            }
        )
        edges = build_portal_overlap_edges(aligned)
        self.assertEqual(len(edges), 1)
        edge = edges[0]
        self.assertEqual(edge.scan_a, "scan_a")
        self.assertEqual(edge.scan_b, "scan_b")
        self.assertEqual(edge.portal_id, "P1")
        self.assertAlmostEqual(edge.dp_m, 5.0, places=6)

        dot = abs(float(np.dot(base, tilt)))
        expected_dr = float(np.degrees(2.0 * np.arccos(min(1.0, dot))))
        self.assertAlmostEqual(edge.dr_deg, expected_dr, places=6)

    def test_mean_pose_within_scan_before_pairing(self):
        aligned = _multi_scan_aligned(
            {
                "scan_a": {
                    "P1": [
                        _pose(0.0, 0.0, 0.0),
                        _pose(0.2, 0.0, 0.0),
                    ]
                },
                "scan_b": {"P1": [_pose(1.0, 0.0, 0.0)]},
            }
        )
        edges = build_portal_overlap_edges(aligned)
        self.assertEqual(len(edges), 1)
        self.assertAlmostEqual(edges[0].dp_m, 0.9, places=6)

    def test_multiple_portals_and_pairs(self):
        aligned = _multi_scan_aligned(
            {
                "scan_a": {"P1": [_pose(0.0, 0.0, 0.0)], "P2": [_pose(10.0, 0.0, 0.0)]},
                "scan_b": {"P1": [_pose(0.0, 0.0, 0.0)], "P2": [_pose(10.0, 0.0, 0.0)]},
                "scan_c": {"P1": [_pose(0.0, 0.0, 0.0)]},
            }
        )
        edges = build_portal_overlap_edges(aligned)
        portal_ids = {e.portal_id for e in edges}
        self.assertEqual(portal_ids, {"P1", "P2"})
        p1_edges = [e for e in edges if e.portal_id == "P1"]
        self.assertEqual(len(p1_edges), 3)


class FlagOutlierPortalEdgesTests(unittest.TestCase):
    def test_flags_planted_outlier_in_healthy_cluster(self):
        # Enough healthy edges so a single planted gap still exceeds median+3σ.
        edges = [
            PortalOverlapEdge("a", "b", "P1", 0.05, 1.0),
            PortalOverlapEdge("a", "c", "P1", 0.06, 1.1),
            PortalOverlapEdge("b", "c", "P1", 0.04, 0.9),
            PortalOverlapEdge("a", "d", "P1", 0.05, 1.0),
            PortalOverlapEdge("c", "d", "P1", 0.05, 1.0),
            PortalOverlapEdge("a", "e", "P1", 0.055, 1.05),
            PortalOverlapEdge("b", "e", "P1", 0.045, 0.95),
            PortalOverlapEdge("c", "e", "P1", 0.05, 1.0),
            PortalOverlapEdge("d", "e", "P1", 0.05, 1.0),
            PortalOverlapEdge("b", "d", "P1", 1.00, 1.0),
        ]
        flagged = flag_outlier_portal_edges(edges)
        self.assertEqual(len(flagged), 1)
        self.assertEqual(flagged[0].scan_a, "b")
        self.assertEqual(flagged[0].scan_b, "d")

    def test_fewer_than_two_edges_flags_none(self):
        edges = [PortalOverlapEdge("a", "b", "P1", 10.0, 90.0)]
        self.assertEqual(flag_outlier_portal_edges(edges), [])

    def test_n_equals_three_may_miss_outlier(self):
        """Small-n graphs can inflate σ so the bad edge stays below median+kσ."""
        edges = [
            PortalOverlapEdge("a", "b", "P1", 0.10, 1.0),
            PortalOverlapEdge("a", "c", "P1", 0.10, 1.0),
            PortalOverlapEdge("b", "c", "P1", 0.50, 1.0),
        ]
        flagged = flag_outlier_portal_edges(edges)
        self.assertEqual(flagged, [])

    def test_n_equals_two_never_flags(self):
        edges = [
            PortalOverlapEdge("a", "b", "P1", 0.10, 1.0),
            PortalOverlapEdge("a", "c", "P1", 5.00, 45.0),
        ]
        self.assertEqual(flag_outlier_portal_edges(edges), [])


class PortalSpreadRmseTests(unittest.TestCase):
    def test_rmse_over_portals_with_at_least_two_detections(self):
        metrics = {
            "P1": PortalSpreadMetrics(position_spread_m=0.3, rotation_spread_deg=2.0, detection_count=3),
            "P2": PortalSpreadMetrics(position_spread_m=0.4, rotation_spread_deg=4.0, detection_count=2),
            "P3": PortalSpreadMetrics(position_spread_m=9.9, rotation_spread_deg=9.9, detection_count=1),
        }
        rmse = compute_portal_spread_rmse(metrics)
        expected_pos = math.sqrt((0.3 ** 2 + 0.4 ** 2) / 2)
        expected_rot = math.sqrt((2.0 ** 2 + 4.0 ** 2) / 2)
        self.assertAlmostEqual(rmse.position_m, expected_pos, places=6)
        self.assertAlmostEqual(rmse.rotation_deg, expected_rot, places=6)

    def test_empty_eligible_portals_returns_zero_rmse(self):
        metrics = {
            "P1": PortalSpreadMetrics(0.0, 0.0, 1),
        }
        rmse = compute_portal_spread_rmse(metrics)
        self.assertEqual(rmse.position_m, 0.0)
        self.assertEqual(rmse.rotation_deg, 0.0)


class PortalEdgeCovMultiplierTests(unittest.TestCase):
    def test_unflagged_edge_returns_unit_multiplier(self):
        self.assertEqual(
            portal_edge_cov_multiplier("scan_a", "scan_b", "P1", None),
            1.0,
        )
        self.assertEqual(
            portal_edge_cov_multiplier(
                "scan_b",
                "scan_a",
                "P1",
                {logical_portal_edge_key("scan_c", "scan_d", "P2")},
            ),
            1.0,
        )

    def test_flagged_edge_returns_cov_scale_both_directions(self):
        flagged = {logical_portal_edge_key("scan_a", "scan_b", "P1")}
        self.assertEqual(
            portal_edge_cov_multiplier("scan_a", "scan_b", "P1", flagged),
            float(PORTAL_DOWNWEIGHT_COV_SCALE),
        )
        self.assertEqual(
            portal_edge_cov_multiplier("scan_b", "scan_a", "P1", flagged),
            float(PORTAL_DOWNWEIGHT_COV_SCALE),
        )

    def test_logical_key_is_order_invariant(self):
        self.assertEqual(
            logical_portal_edge_key("scan_b", "scan_a", "P1"),
            ("scan_a", "scan_b", "P1"),
        )


class BuildPortalAlignmentMetricsTests(unittest.TestCase):
    def test_not_applied_metadata(self):
        aligned = _multi_scan_aligned(
            {
                "scan_a": {"P1": [_pose(0.0, 0.0, 0.0)]},
                "scan_b": {"P1": [_pose(0.0, 0.0, 0.0)]},
            }
        )
        metrics = build_portal_alignment_metrics(
            aligned,
            aligned,
            flagged_edges=[],
            applied=False,
            accepted=False,
        )
        self.assertFalse(metrics.applied)
        self.assertFalse(metrics.accepted)
        self.assertEqual(metrics.cov_scale, PORTAL_DOWNWEIGHT_COV_SCALE)
        self.assertEqual(metrics.downweighted_edges, ())
        self.assertEqual(metrics.rmse_before.position_m, metrics.rmse_after.position_m)

    def test_applied_includes_downweighted_edges_and_before_after(self):
        before = _multi_scan_aligned(
            {
                "scan_a": {"P1": [_pose(0.0, 0.0, 0.0)]},
                "scan_b": {"P1": [_pose(0.5, 0.0, 0.0)]},
            }
        )
        after = _multi_scan_aligned(
            {
                "scan_a": {"P1": [_pose(0.0, 0.0, 0.0)]},
                "scan_b": {"P1": [_pose(0.1, 0.0, 0.0)]},
            }
        )
        flagged = [PortalOverlapEdge("scan_a", "scan_b", "P1", 0.5, 1.0)]
        metrics = build_portal_alignment_metrics(
            before,
            after,
            flagged_edges=flagged,
            applied=True,
            accepted=True,
        )
        self.assertTrue(metrics.applied)
        self.assertTrue(metrics.accepted)
        self.assertEqual(len(metrics.downweighted_edges), 1)
        self.assertAlmostEqual(metrics.downweighted_edges[0].position_gap_m, 0.5)
        overlap = next(o for o in metrics.overlaps if o.portal_id == "P1")
        self.assertAlmostEqual(overlap.before.position_gap_m, 0.5)
        self.assertAlmostEqual(overlap.after.position_gap_m, 0.1)


class PortalAlignmentMetricsManifestTests(unittest.TestCase):
    def test_to_dict_matches_manifest_contract(self):
        before = _multi_scan_aligned(
            {
                "scan_a": {"P1": [_pose(0.0, 0.0, 0.0)]},
                "scan_b": {"P1": [_pose(0.5, 0.0, 0.0)]},
            }
        )
        after = _multi_scan_aligned(
            {
                "scan_a": {"P1": [_pose(0.0, 0.0, 0.0)]},
                "scan_b": {"P1": [_pose(0.1, 0.0, 0.0)]},
            }
        )
        flagged = [PortalOverlapEdge("scan_a", "scan_b", "P1", 0.5, 1.0)]
        metrics = build_portal_alignment_metrics(
            before,
            after,
            flagged_edges=flagged,
            applied=True,
            accepted=True,
        )
        payload = portal_alignment_metrics_to_dict(metrics)

        self.assertTrue(payload["applied"])
        self.assertTrue(payload["accepted"])
        self.assertEqual(payload["cov_scale"], PORTAL_DOWNWEIGHT_COV_SCALE)
        self.assertIn("position_m", payload["rmse_before"])
        self.assertIn("rotation_deg", payload["rmse_after"])
        self.assertIn("position_m", payload["rmse_trusted_before"])
        self.assertIn("rotation_deg", payload["rmse_trusted_after"])
        portal = next(p for p in payload["portals"] if p["portal_id"] == "P1")
        self.assertIn("detection_count", portal)
        self.assertIn("position_spread_m", portal["before"])
        self.assertNotIn("detection_count", portal["before"])
        overlap = next(o for o in payload["overlaps"] if o["portal_id"] == "P1")
        self.assertAlmostEqual(overlap["before"]["position_gap_m"], 0.5)
        self.assertEqual(len(payload["downweighted_edges"]), 1)
        self.assertAlmostEqual(payload["downweighted_edges"][0]["position_gap_m"], 0.5)

    def test_save_manifest_json_writes_portal_alignment_metrics(self):
        metrics = build_portal_alignment_metrics(
            _multi_scan_aligned({"scan_a": {"P1": [_pose(0.0, 0.0, 0.0)]}}),
            _multi_scan_aligned({"scan_a": {"P1": [_pose(0.0, 0.0, 0.0)]}}),
            flagged_edges=[],
            applied=False,
            accepted=False,
        )
        payload = portal_alignment_metrics_to_dict(metrics)

        with tempfile.TemporaryDirectory() as tmpdir:
            job_root = Path(tmpdir)
            manifest_path = job_root / "refined_manifest.json"
            save_manifest_json(
                {},
                manifest_path,
                job_root,
                job_status="refined",
                portal_alignment_metrics=payload,
            )
            manifest = json.loads(manifest_path.read_text())

        self.assertIn("portal_alignment_metrics", manifest)
        self.assertFalse(manifest["portal_alignment_metrics"]["applied"])


class ComputeOverlapRmseTests(unittest.TestCase):
    def test_excludes_downweighted_keys(self):
        edges = [
            PortalOverlapEdge("a", "b", "P1", 0.3, 1.0),
            PortalOverlapEdge("a", "c", "P1", 0.4, 2.0),
            PortalOverlapEdge("b", "c", "P1", 2.0, 10.0),
        ]
        exclude = {logical_portal_edge_key("b", "c", "P1")}
        rmse = compute_overlap_rmse(edges, exclude_keys=exclude)
        expected_pos = math.sqrt((0.3 ** 2 + 0.4 ** 2) / 2)
        expected_rot = math.sqrt((1.0 ** 2 + 2.0 ** 2) / 2)
        self.assertAlmostEqual(rmse.position_m, expected_pos, places=6)
        self.assertAlmostEqual(rmse.rotation_deg, expected_rot, places=6)

    def test_empty_after_exclude_returns_zero(self):
        edges = [PortalOverlapEdge("a", "b", "P1", 0.5, 1.0)]
        exclude = {logical_portal_edge_key("a", "b", "P1")}
        rmse = compute_overlap_rmse(edges, exclude_keys=exclude)
        self.assertEqual(rmse.position_m, 0.0)
        self.assertEqual(rmse.rotation_deg, 0.0)


class TrustedAcceptGateTests(unittest.TestCase):
    def test_accepts_when_full_rot_worsens_but_trusted_improves(self):
        """Downweighted edge rot worsens; trusted edges improve → accept."""
        before_edges = [
            PortalOverlapEdge("a", "b", "P1", 0.50, 3.0),
            PortalOverlapEdge("a", "c", "P1", 0.50, 3.0),
            PortalOverlapEdge("b", "c", "P1", 2.00, 5.0),
        ]
        after_edges = [
            PortalOverlapEdge("a", "b", "P1", 0.40, 2.5),
            PortalOverlapEdge("a", "c", "P1", 0.40, 2.5),
            PortalOverlapEdge("b", "c", "P1", 2.50, 8.0),
        ]
        downweighted = {logical_portal_edge_key("b", "c", "P1")}
        trusted_before = compute_overlap_rmse(before_edges, exclude_keys=downweighted)
        trusted_after = compute_overlap_rmse(after_edges, exclude_keys=downweighted)
        self.assertTrue(accept_portal_spread_rmse(trusted_before, trusted_after))
        full_before = compute_overlap_rmse(before_edges)
        full_after = compute_overlap_rmse(after_edges)
        self.assertGreater(full_after.rotation_deg, full_before.rotation_deg)

    def test_reverts_when_trusted_worsens(self):
        before_edges = [
            PortalOverlapEdge("a", "b", "P1", 0.30, 1.0),
            PortalOverlapEdge("a", "c", "P1", 0.30, 1.0),
            PortalOverlapEdge("b", "c", "P1", 2.00, 5.0),
        ]
        after_edges = [
            PortalOverlapEdge("a", "b", "P1", 0.50, 1.5),
            PortalOverlapEdge("a", "c", "P1", 0.50, 1.5),
            PortalOverlapEdge("b", "c", "P1", 1.50, 4.0),
        ]
        downweighted = {logical_portal_edge_key("b", "c", "P1")}
        trusted_before = compute_overlap_rmse(before_edges, exclude_keys=downweighted)
        trusted_after = compute_overlap_rmse(after_edges, exclude_keys=downweighted)
        self.assertFalse(accept_portal_spread_rmse(trusted_before, trusted_after))


class AcceptPortalSpreadRmseTests(unittest.TestCase):
    def test_accepts_mild_rotation_worsen(self):
        before = PortalSpreadRmse(position_m=0.5, rotation_deg=1.5)
        after = PortalSpreadRmse(position_m=0.5, rotation_deg=2.9)
        self.assertTrue(accept_portal_spread_rmse(before, after))

    def test_rejects_rotation_at_or_above_double(self):
        before = PortalSpreadRmse(position_m=0.5, rotation_deg=2.0)
        after = PortalSpreadRmse(position_m=0.5, rotation_deg=4.1)
        self.assertFalse(accept_portal_spread_rmse(before, after))

    def test_rejects_rotation_absolute_worsen_at_or_above_five_deg(self):
        before = PortalSpreadRmse(position_m=0.5, rotation_deg=1.0)
        after = PortalSpreadRmse(position_m=0.5, rotation_deg=6.5)
        self.assertFalse(accept_portal_spread_rmse(before, after))

    def test_accepts_mild_position_worsen(self):
        before = PortalSpreadRmse(position_m=0.10, rotation_deg=1.0)
        after = PortalSpreadRmse(position_m=0.19, rotation_deg=1.0)
        self.assertTrue(accept_portal_spread_rmse(before, after))

    def test_rejects_position_at_or_above_double(self):
        before = PortalSpreadRmse(position_m=0.10, rotation_deg=1.0)
        after = PortalSpreadRmse(position_m=0.21, rotation_deg=1.0)
        self.assertFalse(accept_portal_spread_rmse(before, after))

    def test_rejects_position_absolute_worsen_at_or_above_twenty_cm(self):
        before = PortalSpreadRmse(position_m=0.10, rotation_deg=1.0)
        after = PortalSpreadRmse(position_m=0.31, rotation_deg=1.0)
        self.assertFalse(accept_portal_spread_rmse(before, after))

    def test_accepts_equal_metrics_without_must_improve(self):
        before = PortalSpreadRmse(position_m=0.5, rotation_deg=3.0)
        same = PortalSpreadRmse(position_m=0.5, rotation_deg=3.0)
        self.assertTrue(accept_portal_spread_rmse(before, same))

    def test_accepts_when_either_axis_improves(self):
        before = PortalSpreadRmse(position_m=0.5, rotation_deg=3.0)
        after_pos = PortalSpreadRmse(position_m=0.4, rotation_deg=3.0)
        after_rot = PortalSpreadRmse(position_m=0.5, rotation_deg=2.5)
        self.assertTrue(accept_portal_spread_rmse(before, after_pos))
        self.assertTrue(accept_portal_spread_rmse(before, after_rot))


if __name__ == "__main__":
    unittest.main()
