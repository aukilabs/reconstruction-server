import math
import unittest

import numpy as np
import pycolmap

from reconstruction.pipeline.global_map.alignment import AlignedScans
from reconstruction.pipeline.global_map.portal_metrics import compute_portal_spread_metrics


def _pose(x: float, y: float, z: float, quat=None) -> pycolmap.Rigid3d:
    if quat is None:
        quat = np.array([-0.7071068, 0.0, 0.0, 0.7071068])
    return pycolmap.Rigid3d(pycolmap.Rotation3d(quat), np.array([x, y, z]))


def _aligned_scans(portal_detections: dict) -> AlignedScans:
    return AlignedScans(
        scan_ids=["scan_a"],
        alignment_transforms={},
        raw_portal_detections={},
        aligned_portal_detections={"scan_a": portal_detections},
    )


class PortalSpreadMetricsTests(unittest.TestCase):
    def test_single_detection_zero_spread(self):
        detections = {"PORTAL1": [_pose(1.0, 0.0, 2.0)]}
        metrics = compute_portal_spread_metrics(_aligned_scans(detections))
        m = metrics["PORTAL1"]
        self.assertEqual(m.detection_count, 1)
        self.assertEqual(m.position_spread_m, 0.0)
        self.assertEqual(m.rotation_spread_deg, 0.0)

    def test_position_rms_spread(self):
        detections = {
            "PORTAL1": [
                _pose(0.0, 0.0, 0.0),
                _pose(0.3, 0.0, 0.0),
                _pose(-0.3, 0.0, 0.0),
            ]
        }
        metrics = compute_portal_spread_metrics(_aligned_scans(detections))
        expected = math.sqrt((0.3 ** 2 + 0.3 ** 2) / 3)
        self.assertAlmostEqual(metrics["PORTAL1"].position_spread_m, expected, places=6)
        self.assertEqual(metrics["PORTAL1"].detection_count, 3)

    def test_rotation_spread_nonzero(self):
        base = np.array([-0.7071068, 0.0, 0.0, 0.7071068])
        tilt = np.array([-0.700, 0.08, 0.0, 0.7071068])
        detections = {
            "PORTAL1": [
                _pose(0.0, 0.0, 0.0, base),
                _pose(0.0, 0.0, 0.0, tilt),
            ]
        }
        metrics = compute_portal_spread_metrics(_aligned_scans(detections))
        self.assertGreater(metrics["PORTAL1"].rotation_spread_deg, 0.0)
        self.assertLess(metrics["PORTAL1"].rotation_spread_deg, 20.0)

    def test_empty_detections_returns_empty(self):
        aligned = AlignedScans(
            scan_ids=[],
            alignment_transforms={},
            raw_portal_detections={},
            aligned_portal_detections={},
        )
        self.assertEqual(compute_portal_spread_metrics(aligned), {})


if __name__ == "__main__":
    unittest.main()
