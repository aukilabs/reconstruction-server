import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from reconstruction.io.manifest import previous_refined_scan_files
from reconstruction.io.poses import parse_portals_from_poses, parse_portals_from_poses_colmap


class ParsePortalsFromPosesTests(unittest.TestCase):
    def test_reads_lighthouse_pose_and_skips_entries_without_one(self):
        payload = {
            "poses": [
                {
                    "short_id": "qr-a",
                    "px": 1.0,
                    "py": 2.0,
                    "pz": 3.0,
                    "rx": 0.0,
                    "ry": 0.0,
                    "rz": 0.0,
                    "rw": 1.0,
                    "reported_size": 0.15,
                },
                {"short_id": "qr-missing"},
                {
                    "px": 0.0,
                    "py": 0.0,
                    "pz": 0.0,
                    "rx": 0.0,
                    "ry": 0.0,
                    "rz": 0.0,
                    "rw": 1.0,
                },
                {
                    "shortId": "qr-b",
                    "px": 4.0,
                    "py": 5.0,
                    "pz": 6.0,
                    "rx": 0.0,
                    "ry": 0.0,
                    "rz": 0.0,
                    "rw": 1.0,
                    "reportedSize": 0.2,
                },
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "poses.json"
            path.write_text(json.dumps(payload))
            portals = parse_portals_from_poses(path)

        self.assertEqual(set(portals), {"qr-a", "qr-b"})
        rotation, translation, size = portals["qr-a"]
        np.testing.assert_allclose(rotation, np.eye(3))
        np.testing.assert_allclose(translation, [1.0, 2.0, 3.0])
        self.assertEqual(size, 0.15)
        self.assertEqual(portals["qr-b"][2], 0.2)

    def test_colmap_loader_swaps_axes_and_converts_size_to_meters(self):
        payload = {
            "poses": [
                {
                    "short_id": "qr-a",
                    "px": 1.0,
                    "py": 2.0,
                    "pz": 3.0,
                    "rx": 0.0,
                    "ry": 0.0,
                    "rz": 0.0,
                    "rw": 1.0,
                    "reported_size": 15,
                },
                {
                    "shortId": "qr-b",
                    "px": 0.0,
                    "py": 0.0,
                    "pz": 0.0,
                    "rx": 0.0,
                    "ry": 0.0,
                    "rz": 0.0,
                    "rw": 1.0,
                },
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "poses.json"
            path.write_text(json.dumps(payload))
            portals = parse_portals_from_poses_colmap(path)

        rotation, translation, size = portals["qr-a"]
        np.testing.assert_allclose(rotation, np.eye(3))
        np.testing.assert_allclose(translation, [2.0, 1.0, -3.0])
        self.assertEqual(size, 0.15)
        self.assertIsNone(portals["qr-b"][2])

    def test_empty_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "poses.json"
            path.write_text(json.dumps({"poses": []}))
            self.assertEqual(parse_portals_from_poses(path), {})


class PreviousRefinedScanFilesTests(unittest.TestCase):
    def test_keeps_only_refined_scan_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "refined_manifest.json"
            path.write_text(json.dumps({
                "dataIDs": [
                    "refined_scan_2026-01-01_00-00-00",
                    "colmap_images_bin_2026-03-17_16-56-19",
                    "refined_scan_2026-02-02_00-00-00",
                ]
            }))
            self.assertEqual(
                previous_refined_scan_files(path),
                [
                    "refined_scan_2026-01-01_00-00-00",
                    "refined_scan_2026-02-02_00-00-00",
                ],
            )


if __name__ == "__main__":
    unittest.main()
