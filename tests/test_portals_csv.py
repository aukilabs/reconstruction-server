import csv
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pycolmap

from reconstruction.io.portals import save_portal_csv
from reconstruction.io.portals import Portal, read_portal_csv, write_portal_csv

# portals.csv has no header. Each row: image_id, short_id, size, px, py, pz, qx, qy, qz, qw, then corner x, y pairs.
CORNERS = [(982.905, 624.91), (1076.52, 725.505), (982.816, 819.901), (894.465, 719.901)]


class PortalsCsvTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "portals.csv"

    def tearDown(self):
        self._tmp.cleanup()

    def test_save_portal_csv_round_trip(self):
        # save_portal_csv is the writer local refinement uses.
        quat = np.array([-0.0019605859, -0.7071040631, 0.0019605859, 0.7071040631])
        poses = {
            "E3JG04P2C0C": [
                pycolmap.Rigid3d(pycolmap.Rotation3d(quat), np.array([0.1, -4.22, -0.62])),
                pycolmap.Rigid3d(pycolmap.Rotation3d(quat), np.array([0.2, -4.23, -0.63])),
            ],
            "MPA84VWC4E8": [pycolmap.Rigid3d(pycolmap.Rotation3d(quat), np.array([0.0, -6.58, -0.63]))],
        }
        image_ids = {"E3JG04P2C0C": [1, 2], "MPA84VWC4E8": [113]}
        sizes = {"E3JG04P2C0C": 0.1, "MPA84VWC4E8": 0.15}
        corners = {"E3JG04P2C0C": [CORNERS, CORNERS], "MPA84VWC4E8": [CORNERS]}
        save_portal_csv(poses, self.path, image_ids, sizes, corners)

        with open(self.path, newline="") as f:
            rows = list(csv.reader(f))
        self.assertEqual([len(r) for r in rows], [10 + 2 * len(CORNERS)] * 3)
        self.assertEqual([r[:2] for r in rows], [["1", "E3JG04P2C0C"], ["2", "E3JG04P2C0C"], ["113", "MPA84VWC4E8"]])

        portals = read_portal_csv(self.path)
        self.assertEqual([p.id for p in portals], [0, 1, 2])
        self.assertEqual([p.image_id for p in portals], [1, 2, 113])
        self.assertEqual([p.size for p in portals], [0.1, 0.1, 0.15])
        expected = [(sid, pose) for sid, qr_poses in poses.items() for pose in qr_poses]
        for portal, (short_id, pose) in zip(portals, expected):
            self.assertEqual(portal.short_id, short_id)
            np.testing.assert_allclose(portal.tvec, pose.translation)
            np.testing.assert_allclose(portal.qvec, pose.rotation.quat)
            self.assertEqual(portal.corners, CORNERS)

    def test_write_portal_csv_round_trip(self):
        portals = [
            Portal(id=0, short_id="A", qvec=np.array([0.0, 0.0, 0.0, 1.0]), tvec=np.array([1.0, 2.0, 3.0]),
                   image_id=7, corners=CORNERS, size=0.1),
            Portal(id=1, short_id="B", qvec=np.array([0.5, 0.5, 0.5, 0.5]), tvec=np.array([-1.0, 0.0, 0.25]),
                   image_id=9, corners=[], size=0.2),
        ]
        write_portal_csv(portals, self.path)
        read = read_portal_csv(self.path)
        self.assertEqual(len(read), 2)
        for original, loaded in zip(portals, read):
            self.assertEqual(loaded.id, original.id)
            self.assertEqual(loaded.short_id, original.short_id)
            self.assertEqual(loaded.image_id, original.image_id)
            self.assertEqual(loaded.size, original.size)
            self.assertEqual(loaded.corners, original.corners)
            np.testing.assert_array_equal(loaded.qvec, original.qvec)
            np.testing.assert_array_equal(loaded.tvec, original.tvec)

    def test_write_portal_csv_without_portals_writes_no_file(self):
        # Without portals.csv the local runner skips uploading the scan (see test_upload_contract.py).
        write_portal_csv([], self.path)
        self.assertFalse(self.path.exists())


if __name__ == "__main__":
    unittest.main()
