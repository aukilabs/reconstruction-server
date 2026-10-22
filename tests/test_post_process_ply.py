import logging
import os
import tempfile
import unittest
from pathlib import Path

import numpy as np
import open3d as o3d

from reconstruction.output.pointcloud import post_process_ply

DRACO_ENCODER = "/usr/local/bin/draco_encoder"


def _vertex_properties(path):
    with open(path, "rb") as f:
        header = []
        for raw in f:
            line = raw.decode("ascii").strip()
            if line == "end_header":
                return header
            header.append(line)
    raise AssertionError(f"no end_header in {path}")


class PostProcessPlyTests(unittest.TestCase):
    """post_process_ply writes the global/update point clouds that the runners upload."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls._tmp.name)
        # A dense 10x10x10 grid (2 cm spacing) in OpenCV coordinates, plus one far outlier.
        axis = np.arange(10) * 0.02
        grid = np.stack(np.meshgrid(axis, axis, axis, indexing="ij"), axis=-1).reshape(-1, 3)
        points = np.vstack([grid + [0.0, 2.0, 4.0], [[50.0, 50.0, 50.0]]])
        pcd = o3d.geometry.PointCloud(o3d.utility.Vector3dVector(points))
        pcd.colors = o3d.utility.Vector3dVector(np.full_like(points, 0.5))
        o3d.io.write_point_cloud(str(cls.out / "RefinedPointCloud.ply"), pcd)
        # filter_ply calls logger.info unconditionally, so a logger is required.
        post_process_ply(cls.out, logger=logging.getLogger("test_post_process_ply"))

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_point_cloud_is_filtered_and_converted_to_opengl(self):
        pcd = o3d.io.read_point_cloud(str(self.out / "RefinedPointCloud.ply"))
        points = np.asarray(pcd.points)
        self.assertGreater(len(points), 0)
        self.assertLess(len(points), 1000)  # outlier removed, then voxel + random downsampling
        # OpenCV -> OpenGL: x' = y, y' = x, z' = -z. Voxel averaging keeps points inside the grid.
        np.testing.assert_array_less([2.0 - 1e-6, -1e-6, -4.18 - 1e-6], points.min(axis=0))
        np.testing.assert_array_less(points.max(axis=0), [2.18 + 1e-6, 0.18 + 1e-6, -4.0 + 1e-6])

    def test_reduced_copy_is_written(self):
        # Under the 20 MB limit the reduced PLY is a plain copy.
        reduced = (self.out / "RefinedPointCloudReduced.ply").read_bytes()
        self.assertEqual(reduced, (self.out / "RefinedPointCloud.ply").read_bytes())

    def test_float_ply_for_draco(self):
        header = _vertex_properties(self.out / "RefinedPointCloudFloat.ply")
        for axis in "xyz":
            self.assertIn(f"property float {axis}", header)
        self.assertNotIn("property double x", header)

    @unittest.skipUnless(os.access(DRACO_ENCODER, os.X_OK), "draco_encoder is only in the Docker image")
    def test_draco_file(self):
        self.assertGreater((self.out / "RefinedPointCloud.ply.drc").stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
