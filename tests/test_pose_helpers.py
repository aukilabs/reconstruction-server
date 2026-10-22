import unittest

import numpy as np
import pycolmap
from scipy.spatial.transform import Rotation

from reconstruction._legacy.data_utils import (
    average_quaternions_svd,
    convert_pose_colmap_to_opengl,
    convert_pose_opengl_to_colmap,
    is_floor_portal,
    mean_pose,
    quaternion_to_rotation_matrix,
    vec3_angle,
)

# Quaternions are (x, y, z, w) throughout, as in pycolmap and scipy.
RANDOM_QUATS = Rotation.random(20, random_state=7).as_quat()
# OpenGL <-> COLMAP swaps x and y and flips z. That is a proper rotation, applied by conjugation.
AXIS_SWAP = np.array([[0.0, 1.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.0, -1.0]])


def _same_rotation(q1, q2):
    np.testing.assert_allclose(Rotation.from_quat(q1).as_matrix(), Rotation.from_quat(q2).as_matrix(), atol=1e-9)


class OpenGlColmapConversionTests(unittest.TestCase):
    def test_axis_mapping(self):
        position, quat = convert_pose_opengl_to_colmap([1.0, 2.0, 3.0], [0.1, 0.2, 0.3, 0.9])
        np.testing.assert_array_equal(position, [2.0, 1.0, -3.0])
        np.testing.assert_array_equal(quat, [0.2, 0.1, -0.3, 0.9])

    def test_conversion_conjugates_the_rotation(self):
        for q in RANDOM_QUATS:
            _, converted = convert_pose_opengl_to_colmap(np.zeros(3), q)
            expected = AXIS_SWAP @ Rotation.from_quat(q).as_matrix() @ AXIS_SWAP.T
            np.testing.assert_allclose(Rotation.from_quat(converted).as_matrix(), expected, atol=1e-9)

    def test_round_trip(self):
        position = np.array([0.5, -1.5, 2.5])
        for q in RANDOM_QUATS:
            p1, q1 = convert_pose_opengl_to_colmap(position, q)
            p2, q2 = convert_pose_colmap_to_opengl(p1, q1)
            np.testing.assert_allclose(p2, position)
            np.testing.assert_allclose(q2, q)


class QuaternionHelperTests(unittest.TestCase):
    def test_quaternion_to_rotation_matrix_matches_scipy(self):
        for q in RANDOM_QUATS:
            np.testing.assert_allclose(quaternion_to_rotation_matrix(q), Rotation.from_quat(q).as_matrix(), atol=1e-9)

    def test_average_of_identical_quaternions(self):
        q = RANDOM_QUATS[0]
        _same_rotation(average_quaternions_svd(np.array([q, q, q])), q)

    def test_average_ignores_quaternion_sign(self):
        q = RANDOM_QUATS[1]
        _same_rotation(average_quaternions_svd(np.array([q, -q])), q)

    def test_average_of_symmetric_rotations(self):
        about_y = [Rotation.from_euler("y", deg, degrees=True).as_quat() for deg in (-10.0, 10.0)]
        _same_rotation(average_quaternions_svd(np.array(about_y)), [0.0, 0.0, 0.0, 1.0])


class MeanPoseTests(unittest.TestCase):
    def test_single_pose_is_returned_unchanged(self):
        pose = pycolmap.Rigid3d(pycolmap.Rotation3d(RANDOM_QUATS[2]), np.array([1.0, 2.0, 3.0]))
        self.assertIs(mean_pose([pose]), pose)

    def test_translations_are_averaged(self):
        rotation = pycolmap.Rotation3d(RANDOM_QUATS[3])
        poses = [pycolmap.Rigid3d(rotation, np.array(t)) for t in ([0.0, 0.0, 0.0], [2.0, 4.0, -6.0])]
        mean = mean_pose(poses)
        np.testing.assert_allclose(mean.translation, [1.0, 2.0, -3.0])
        _same_rotation(mean.rotation.quat, RANDOM_QUATS[3])


class FloorPortalTests(unittest.TestCase):
    # In COLMAP coordinates x is height, and a floor portal's z axis points down (-x).
    FACING_DOWN = Rotation.from_euler("y", -90.0, degrees=True).as_quat()

    def _pose(self, quat, position):
        return pycolmap.Rigid3d(pycolmap.Rotation3d(quat), np.array(position, dtype=float))

    def test_flat_portal_at_floor_height(self):
        self.assertTrue(is_floor_portal(self._pose(self.FACING_DOWN, [0.1, 3.0, -2.0])))

    def test_flat_portal_above_floor(self):
        self.assertFalse(is_floor_portal(self._pose(self.FACING_DOWN, [1.2, 3.0, -2.0])))

    def test_wall_portal_at_floor_height(self):
        self.assertFalse(is_floor_portal(self._pose([0.0, 0.0, 0.0, 1.0], [0.1, 3.0, -2.0])))


class Vec3AngleTests(unittest.TestCase):
    def test_angles(self):
        self.assertAlmostEqual(vec3_angle(np.array([1.0, 0, 0]), np.array([0, 2.0, 0])), 90.0)
        self.assertAlmostEqual(vec3_angle(np.array([1.0, 1.0, 0]), np.array([2.0, 2.0, 0])), 0.0, places=5)
        self.assertAlmostEqual(vec3_angle(np.array([1.0, 0, 0]), np.array([-3.0, 0, 0])), 180.0)


if __name__ == "__main__":
    unittest.main()
