import unittest

import numpy as np
from scipy.spatial.transform import Rotation

from reconstruction.geometry import frames
from reconstruction.geometry.frames import COLMAP_WORLD, OPENGL, convention_matrix, convert_pose_convention

# Quaternions are (x, y, z, w) throughout, as in pycolmap and scipy.
RANDOM_QUATS = Rotation.random(20, random_state=7).as_quat()
# OpenGL <-> COLMAP world swaps x and y and flips z. That is a proper rotation, applied by conjugation.
AXIS_SWAP = np.array([[0.0, 1.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.0, -1.0]])

# Each axis word as a vector in one fixed right-handed basis (x forward, y left, z up).
DIRECTIONS = {
    "forward": (1, 0, 0), "backward": (-1, 0, 0),
    "left": (0, 1, 0), "right": (0, -1, 0),
    "up": (0, 0, 1), "down": (0, 0, -1),
}
DECLARED_FRAMES = [value for value in vars(frames).values() if isinstance(value, dict) and "axes" in value]


def _rotation(q):
    return Rotation.from_quat(q).as_matrix()


class FrameDeclarationTests(unittest.TestCase):
    def test_frames_are_declared(self):
        self.assertIn(OPENGL, DECLARED_FRAMES)
        self.assertIn(COLMAP_WORLD, DECLARED_FRAMES)

    def test_axes_match_declared_handedness(self):
        # The SDK trusts the declared handedness without checking it against the axes.
        for frame in DECLARED_FRAMES:
            basis = np.array([DIRECTIONS[frame["axes"][axis]] for axis in "xyz"])
            expected = 1 if frame["handedness"] == "right" else -1
            self.assertEqual(round(np.linalg.det(basis)), expected, frame["frame_id"])


class ConventionConversionTests(unittest.TestCase):
    def test_axis_mapping(self):
        position, quat = convert_pose_convention([1.0, 2.0, 3.0], [0.0, 0.0, 0.0, 1.0], OPENGL, COLMAP_WORLD)
        np.testing.assert_array_equal(position, [2.0, 1.0, -3.0])
        np.testing.assert_allclose(_rotation(quat), np.eye(3), atol=1e-12)

    def test_conversion_conjugates_the_rotation(self):
        for q in RANDOM_QUATS:
            _, converted = convert_pose_convention(np.zeros(3), q, OPENGL, COLMAP_WORLD)
            expected = AXIS_SWAP @ _rotation(q) @ AXIS_SWAP.T
            np.testing.assert_allclose(_rotation(converted), expected, atol=1e-12)

    def test_round_trip(self):
        position = np.array([0.5, -1.5, 2.5])
        for q in RANDOM_QUATS:
            p1, q1 = convert_pose_convention(position, q, OPENGL, COLMAP_WORLD)
            p2, q2 = convert_pose_convention(p1, q1, COLMAP_WORLD, OPENGL)
            np.testing.assert_array_equal(p2, position)
            np.testing.assert_allclose(_rotation(q2), _rotation(q), atol=1e-12)

    def test_convention_matrix(self):
        expected = np.eye(4)
        expected[:3, :3] = AXIS_SWAP
        np.testing.assert_array_equal(convention_matrix(OPENGL, COLMAP_WORLD), expected)
        np.testing.assert_array_equal(convention_matrix(COLMAP_WORLD, OPENGL), expected)


if __name__ == "__main__":
    unittest.main()
