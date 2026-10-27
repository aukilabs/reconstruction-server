import logging
import unittest

import numpy as np
import pycolmap
from scipy.spatial.transform import Rotation

from tools.auki_session import segment, stitch_qr, stitch_trajectory
from tools.auki_session.rig_merge import merge_reconstructions_preserving_rigs


class SegmentWindowTests(unittest.TestCase):
    def test_sliding_windows_with_a_partial_tail(self):
        windows = segment.compute_segment_windows(0, int(25e9), window_s=10.0, overlap_s=2.0)
        self.assertEqual([w.segment_id for w in windows], ["segment_000", "segment_001", "segment_002", "segment_003"])
        self.assertEqual([w.start_ns for w in windows], [0, int(8e9), int(16e9), int(24e9)])
        self.assertEqual(windows[-1].end_ns, int(25e9))
        self.assertEqual([w.is_partial for w in windows], [False, False, True, True])

    def test_overlap_must_be_shorter_than_the_window(self):
        with self.assertRaises(ValueError):
            segment.compute_segment_windows(0, int(10e9), window_s=5.0, overlap_s=5.0)


class KabschTests(unittest.TestCase):
    def test_recovers_a_rigid_transform(self):
        rng = np.random.default_rng(3)
        points = rng.normal(size=(20, 3))
        rotation = Rotation.from_euler("xyz", [10, -20, 30], degrees=True).as_matrix()
        translation = np.array([1.0, -2.0, 0.5])
        R, t = stitch_trajectory.kabsch_rigid_transform(points, points @ rotation.T + translation)
        np.testing.assert_allclose(R, rotation, atol=1e-12)
        np.testing.assert_allclose(t, translation, atol=1e-12)


def _segment_reconstruction(timestamp, offset):
    """Two cameras (cam_a, cam_b) on one rig, one frame, one point seen by both."""
    rec = pycolmap.Reconstruction()
    for camera_id in (1, 2):
        rec.add_camera(pycolmap.Camera(model="PINHOLE", width=640, height=480,
                                       params=[500.0, 500.0, 320.0, 240.0], camera_id=camera_id))
    rig = pycolmap.Rig()
    rig.rig_id = 1
    ref = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=1)
    other = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=2)
    rig.add_ref_sensor(ref)
    rig.add_sensor(other, pycolmap.Rigid3d(pycolmap.Rotation3d(), np.array([0.1, 0.0, 0.0])))
    rec.add_rig(rig)

    frame = pycolmap.Frame()
    frame.frame_id = 1
    frame.rig_id = 1
    frame.rig_from_world = pycolmap.Rigid3d(pycolmap.Rotation3d(), np.array([offset, 0.0, 0.0]))
    frame.add_data_id(pycolmap.data_t(sensor_id=ref, id=1))
    frame.add_data_id(pycolmap.data_t(sensor_id=other, id=2))
    rec.add_frame(frame)
    for image_id, sensor in ((1, "cam_a"), (2, "cam_b")):
        image = pycolmap.Image(f"{sensor}/{timestamp}.jpg",
                               pycolmap.Point2DList([pycolmap.Point2D(np.array([320.0, 240.0]))]), image_id, image_id)
        image.frame_id = 1
        rec.add_image(image)
    rec.register_frame(1)
    track = pycolmap.Track()
    track.add_element(1, 0)
    track.add_element(2, 0)
    rec.add_point3D(np.array([0.0, 0.0, 5.0]), track, np.array([255, 0, 0], dtype=np.uint8))
    return rec


class StitchQrTests(unittest.TestCase):
    def test_pose_matrix(self):
        quat = Rotation.from_euler("z", 90, degrees=True).as_quat()
        T = stitch_qr.rigid_4x4_from_position_quat([1.0, 2.0, 3.0], quat)
        np.testing.assert_allclose(T[:3, :3], Rotation.from_quat(quat).as_matrix(), atol=1e-12)
        np.testing.assert_allclose(T[:3, 3], [1.0, 2.0, 3.0])


class RigMergeTests(unittest.TestCase):
    def test_keeps_one_camera_per_sensor_and_the_rig(self):
        merged, stats = merge_reconstructions_preserving_rigs(
            [_segment_reconstruction(1, 0.0), lambda: _segment_reconstruction(2, 1.0)],
            labels=["segment_000", "segment_001"],
            logger=logging.getLogger("test"),
        )
        self.assertEqual((stats.n_cameras, stats.n_rigs, stats.n_frames, stats.n_images, stats.n_points3D),
                         (2, 1, 2, 4, 2))
        self.assertEqual(stats.sensor_keys, ["cam_a", "cam_b"])
        self.assertEqual(stats.camera_param_conflicts, [])
        self.assertEqual(stats.extrinsics_conflicts, [])
        self.assertEqual(merged.rigs[1].num_sensors(), 2)


if __name__ == "__main__":
    unittest.main()
