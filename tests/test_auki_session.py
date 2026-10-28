import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pycolmap
from scipy.spatial.transform import Rotation

from reconstruction.geometry.auki_conventions import (
    convert_colmap_points_to_qr_origin_auki,
    convert_colmap_pose_to_qr_origin_auki,
    convert_to_colmap_world,
)
from reconstruction.geometry.frames import AUKI_ROBOT_WORLD
from reconstruction.io.auki_session import PoseSample, SensorInfo, analyze_sensor_motion, resolve_auki_session_id
from reconstruction.pipeline.auki_session.refine import check_rig_offsets_unchanged, fixed_intrinsics_sensor_ids
from reconstruction.pipeline.auki_session.rig import seed_camera_model


def _pose(position, rotvec_deg=(0.0, 0.0, 0.0)):
    quat = Rotation.from_rotvec(rotvec_deg, degrees=True).as_quat()
    return pycolmap.Rigid3d(pycolmap.Rotation3d(quat), np.array(position, dtype=float))


class SensorMotionTests(unittest.TestCase):
    def _stats(self, positions, declared):
        samples = [PoseSample(i, _pose(p)) for i, p in enumerate(positions)]
        return analyze_sensor_motion({"cam": samples}, {"cam": declared})["cam"]

    def test_parked_movable_camera_joins_the_rig(self):
        stat = self._stats([[0.1, 0.0, 1.0], [0.1004, 0.0, 1.0], [0.0998, 0.0, 1.0]], "movable")
        self.assertTrue(stat.is_static)
        self.assertEqual(stat.effective_mode, "rigid")

    def test_moving_camera_stays_movable(self):
        stat = self._stats([[0.1, 0.0, 1.0], [0.3, 0.0, 1.0]], "movable")
        self.assertFalse(stat.is_static)
        self.assertEqual(stat.effective_mode, "movable")

    def test_declared_rigid_stays_rigid(self):
        stat = self._stats([[0.1, 0.0, 1.0], [0.3, 0.0, 1.0]], "rigid")
        self.assertEqual(stat.effective_mode, "rigid")


class SeedCameraModelTests(unittest.TestCase):
    INFO = SensorInfo("cam", "cam_optical", 1280, 960, "kannala_brandt", "rigid")

    def _record(self, intrinsics):
        return SimpleNamespace(dynamic_intrinsics=intrinsics)

    def test_external_intrinsics_win(self):
        external = {"fx": 600.0, "fy": 601.0, "cx": 640.0, "cy": 480.0, "dist": [0.1]}
        model = seed_camera_model(self.INFO, [self._record((1.0, 1.0, 1.0, 1.0, []))], external_intrinsics=external)
        self.assertEqual(model["source"], "external_recorded")
        self.assertEqual(model["params"], [600.0, 601.0, 640.0, 480.0, 0.1, 0.0, 0.0, 0.0])

    def test_recorded_intrinsics(self):
        model = seed_camera_model(self.INFO, [self._record(None), self._record((500.0, 502.0, 630.0, 470.0, [0.1, 0.2, 0.3, 0.4, 0.5]))])
        self.assertEqual(model["source"], "recorded")
        self.assertEqual(model["model"], "OPENCV_FISHEYE")
        self.assertEqual(model["params"], [500.0, 502.0, 630.0, 470.0, 0.1, 0.2, 0.3, 0.4])

    def test_fov_guess(self):
        model = seed_camera_model(self.INFO, [], assumed_hfov_deg=90.0)
        self.assertEqual(model["source"], "seeded_fov_guess")
        np.testing.assert_allclose(model["params"][:4], [640.0, 640.0, 640.0, 480.0])

    PLUMB_BOB = INFO._replace(distortion_model="plumb_bob")
    D405_DIST = [-0.050, 0.053, 0.0012, 0.0004, -0.018]

    def test_recorded_plumb_bob_with_k3_keeps_it(self):
        model = seed_camera_model(self.PLUMB_BOB, [self._record((652.0, 651.0, 636.0, 354.0, self.D405_DIST))])
        self.assertEqual(model["model"], "FULL_OPENCV")
        self.assertEqual(model["params"], [652.0, 651.0, 636.0, 354.0, *self.D405_DIST, 0.0, 0.0, 0.0])

    def test_external_plumb_bob_with_k3_keeps_it(self):
        external = {"fx": 652.0, "fy": 651.0, "cx": 636.0, "cy": 354.0, "dist": self.D405_DIST}
        model = seed_camera_model(self.PLUMB_BOB, external_intrinsics=external)
        self.assertEqual(model["model"], "FULL_OPENCV")
        self.assertEqual(len(model["params"]), 12)

    def test_plumb_bob_without_k3_stays_opencv(self):
        for dist in ([0.1, 0.2, 0.3, 0.4], [0.1, 0.2, 0.3, 0.4, 0.0]):
            model = seed_camera_model(self.PLUMB_BOB, [self._record((1.0, 1.0, 1.0, 1.0, dist))])
            self.assertEqual(model["model"], "OPENCV")
            self.assertEqual(model["params"], [1.0, 1.0, 1.0, 1.0, 0.1, 0.2, 0.3, 0.4])

    def test_fov_guess_never_full_opencv(self):
        model = seed_camera_model(self.PLUMB_BOB, [])
        self.assertEqual(model["model"], "OPENCV")
        self.assertEqual(len(model["params"]), 8)

    def test_unknown_distortion_model(self):
        with self.assertRaises(ValueError):
            seed_camera_model(self.INFO._replace(distortion_model="division"))


class FixedIntrinsicsTests(unittest.TestCase):
    def test_only_guessed_intrinsics_are_left_to_refine(self):
        sources = {"arm": "recorded", "head": "seeded_fov_guess", "rear": "external_recorded"}
        self.assertEqual(fixed_intrinsics_sensor_ids(sources), ["arm", "rear"])


class SessionDiscoveryTests(unittest.TestCase):
    def test_single_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "registries").mkdir()
            (Path(tmp) / "R001-abc" / "sensorlogs").mkdir(parents=True)
            self.assertEqual(resolve_auki_session_id(tmp), "R001-abc")

    def test_several_sessions_need_an_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            for session in ("a", "b"):
                (Path(tmp) / session / "sensorlogs").mkdir(parents=True)
            with self.assertRaises(ValueError):
                resolve_auki_session_id(tmp)


class AukiConventionTests(unittest.TestCase):
    def test_robot_world_to_colmap_world(self):
        # Robot world: x forward, y left, z up. COLMAP world: x up, y right, z forward.
        world_from_camera = _pose([1.0, 2.0, 3.0])
        converted = convert_to_colmap_world(world_from_camera, AUKI_ROBOT_WORLD)
        np.testing.assert_allclose(converted.translation, [3.0, -2.0, 1.0], atol=1e-12)

    def test_only_the_world_side_is_relabelled(self):
        # A camera pose's own axes are already COLMAP's, so a camera at the origin facing along
        # the robot's forward axis has its optical axis along COLMAP world z (forward) afterwards.
        robot_from_camera = Rotation.from_matrix([[0, 0, 1], [-1, 0, 0], [0, -1, 0]]).as_quat()
        world_from_camera = pycolmap.Rigid3d(pycolmap.Rotation3d(robot_from_camera), np.zeros(3))
        converted = convert_to_colmap_world(world_from_camera, AUKI_ROBOT_WORLD)
        np.testing.assert_allclose(converted.rotation.matrix() @ [0, 0, 1], [0, 0, 1], atol=1e-12)

    def test_points_match_poses(self):
        qr0_from_world = _pose([0.5, -1.0, 2.0], (10.0, 20.0, 30.0))
        point = np.array([1.0, 2.0, 3.0])
        pose = convert_colmap_pose_to_qr_origin_auki(_pose(point), qr0_from_world)
        np.testing.assert_allclose(convert_colmap_points_to_qr_origin_auki([point], qr0_from_world)[0], pose.translation, atol=1e-12)


class RigOffsetCheckTests(unittest.TestCase):
    def _rig(self, sensor_from_rig):
        rig = pycolmap.Rig()
        rig.rig_id = 1
        rig.add_ref_sensor(pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=1))
        rig.add_sensor(pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=2), sensor_from_rig)
        rec = pycolmap.Reconstruction()
        for camera_id in (1, 2):
            rec.add_camera(pycolmap.Camera(model="PINHOLE", width=640, height=480,
                                           params=[500.0, 500.0, 320.0, 240.0], camera_id=camera_id))
        rec.add_rig(rig)
        return rec

    def test_changed_offset_raises(self):
        build = SimpleNamespace(main_rig_id=1, camera_id_per_sensor={"ref": 1, "side": 2},
                                reconstruction=self._rig(_pose([0.1, 0.0, 0.0])))
        data = SimpleNamespace(rigid_sensor_ids=["ref", "side"], ref_sensor_id="ref")
        logger = SimpleNamespace(info=lambda *_: None)
        check_rig_offsets_unchanged(build, data, self._rig(_pose([0.1, 0.0, 0.0])), logger)
        with self.assertRaises(RuntimeError):
            check_rig_offsets_unchanged(build, data, self._rig(_pose([0.2, 0.0, 0.0])), logger)


if __name__ == "__main__":
    unittest.main()
