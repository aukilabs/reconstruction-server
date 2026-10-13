#!/usr/bin/env python3
"""Unit tests for PointcloudCovisibilityIndex.

Requires pycolmap. Run inside the reconstruction-server devcontainer:
    python -m pytest tests/test_pointcloud_covisibility.py -v
"""

import sys
from pathlib import Path

import numpy as np
import pytest

# Add project root so utils/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

import pycolmap
from utils.pointcloud_covisibility import PointcloudCovisibilityIndex


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_camera(camera_id: int = 1, w: int = 640, h: int = 480) -> pycolmap.Camera:
    return pycolmap.Camera(
        model="PINHOLE",
        width=w,
        height=h,
        params=[500.0, 500.0, w / 2.0, h / 2.0],
        camera_id=camera_id,
    )


def _make_reconstruction(n_images: int = 5, n_points: int = 200, seed: int = 42):
    """Create a minimal synthetic COLMAP reconstruction.

    Generates cameras in an arc all facing the +Z direction, looking at a cube
    of 3D points centred at (0, 0, 3).
    """
    rec = pycolmap.Reconstruction()
    rng = np.random.RandomState(seed)

    cam = _make_camera(camera_id=1)
    rec.add_camera(cam)

    # 3D points in a cube at z=2.5..3.5
    point3d_ids = []
    for _ in range(n_points):
        xyz = rng.uniform([-0.5, -0.5, 2.5], [0.5, 0.5, 3.5])
        pid = rec.add_point3D(xyz, pycolmap.Track(), np.array([128, 128, 128], dtype=np.uint8))
        point3d_ids.append(pid)

    # Cameras on an arc at z~0, looking toward +Z
    for i in range(1, n_images + 1):
        angle = (i - 1) / max(n_images - 1, 1) * 0.5 - 0.25
        tx = np.sin(angle) * 2.0

        # cam_to_world: identity rotation, position at (tx, 0, 0)
        world_from_cam = pycolmap.Rigid3d(
            pycolmap.Rotation3d(np.eye(3)),
            np.array([tx, 0.0, 0.0]),
        )
        cam_from_world = world_from_cam.inverse()

        rig = pycolmap.Rig()
        rig.rig_id = i
        sensor = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=1)
        rig.add_ref_sensor(sensor)
        rec.add_rig(rig)

        frame = pycolmap.Frame(rig_id=i, rig_from_world=cam_from_world, frame_id=i)
        frame.add_data_id(pycolmap.data_t(sensor_id=sensor, id=i))
        rec.add_frame(frame)

        img = pycolmap.Image(f"image_{i:03d}.jpg", pycolmap.Point2DList([]), i, 1)
        img.frame_id = i
        rec.add_image(img)
        rec.register_frame(i)

    # Assign ~40% of points to each image's tracks
    for pid in point3d_ids:
        track = rec.points3D[pid].track
        for img_id in rec.images:
            if rng.random() < 0.4:
                track.add_element(pycolmap.TrackElement(img_id, 0))

    return rec


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPointcloudCovisibilityIndex:

    def test_build_index_populates_voxels(self):
        rec = _make_reconstruction()
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.2)
        assert len(index.voxel_to_image_ids) > 0
        assert len(index.voxel_to_point3d_ids) > 0

    def test_every_point_in_exactly_one_voxel(self):
        rec = _make_reconstruction(n_points=50)
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.2)
        # Flatten all point IDs from voxel_to_point3d_ids
        all_point_ids = []
        for pids in index.voxel_to_point3d_ids.values():
            all_point_ids.extend(pids)
        assert len(all_point_ids) == len(rec.points3D)
        assert set(all_point_ids) == set(rec.points3D.keys())

    def test_find_covisible_images_returns_correct_count(self):
        rec = _make_reconstruction(n_images=10, n_points=500)
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.2)

        cam = _make_camera()
        # Query from origin, looking at +Z — should see the point cloud
        query_pose = pycolmap.Rigid3d(
            pycolmap.Rotation3d(np.eye(3)),
            np.array([0.0, 0.0, 0.0]),
        )

        results = index.find_covisible_images(query_pose, cam, num_images=5)
        assert len(results) > 0
        assert len(results) <= 5
        # Results are (image_id, score) tuples
        for img_id, score in results:
            assert img_id in rec.images
            assert score >= 0

    def test_find_covisible_images_sorted_descending(self):
        rec = _make_reconstruction(n_images=10, n_points=500)
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.2)

        cam = _make_camera()
        query_pose = pycolmap.Rigid3d(
            pycolmap.Rotation3d(np.eye(3)),
            np.array([0.0, 0.0, 0.0]),
        )

        results = index.find_covisible_images(query_pose, cam, num_images=10)
        scores = [s for _, s in results]
        assert scores == sorted(scores, reverse=True)

    def test_no_visibility_triggers_fallback(self):
        rec = _make_reconstruction()
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.2)

        cam = _make_camera()
        # Camera far away, looking in -Z (away from the point cloud at z~3)
        R_flip = np.diag([1.0, -1.0, -1.0])
        query_pose = pycolmap.Rigid3d(
            pycolmap.Rotation3d(R_flip),
            np.array([0.0, 0.0, 100.0]),
        )

        results = index.find_covisible_images(query_pose, cam, num_images=3)
        # Should still return results via position fallback
        assert len(results) > 0

    def test_query_visible_voxels_empty_for_opposite_direction(self):
        rec = _make_reconstruction()
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.2)

        cam = _make_camera()
        # Camera at origin looking in -Z (points are at +Z)
        R_flip = np.diag([1.0, -1.0, -1.0])
        query_pose = pycolmap.Rigid3d(
            pycolmap.Rotation3d(R_flip),
            np.array([0.0, 0.0, 0.0]),
        )

        voxels = index.query_visible_voxels(query_pose, cam, max_depth=10.0)
        # Should find very few or no occupied voxels when looking away
        # (some may be hit by peripheral rays, but the bulk should be missed)
        assert len(voxels) < 5  # generous threshold

    def test_get_point3d_ids_in_voxels(self):
        rec = _make_reconstruction(n_points=100)
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.5)  # large voxels

        occupied = list(index.voxel_to_point3d_ids.keys())[:3]
        point_ids = index.get_point3d_ids_in_voxels(occupied)
        assert len(point_ids) > 0
        for pid in point_ids:
            assert pid in rec.points3D

    def test_different_voxel_sizes(self):
        rec = _make_reconstruction(n_points=200)

        idx_small = PointcloudCovisibilityIndex(rec, voxel_size=0.1)
        idx_large = PointcloudCovisibilityIndex(rec, voxel_size=1.0)

        # Smaller voxels = more voxels
        assert len(idx_small.voxel_to_image_ids) >= len(idx_large.voxel_to_image_ids)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
