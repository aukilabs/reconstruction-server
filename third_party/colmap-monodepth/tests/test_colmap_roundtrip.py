"""Synthetic COLMAP reader/writer round-trip tests."""

from __future__ import annotations

from pathlib import Path

import numpy as np

from colmap_monodepth.colmap_io import (
    Camera,
    Image,
    Point3D,
    camera_to_K,
    image_to_extrinsic,
    load_colmap_scene,
    qvec2rotmat,
    read_model,
    rotmat2qvec,
    write_model,
)


def _make_synthetic_scene(tmp: Path, n_images: int = 3):
    images_dir = tmp / "images"
    sparse_dir = tmp / "sparse" / "0"
    images_dir.mkdir(parents=True)
    sparse_dir.mkdir(parents=True)

    # Dummy RGB files so load_colmap_scene can resolve paths
    for i in range(n_images):
        (images_dir / f"frame_{i:04d}.png").write_bytes(
            b"\x89PNG\r\n\x1a\n"  # minimal stub; loader only checks existence
        )

    width, height = 640, 480
    fx, fy, cx, cy = 525.0, 525.0, 320.0, 240.0
    cameras = {
        1: Camera(id=1, model="PINHOLE", width=width, height=height, params=np.array([fx, fy, cx, cy]))
    }

    images = {}
    points3D = {}
    rng = np.random.default_rng(0)
    for i in range(n_images):
        # Random SO(3) via QR
        A = rng.normal(size=(3, 3))
        R, _ = np.linalg.qr(A)
        if np.linalg.det(R) < 0:
            R[:, 0] *= -1
        t = rng.uniform(-1, 1, size=3)
        qvec = rotmat2qvec(R)
        images[i + 1] = Image(
            id=i + 1,
            qvec=qvec,
            tvec=t,
            camera_id=1,
            name=f"frame_{i:04d}.png",
            xys=np.array([[100.0, 200.0], [300.0, 400.0]]),
            point3D_ids=np.array([-1, -1], dtype=np.int64),
        )

    points3D[1] = Point3D(
        id=1,
        xyz=np.array([0.1, 0.2, 1.5]),
        rgb=np.array([10, 20, 30]),
        error=0.01,
        image_ids=np.array([1]),
        point2D_idxs=np.array([0]),
    )

    write_model(cameras, images, points3D, str(sparse_dir), ext=".txt")
    return cameras, images, points3D, tmp


def test_qvec_rotmat_roundtrip():
    R = qvec2rotmat(np.array([1.0, 0.0, 0.0, 0.0]))
    assert np.allclose(R, np.eye(3))
    q = rotmat2qvec(R)
    assert np.allclose(qvec2rotmat(q), R, atol=1e-6)


def test_colmap_text_model_roundtrip(tmp_path: Path):
    cameras, images, points3D, root = _make_synthetic_scene(tmp_path)
    sparse = root / "sparse" / "0"

    cam2, img2, pts2 = read_model(str(sparse), ext=".txt")

    assert set(cam2.keys()) == set(cameras.keys())
    assert set(img2.keys()) == set(images.keys())
    assert set(pts2.keys()) == set(points3D.keys())

    c0, c1 = cameras[1], cam2[1]
    assert c0.model == c1.model
    assert c0.width == c1.width and c0.height == c1.height
    assert np.allclose(c0.params, c1.params)

    for iid, img in images.items():
        got = img2[iid]
        assert got.name == img.name
        assert got.camera_id == img.camera_id
        assert np.allclose(got.qvec, img.qvec)
        assert np.allclose(got.tvec, img.tvec)
        assert np.allclose(got.xys, img.xys)
        assert np.array_equal(got.point3D_ids, img.point3D_ids)

    assert np.allclose(pts2[1].xyz, points3D[1].xyz)
    assert np.array_equal(pts2[1].rgb, points3D[1].rgb)


def test_load_colmap_scene_extrinsics_intrinsics(tmp_path: Path):
    cameras, images, _pts, root = _make_synthetic_scene(tmp_path, n_images=2)
    paths, extrinsics, intrinsics = load_colmap_scene(str(root), sparse_subdir="0")

    assert len(paths) == 2
    assert extrinsics.shape == (2, 4, 4)
    assert intrinsics.shape == (2, 3, 3)

    K_expected = camera_to_K(cameras[1])
    assert np.allclose(intrinsics[0], K_expected)
    assert np.allclose(intrinsics[1], K_expected)

    # Sorted by image id
    for i, iid in enumerate(sorted(images.keys())):
        E = image_to_extrinsic(images[iid])
        assert np.allclose(extrinsics[i], E)
        assert paths[i].endswith(images[iid].name)


def test_load_colmap_scene_model_in_root(tmp_path: Path):
    """Model files directly under colmap_dir (no sparse/ subfolder)."""
    import shutil

    _cameras, _images, _pts, root = _make_synthetic_scene(tmp_path, n_images=2)
    sparse = root / "sparse" / "0"
    for name in ("cameras.txt", "images.txt", "points3D.txt"):
        (root / name).write_text((sparse / name).read_text(encoding="utf-8"), encoding="utf-8")
    shutil.rmtree(root / "sparse")

    paths, extrinsics, intrinsics = load_colmap_scene(str(root))
    assert len(paths) == 2
    assert extrinsics.shape == (2, 4, 4)
    assert intrinsics.shape == (2, 3, 3)
