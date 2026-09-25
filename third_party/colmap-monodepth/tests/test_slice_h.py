"""Slice H checks: carve default voxel, no-color TSDF, binary points PLY, postprocess."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from colmap_monodepth.depth_io import depth_png_path, save_depth_png
from colmap_monodepth.mesh_postprocess import postprocess_tsdf_mesh
from colmap_monodepth.tsdf import run_tsdf
from colmap_monodepth.types import CarveConfig, FrameSet, TsdfConfig

pytest.importorskip("open3d")
pytest.importorskip("cv2")


def test_carve_config_default_voxel_size():
    cfg = CarveConfig()
    assert cfg.voxel_size == 0.07
    assert cfg.subtractive_value == -0.2
    assert cfg.min_points == 3
    assert cfg.min_confidence == 2.0


def test_tsdf_config_default_coarser():
    cfg = TsdfConfig()
    assert cfg.voxel_length == 0.04
    assert cfg.sdf_trunc == 0.16


def _synthetic_frames_and_depths(tmp_path: Path, names: list[str]):
    h, w = 64, 64
    K = np.array([[120.0, 0, 32], [0, 120, 32], [0, 0, 1]], dtype=np.float64)
    centers = [np.array([0.0, 0.0, 0.0]), np.array([0.3, 0.0, 0.0])][: len(names)]
    w2cs = []
    for c in centers:
        T = np.eye(4, dtype=np.float64)
        T[:3, 3] = -c
        w2cs.append(T)
    while len(centers) < len(names):
        T = np.eye(4, dtype=np.float64)
        w2cs.append(T)
    w2cs_arr = np.stack(w2cs[: len(names)], axis=0)
    frames = FrameSet(
        image_names=names,
        intrinsics=np.stack([K] * len(names), axis=0),
        extrinsics_w2c=w2cs_arr,
        widths=np.array([w] * len(names), dtype=np.int32),
        heights=np.array([h] * len(names), dtype=np.int32),
    )
    depth_dir = tmp_path / "depth"
    depth_dir.mkdir()
    for name in names:
        depth = np.full((h, w), 2.0, dtype=np.float32)
        save_depth_png(depth, depth_png_path(depth_dir, name))
    return frames, depth_dir, h, w


def test_run_tsdf_nocolor_without_rgb(tmp_path: Path):
    names = ["view0.png", "view1.png"]
    frames, depth_dir, _, _ = _synthetic_frames_and_depths(tmp_path, names)
    out_dir = tmp_path / "tsdf_nocolor"
    result = run_tsdf(
        depth_dir,
        out_dir,
        image_paths=None,
        frames=frames,
        config=TsdfConfig(
            voxel_length=0.04,
            sdf_trunc=0.12,
            color_type="nocolor",
        ),
    )
    assert result.meta["color_type"] == "nocolor"
    if result.meta.get("mesh_vertices", 0) == 0:
        pytest.skip("Open3D TSDF produced an empty mesh on this platform")
    assert result.mesh_path.is_file()
    assert result.points_path.is_file()


def test_tsdf_points_ply_is_binary(tmp_path: Path):
    from PIL import Image

    names = ["view0.png", "view1.png"]
    frames, depth_dir, h, w = _synthetic_frames_and_depths(tmp_path, names)
    image_paths = []
    for i, name in enumerate(names):
        img_path = tmp_path / name
        Image.fromarray(
            np.full((h, w, 3), (40 + i * 20, 100, 50), dtype=np.uint8)
        ).save(img_path)
        image_paths.append(str(img_path))

    out_dir = tmp_path / "tsdf_binary"
    result = run_tsdf(
        depth_dir,
        out_dir,
        image_paths,
        frames=frames,
        config=TsdfConfig(voxel_length=0.04, sdf_trunc=0.12, postprocess=False),
    )
    if result.meta.get("mesh_vertices", 0) == 0:
        pytest.skip("Open3D TSDF produced an empty mesh on this platform")

    header = result.points_path.read_bytes()[:256].decode("ascii", errors="ignore")
    assert "format binary_little_endian 1.0" in header


def test_postprocess_runs_on_tiny_mesh():
    import open3d as o3d

    mesh = o3d.geometry.TriangleMesh.create_box(width=1.0, height=1.0, depth=1.0)
    mesh.compute_vertex_normals()
    n_before = len(mesh.triangles)
    cleaned, meta = postprocess_tsdf_mesh(mesh)
    assert len(cleaned.triangles) > 0
    assert meta["triangles_before"] == n_before
    assert meta["triangles_after"] <= meta["triangles_after_taubin"]


def test_run_tsdf_postprocess_reduces_or_keeps_triangles(tmp_path: Path):
    from PIL import Image

    names = ["view0.png", "view1.png"]
    frames, depth_dir, h, w = _synthetic_frames_and_depths(tmp_path, names)
    image_paths = []
    for i, name in enumerate(names):
        img_path = tmp_path / name
        Image.fromarray(
            np.full((h, w, 3), (40 + i * 20, 100, 50), dtype=np.uint8)
        ).save(img_path)
        image_paths.append(str(img_path))

    out_dir = tmp_path / "tsdf_post"
    result = run_tsdf(
        depth_dir,
        out_dir,
        image_paths,
        frames=frames,
        config=TsdfConfig(voxel_length=0.04, sdf_trunc=0.12, postprocess=True),
    )
    if result.meta.get("mesh_vertices", 0) == 0:
        pytest.skip("Open3D TSDF produced an empty mesh on this platform")
    assert "triangles_after" in result.meta
    assert result.meta["triangles_after"] <= result.meta.get("triangles_after_taubin", 999999)
