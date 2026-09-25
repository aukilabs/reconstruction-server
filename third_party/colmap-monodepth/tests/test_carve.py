"""Synthetic multi-view carve tests (no GPU, no real scan)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from colmap_monodepth.carve import build_support_grid, run_carve
from colmap_monodepth.depth_io import depth_png_path, load_depth_png, save_depth_png
from colmap_monodepth.types import CarveConfig, FrameSet
from colmap_monodepth.voxel_grid import VoxelGrid


pytest.importorskip("numba")
pytest.importorskip("cv2")


def _pinhole_K(fx: float = 100.0, cx: float = 32.0, cy: float = 32.0) -> np.ndarray:
    return np.array([[fx, 0.0, cx], [0.0, fx, cy], [0.0, 0.0, 1.0]], dtype=np.float64)


def _w2c_from_center(center: np.ndarray) -> np.ndarray:
    T = np.eye(4, dtype=np.float64)
    T[:3, 3] = -center
    return T


def _world_from_pixel(
    K: np.ndarray,
    w2c: np.ndarray,
    u: int,
    v: int,
    depth_m: float,
) -> np.ndarray:
    Xc = np.linalg.inv(K) @ np.array([u * depth_m, v * depth_m, depth_m], dtype=np.float64)
    R, t = w2c[:3, :3], w2c[:3, 3]
    return (R.T @ (Xc - t)).astype(np.float64)


def _make_two_view_scene() -> tuple[FrameSet, tuple[int, int], tuple[int, int], dict[str, np.ndarray]]:
    """Two cameras share a z=2 wall; cam0 alone has a small z=1 floater patch."""
    h, w = 64, 64
    names = ["cam0.png", "cam1.png"]
    K = _pinhole_K()
    centers = [np.array([0.0, 0.0, 0.0]), np.array([0.5, 0.0, 0.0])]
    w2cs = np.stack([_w2c_from_center(c) for c in centers], axis=0)
    frames = FrameSet(
        image_names=names,
        intrinsics=np.stack([K, K], axis=0),
        extrinsics_w2c=w2cs,
        widths=np.array([w, w], dtype=np.int32),
        heights=np.array([h, h], dtype=np.int32),
    )

    floater_uv = (10, 10)
    wall_uv = (32, 32)
    depths: dict[str, np.ndarray] = {}
    for i, name in enumerate(names):
        d = np.full((h, w), 2.0, dtype=np.float32)
        if i == 0:
            fu, fv = floater_uv
            d[fv - 2 : fv + 3, fu - 2 : fu + 3] = 1.0
        depths[name] = d

    return frames, floater_uv, wall_uv, depths


def test_build_support_grid_excludes_single_view_floater(tmp_path: Path):
    frames, floater_uv, wall_uv, depths = _make_two_view_scene()
    depth_dir = tmp_path / "depth_in"
    depth_dir.mkdir()
    for name, d in depths.items():
        save_depth_png(d, depth_png_path(depth_dir, name))

    config = CarveConfig(
        voxel_size=0.05,
        subtractive_value=-0.2,
        min_points=2,
        min_confidence=1.0,
        max_depth=8.0,
        unproject_stride=2,
        mask_stride=2,
    )
    _grid, kept, meta = build_support_grid(frames, depth_dir, config)

    assert meta["n_kept_cells"] > 0

    K = frames.intrinsics[0]
    w2c = frames.extrinsics_w2c[0]
    fu, fv = floater_uv
    z_f = depths["cam0.png"][fv, fu]
    Xw_f = _world_from_pixel(K, w2c, fu, fv, float(z_f))
    floater_key = tuple(np.round(Xw_f / config.voxel_size).astype(int))
    assert floater_key not in kept

    wu, wv = wall_uv
    z_w = depths["cam0.png"][wv, wu]
    Xw_w = _world_from_pixel(K, w2c, wu, wv, float(z_w))
    wall_key = tuple(np.round(Xw_w / config.voxel_size).astype(int))
    assert wall_key in kept


def test_run_carve_masks_floater_depth(tmp_path: Path):
    frames, floater_uv, wall_uv, depths = _make_two_view_scene()
    depth_dir = tmp_path / "depth_in"
    out_dir = tmp_path / "depth_out"
    depth_dir.mkdir()
    for name, d in depths.items():
        save_depth_png(d, depth_png_path(depth_dir, name))

    config = CarveConfig(unproject_stride=2, mask_stride=2)
    result = run_carve(depth_dir, out_dir, frames=frames, config=config)

    carved0 = load_depth_png(depth_png_path(out_dir, "cam0.png"))
    fu, fv = floater_uv
    wu, wv = wall_uv
    assert carved0[fv, fu] == 0.0
    assert carved0[wv, wu] > 0.0
    assert result.meta["n_kept_cells"] > 0
    assert len(result.kept_voxel_keys) == result.meta["n_kept_cells"]


def test_free_space_voxel_not_kept_without_surface_support():
    """Voxels only hit by subtractive free-space rays should not pass the gate."""
    grid = VoxelGrid(voxel_size=0.05)
    empty = np.array([[0, 0, 4], [0, 0, 5], [0, 0, 6]], dtype=np.int32)
    grid.add_empty_batch(empty, subtractive_value=-0.2)
    kept = {
        key
        for key, cell in grid.cells.items()
        if cell.point_count >= 2 and cell.confidence >= 1.0
    }
    assert kept == set()
