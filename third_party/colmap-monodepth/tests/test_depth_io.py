"""depth_io round-trip tests."""

from __future__ import annotations

from pathlib import Path

import numpy as np

from colmap_monodepth.depth_io import (
    depth_png_path,
    depth_to_uint16_mm,
    load_depth_png,
    load_depth_folder,
    save_depth_folder,
    save_depth_png,
    uint16_mm_to_depth_m,
)


def test_uint16_mm_roundtrip():
    depth = np.array([[0.0, 1.25, np.nan], [3.5, 0.001, 10.0]], dtype=np.float32)
    mm = depth_to_uint16_mm(depth)
    back = uint16_mm_to_depth_m(mm)
    valid = np.isfinite(depth) & (depth > 0)
    assert np.allclose(back[valid], depth[valid], rtol=1e-3)
    assert back[~valid].sum() == 0


def test_depth_png_roundtrip(tmp_path: Path):
    depth = np.full((48, 64), 2.0, dtype=np.float32)
    depth[10:20, 30:40] = 1.5
    path = tmp_path / "view_depth.png"
    save_depth_png(depth, path)
    loaded = load_depth_png(path)
    assert loaded.shape == depth.shape
    assert np.allclose(loaded[10:20, 30:40], 1.5, rtol=1e-3)
    assert np.allclose(loaded[0, 0], 2.0, rtol=1e-3)


def test_depth_folder_helpers(tmp_path: Path):
    depths = {
        "a/frame_0001.png": np.ones((32, 32), dtype=np.float32) * 2.0,
        "b/frame_0002.jpg": np.ones((16, 24), dtype=np.float32) * 3.0,
    }
    save_depth_folder(depths, tmp_path)
    assert depth_png_path(tmp_path, "a/frame_0001.png").is_file()
    assert depth_png_path(tmp_path, "b/frame_0002.jpg").is_file()
    loaded = load_depth_folder(tmp_path, list(depths.keys()))
    assert np.allclose(loaded["a/frame_0001.png"], 2.0, rtol=1e-3)
    assert np.allclose(loaded["b/frame_0002.jpg"], 3.0, rtol=1e-3)
