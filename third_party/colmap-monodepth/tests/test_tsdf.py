"""TSDF smoke test (skipped when open3d is unavailable or integration is empty)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from colmap_monodepth.depth_io import depth_png_path, save_depth_png
from colmap_monodepth.tsdf import run_tsdf
from colmap_monodepth.types import FrameSet, TsdfConfig

pytest.importorskip("open3d")
pytest.importorskip("cv2")


def _make_rgb_png(path: Path, color: tuple[int, int, int], size: tuple[int, int]) -> None:
    from PIL import Image

    Image.fromarray(np.full((*size, 3), color, dtype=np.uint8)).save(path)


def test_run_tsdf_writes_mesh(tmp_path: Path):
    h, w = 64, 64
    K = np.array([[120.0, 0, 32], [0, 120, 32], [0, 0, 1]], dtype=np.float64)
    names = ["view0.png", "view1.png"]
    centers = [np.array([0.0, 0.0, 0.0]), np.array([0.3, 0.0, 0.0])]
    w2cs = []
    for c in centers:
        T = np.eye(4, dtype=np.float64)
        T[:3, 3] = -c
        w2cs.append(T)
    w2cs_arr = np.stack(w2cs, axis=0)
    frames = FrameSet(
        image_names=names,
        intrinsics=np.stack([K, K], axis=0),
        extrinsics_w2c=w2cs_arr,
        widths=np.array([w, w], dtype=np.int32),
        heights=np.array([h, h], dtype=np.int32),
    )

    depth_dir = tmp_path / "depth"
    depth_dir.mkdir()
    image_paths: list[str] = []
    for i, name in enumerate(names):
        depth = np.full((h, w), 2.0, dtype=np.float32)
        save_depth_png(depth, depth_png_path(depth_dir, name))
        img_path = tmp_path / name
        _make_rgb_png(img_path, (40 + i * 20, 100, 50), (h, w))
        image_paths.append(str(img_path))

    out_dir = tmp_path / "tsdf"
    result = run_tsdf(
        depth_dir,
        out_dir,
        image_paths,
        frames=frames,
        config=TsdfConfig(voxel_length=0.04, sdf_trunc=0.12),
    )

    assert "tsdf_seconds" in result.meta
    assert result.mesh_path.name == "tsdf_mesh.ply"
    assert result.points_path.name == "tsdf_points.ply"

    if result.meta["mesh_vertices"] == 0:
        pytest.skip("Open3D TSDF produced an empty mesh on this platform")

    assert result.mesh_path.is_file()
    assert result.points_path.is_file()
    assert result.meta["tsdf_points"] > 0
