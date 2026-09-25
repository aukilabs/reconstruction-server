"""Joint depth fit tests (CPU torch; no GPU / DA3)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from colmap_monodepth.colmap_io import Camera, Image, Point3D, write_model
from colmap_monodepth.depth_io import depth_png_path, load_depth_png, save_depth_png
from colmap_monodepth.fit import run_fit
from colmap_monodepth.fit_geometry import build_geo_pairs, fit_scale_np
from colmap_monodepth.types import FitConfig

pytest.importorskip("torch")
pytest.importorskip("cv2")


def _pinhole_K(fx: float = 80.0, cx: float = 32.0, cy: float = 32.0) -> np.ndarray:
    return np.array([[fx, 0.0, cx], [0.0, fx, cy], [0.0, 0.0, 1.0]], dtype=np.float64)


def _make_fit_colmap_scene(tmp: Path, n_images: int = 2) -> tuple[Path, list[str], float]:
    """Two frontal cameras with many shared triangulated points at z≈2 m."""
    images_dir = tmp / "images"
    sparse_dir = tmp / "sparse" / "0"
    images_dir.mkdir(parents=True)
    sparse_dir.mkdir(parents=True)

    names = [f"view_{i:02d}.png" for i in range(n_images)]
    for name in names:
        (images_dir / name).write_bytes(b"\x89PNG\r\n\x1a\n")

    w, h = 64, 64
    fx, fy, cx, cy = 80.0, 80.0, 32.0, 32.0
    cameras = {
        1: Camera(id=1, model="PINHOLE", width=w, height=h, params=np.array([fx, fy, cx, cy]))
    }

    images: dict[int, Image] = {}
    points3d: dict[int, Point3D] = {}
    pid = 1
    grid_us = np.linspace(12, 52, 8)
    grid_vs = np.linspace(12, 52, 5)
    true_z = 2.0

    for i in range(n_images):
        qvec = np.array([1.0, 0.0, 0.0, 0.0])
        tvec = np.array([0.0, 0.0, float(i) * 0.3])
        xys = []
        pids = []
        for v in grid_vs:
            for u in grid_us:
                Xw = np.array([(u - cx) * true_z / fx, (v - cy) * true_z / fy, true_z])
                points3d[pid] = Point3D(
                    id=pid,
                    xyz=Xw,
                    rgb=np.array([128, 128, 128]),
                    error=0.01,
                    image_ids=np.array([i + 1]),
                    point2D_idxs=np.array([len(xys)]),
                )
                xys.append([float(u), float(v)])
                pids.append(pid)
                pid += 1
        images[i + 1] = Image(
            id=i + 1,
            qvec=qvec,
            tvec=tvec,
            camera_id=1,
            name=names[i],
            xys=np.array(xys),
            point3D_ids=np.array(pids, dtype=np.int64),
        )

    # Cross-link tracks so covis / track losses have shared observations
    for pt_id in list(points3d.keys())[:20]:
        pt = points3d[pt_id]
        if n_images < 2:
            break
        pt = pt._replace(
            image_ids=np.array([1, 2]),
            point2D_idxs=np.array([0, 0]),
        )
        u = float(pt.xyz[0] * fx / true_z + cx)
        v = float(pt.xyz[1] * fy / true_z + cy)
        for im_id in (1, 2):
            im = images[im_id]
            new_xys = np.vstack([im.xys, [u, v]])
            new_pids = np.append(im.point3D_ids, pt_id)
            images[im_id] = im._replace(xys=new_xys, point3D_ids=new_pids)

    write_model(cameras, images, points3d, str(sparse_dir), ext=".txt")
    return tmp, names, true_z


def test_fit_scale_np_recovers_ratio():
    pred = np.linspace(1.0, 3.0, 100)
    # Slightly varying scale so trimmed quantiles do not collapse to an empty band
    gt = pred * (1.08 + 0.04 * np.linspace(0, 1, 100))
    scale = fit_scale_np(pred, gt, min_n=10)
    assert np.isfinite(scale)
    assert 1.05 < scale < 1.15


def test_run_fit_writes_outputs_and_adjusts_scale(tmp_path: Path):
    colmap_dir, names, true_z = _make_fit_colmap_scene(tmp_path)
    depth_dir = tmp_path / "depth_in"
    depth_dir.mkdir()
    scale_error = 0.88
    for name in names:
        d = np.full((64, 64), true_z * scale_error, dtype=np.float32)
        save_depth_png(d, depth_png_path(depth_dir, name))

    out_dir = tmp_path / "fit_out"
    config = FitConfig(
        mode="affine",
        steps_affine=30,
        steps_res=0,
        covis_min_shared=2,
        device="cpu",
    )
    result = run_fit(colmap_dir, depth_dir, out_dir, config=config)

    assert result.fitted_depth_dir.is_dir()
    assert result.confident_depth_dir.is_dir()
    assert result.params_path.is_file()
    assert (out_dir / "meta.json").is_file()

    fitted = load_depth_png(depth_png_path(result.fitted_depth_dir, names[0]))
    valid = fitted > 0
    assert valid.any()
    median_fitted = float(np.median(fitted[valid]))
    assert median_fitted > true_z * scale_error * 1.02


def test_run_fit_residual_icp_smoke(tmp_path: Path):
    colmap_dir, names, true_z = _make_fit_colmap_scene(tmp_path)
    depth_dir = tmp_path / "depth_in"
    depth_dir.mkdir()
    for name in names:
        d = np.full((64, 64), true_z * 0.92, dtype=np.float32)
        save_depth_png(d, depth_png_path(depth_dir, name))

    out_dir = tmp_path / "fit_icp"
    config = FitConfig(
        mode="residual_icp",
        steps_affine=8,
        steps_res=12,
        covis_min_shared=2,
        max_geo_pairs=8,
        device="cpu",
    )
    result = run_fit(colmap_dir, depth_dir, out_dir, config=config)
    assert result.meta["mode"] == "residual_icp"
    assert result.meta.get("rematch") is not None
    assert result.meta.get("residual_grid") == [config.res_gh, config.res_gw]
    assert result.meta["pair_stats"]["cost_mode"] == "temporal"
    assert result.meta.get("geo_stride") == config.geo_stride
    assert result.meta.get("geo_uv_samples", 0) > 0
    conf = load_depth_png(depth_png_path(result.confident_depth_dir, names[0]))
    assert conf.shape == (64, 64)


def test_build_geo_pairs_cost_temporal_only_reports_full_union():
    n = 6
    # Cameras along x so spatial neighbors exist beyond temporal radius.
    centers = np.stack([np.arange(n, dtype=np.float64), np.zeros(n), np.zeros(n)], axis=1)
    # Dense covis between non-adjacent views (0,3), (1,4), (2,5).
    obs: dict[int, list[tuple[int, float, float]]] = {}
    for pid, (a, b) in enumerate([(0, 3), (1, 4), (2, 5)]):
        obs[pid] = [(a, 10.0, 10.0), (b, 12.0, 12.0)] * 30

    cfg = FitConfig(
        temporal_radius=1,
        covis_min_shared=2,
        covis_max_per_view=4,
        geo_max_pairs_per_view=3,
        geo_max_baseline_m=10.0,
        max_geo_pairs=20,
        geo_cost_temporal_only=True,
    )
    cost_pairs, stats = build_geo_pairs(n, centers, obs, cfg)
    temporal_expected = {(i, i + 1) for i in range(n - 1)}
    assert set(cost_pairs) == temporal_expected
    assert stats["cost_mode"] == "temporal"
    assert stats["cost"] == len(temporal_expected)
    assert stats["temporal"] == len(temporal_expected)
    assert stats["covis"] >= 1
    assert stats["union"] >= stats["cost"]

    cfg_union = FitConfig(
        temporal_radius=1,
        covis_min_shared=2,
        covis_max_per_view=4,
        geo_max_pairs_per_view=3,
        geo_max_baseline_m=10.0,
        max_geo_pairs=20,
        geo_cost_temporal_only=False,
    )
    union_pairs, union_stats = build_geo_pairs(n, centers, obs, cfg_union)
    assert union_stats["cost_mode"] == "union"
    assert set(union_pairs) == set(build_geo_pairs(n, centers, obs, cfg_union)[0])
    assert len(union_pairs) >= len(cost_pairs)
    assert any(abs(a - b) > 1 for a, b in union_pairs)
