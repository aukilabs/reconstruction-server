"""E1 subtractive carve + multi-view voxel gate → masked depth folder."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import numpy as np

from colmap_monodepth.depth_io import load_depth_png, save_depth_png, depth_png_path
from colmap_monodepth.types import (
    CarveConfig,
    CarveResult,
    FrameSet,
    camera_center_from_w2c,
    frameset_from_colmap,
    scale_intrinsics_for_depth,
)
from colmap_monodepth.voxel_grid import VoxelGrid, compute_empty_voxel_indices


def _unproject_strided(
    depth: np.ndarray,
    K: np.ndarray,
    w2c: np.ndarray,
    stride: int,
    max_depth: float,
    min_depth: float,
) -> np.ndarray:
    """Back-project valid depth samples to world points on a strided grid."""
    h, w = depth.shape
    us = np.arange(0, w, stride)
    vs = np.arange(0, h, stride)
    uu, vv = np.meshgrid(us, vs)
    z = depth[vv, uu].astype(np.float64)
    valid = np.isfinite(z) & (z > min_depth) & (z < max_depth)
    if valid.sum() < 20:
        return np.zeros((0, 3), dtype=np.float64)
    uu, vv, z = uu[valid].astype(np.float64), vv[valid].astype(np.float64), z[valid]
    Xc = np.linalg.inv(K) @ np.stack([uu * z, vv * z, z], axis=0)
    R, t = w2c[:3, :3], w2c[:3, 3]
    return (R.T @ (Xc - t.reshape(3, 1))).T


def build_support_grid(
    frames: FrameSet,
    depth_dir: str | Path,
    config: CarveConfig,
) -> tuple[VoxelGrid, set[tuple[int, int, int]], dict]:
    """Accumulate multi-view voxel support and return kept voxel keys."""
    depth_dir = Path(depth_dir)
    grid = VoxelGrid(voxel_size=config.voxel_size)
    t0 = time.perf_counter()

    for idx, name in enumerate(frames.image_names):
        K = frames.intrinsics[idx]
        w2c = frames.extrinsics_w2c[idx]
        depth = load_depth_png(depth_png_path(depth_dir, name))
        K_use = scale_intrinsics_for_depth(
            K,
            int(frames.widths[idx]),
            int(frames.heights[idx]),
            depth.shape[1],
            depth.shape[0],
        )

        pts = _unproject_strided(
            depth,
            K_use,
            w2c,
            config.unproject_stride,
            config.max_depth,
            config.min_depth,
        )
        if pts.size == 0:
            continue

        frame_grid = VoxelGrid(voxel_size=config.voxel_size)
        frame_grid.add_points_batch(pts)
        fpts = []
        for cell in frame_grid.cells.values():
            if cell.point_count < 1:
                continue
            pt, _, _ = cell.get_means()
            if pt is not None:
                fpts.append(pt)
        if not fpts:
            continue
        fpts = np.asarray(fpts, dtype=np.float64)
        grid.add_points_batch(fpts)

        cam = camera_center_from_w2c(w2c)
        ray_dirs = fpts - cam
        ray_lengths = np.linalg.norm(ray_dirs, axis=1)
        valid = ray_lengths > config.voxel_size
        dirs = np.zeros_like(ray_dirs)
        dirs[valid] = ray_dirs[valid] / ray_lengths[valid, None]
        empty = compute_empty_voxel_indices(
            cam,
            dirs,
            ray_lengths,
            valid,
            config.voxel_size,
            config.max_depth,
        )
        if len(empty):
            grid.add_empty_batch(empty, config.subtractive_value)

    kept: set[tuple[int, int, int]] = set()
    n_occ = 0
    for key, cell in grid.cells.items():
        if cell.point_count <= 0:
            continue
        n_occ += 1
        if cell.point_count >= config.min_points and cell.confidence >= config.min_confidence:
            kept.add(key)

    meta = {
        "voxel_size": config.voxel_size,
        "subtractive_value": config.subtractive_value,
        "min_points": config.min_points,
        "min_confidence": config.min_confidence,
        "max_depth": config.max_depth,
        "n_occupied_cells": n_occ,
        "n_kept_cells": len(kept),
        "build_seconds": time.perf_counter() - t0,
        "source_depth": str(depth_dir),
    }
    return grid, kept, meta


def mask_depths_by_voxels(
    frames: FrameSet,
    kept: set[tuple[int, int, int]],
    src_depth_dir: str | Path,
    out_depth_dir: str | Path,
    config: CarveConfig,
) -> dict:
    """Mask depth maps, keeping only samples whose voxels passed the gate."""
    import cv2

    src_depth_dir = Path(src_depth_dir)
    out_depth_dir = Path(out_depth_dir)
    out_depth_dir.mkdir(parents=True, exist_ok=True)
    stats: dict = {"n_frames": frames.num_frames, "frac_kept": []}

    for idx, name in enumerate(frames.image_names):
        depth = load_depth_png(depth_png_path(src_depth_dir, name))
        K = frames.intrinsics[idx]
        w2c = frames.extrinsics_w2c[idx]
        K_use = scale_intrinsics_for_depth(
            K,
            int(frames.widths[idx]),
            int(frames.heights[idx]),
            depth.shape[1],
            depth.shape[0],
        )

        h, w = depth.shape
        stride = config.mask_stride
        us = np.arange(0, w, stride)
        vs = np.arange(0, h, stride)
        uu, vv = np.meshgrid(us, vs)
        z = depth[vv, uu].astype(np.float64)
        valid = np.isfinite(z) & (z > config.min_depth) & (z < config.max_depth)
        keep_mask = np.zeros_like(depth, dtype=bool)

        if valid.sum() > 0:
            u = uu[valid].astype(np.float64)
            v = vv[valid].astype(np.float64)
            zz = z[valid]
            Xc = np.linalg.inv(K_use) @ np.stack([u * zz, v * zz, zz], axis=0)
            R, t = w2c[:3, :3], w2c[:3, 3]
            Xw = (R.T @ (Xc - t.reshape(3, 1))).T
            keys = np.round(Xw / config.voxel_size).astype(np.int32)
            ok = np.array([tuple(k) in kept for k in keys], dtype=bool)
            yi = vv[valid][ok]
            xi = uu[valid][ok]
            keep_mask[yi, xi] = True
            keep_mask = (
                cv2.dilate(keep_mask.astype(np.uint8), np.ones((3, 3), np.uint8), iterations=1).astype(bool)
            )

        base_ok = np.isfinite(depth) & (depth > config.min_depth) & (depth < config.mask_max_depth)
        keep_mask &= base_ok
        masked = depth.copy()
        masked[~keep_mask] = 0.0
        frac = float(keep_mask.sum() / max(1, base_ok.sum()))
        stats["frac_kept"].append(frac)
        save_depth_png(masked, depth_png_path(out_depth_dir, name))

    stats["frac_kept_mean"] = float(np.mean(stats["frac_kept"])) if stats["frac_kept"] else 0.0
    stats["frac_kept_min"] = float(np.min(stats["frac_kept"])) if stats["frac_kept"] else 0.0
    return stats


def run_carve(
    depth_dir: str | Path,
    output_dir: str | Path,
    *,
    frames: Optional[FrameSet] = None,
    colmap_dir: Optional[str | Path] = None,
    image_names: Optional[list[str]] = None,
    config: Optional[CarveConfig] = None,
) -> CarveResult:
    """Run E1 carve: build support grid, gate voxels, write masked depths.

    Provide either ``frames`` or ``colmap_dir`` (with optional ``image_names`` filter).
    """
    if frames is None:
        if colmap_dir is None:
            raise ValueError("run_carve requires frames or colmap_dir")
        frames = frameset_from_colmap(colmap_dir, image_names=image_names)
    elif image_names is not None:
        name_to_idx = frames.name_to_index()
        indices = [name_to_idx[n] for n in image_names]
        frames = FrameSet(
            image_names=list(image_names),
            intrinsics=frames.intrinsics[indices],
            extrinsics_w2c=frames.extrinsics_w2c[indices],
            widths=frames.widths[indices],
            heights=frames.heights[indices],
        )

    config = config or CarveConfig()
    depth_dir = Path(depth_dir)
    output_dir = Path(output_dir)

    _grid, kept, carve_meta = build_support_grid(frames, depth_dir, config)
    mask_stats = mask_depths_by_voxels(frames, kept, depth_dir, output_dir, config)
    meta = {**carve_meta, "mask_stats": mask_stats}

    return CarveResult(
        output_depth_dir=output_dir,
        meta=meta,
        kept_voxel_keys=kept,
    )
