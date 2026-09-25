"""Export per-image depth PNGs and a colored point-cloud PLY from DA3 predictions."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Sequence, Tuple

import numpy as np

from colmap_monodepth.depth_io import depth_to_uint16_mm


def _as_homogeneous44(ext: np.ndarray) -> np.ndarray:
    if ext.shape == (4, 4):
        return ext
    if ext.shape == (3, 4):
        H = np.eye(4, dtype=np.float64)
        H[:3, :4] = ext
        return H
    raise ValueError(f"extrinsic must be (4,4) or (3,4), got {ext.shape}")


def write_depth_pngs(
    depth: np.ndarray,
    output_dir: str | Path,
    names: Optional[Sequence[str]] = None,
) -> list[Path]:
    """Write one 16-bit depth PNG per view (millimeters)."""
    try:
        from PIL import Image
    except ImportError as exc:  # pragma: no cover
        raise ImportError("Pillow is required to write depth PNGs") from exc

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    n = depth.shape[0]
    for i in range(n):
        if names is not None:
            stem = Path(names[i]).stem
        else:
            stem = f"{i:06d}"
        path = output_dir / f"{stem}_depth.png"
        Image.fromarray(depth_to_uint16_mm(depth[i]), mode="I;16").save(path)
        paths.append(path)
    return paths


def depths_to_world_points(
    depth: np.ndarray,
    intrinsics: np.ndarray,
    extrinsics_w2c: np.ndarray,
    images_u8: Optional[np.ndarray] = None,
    conf: Optional[np.ndarray] = None,
    conf_percentile: float = 40.0,
    max_points: int = 1_000_000,
) -> Tuple[np.ndarray, np.ndarray]:
    """Back-project depth maps to world points (same geometry as DA3 glb export)."""
    n, h, w = depth.shape
    us, vs = np.meshgrid(np.arange(w), np.arange(h))
    ones = np.ones_like(us)
    pix = np.stack([us, vs, ones], axis=-1).reshape(-1, 3)

    conf_thr = None
    if conf is not None:
        conf_thr = float(np.percentile(conf, conf_percentile))

    pts_all = []
    col_all = []
    for i in range(n):
        d = depth[i]
        valid = np.isfinite(d) & (d > 0)
        if conf is not None and conf_thr is not None:
            valid &= conf[i] >= conf_thr
        if not np.any(valid):
            continue
        d_flat = d.reshape(-1)
        vidx = np.flatnonzero(valid.reshape(-1))
        K_inv = np.linalg.inv(intrinsics[i].astype(np.float64))
        c2w = np.linalg.inv(_as_homogeneous44(extrinsics_w2c[i].astype(np.float64)))
        rays = K_inv @ pix[vidx].T
        Xc = rays * d_flat[vidx][None, :]
        Xc_h = np.vstack([Xc, np.ones((1, Xc.shape[1]))])
        Xw = (c2w @ Xc_h)[:3].T.astype(np.float32)
        if images_u8 is not None:
            cols = images_u8[i].reshape(-1, 3)[vidx].astype(np.uint8)
        else:
            cols = np.full((Xw.shape[0], 3), 200, dtype=np.uint8)
        pts_all.append(Xw)
        col_all.append(cols)

    if not pts_all:
        return np.zeros((0, 3), dtype=np.float32), np.zeros((0, 3), dtype=np.uint8)

    points = np.concatenate(pts_all, 0)
    colors = np.concatenate(col_all, 0)
    finite = np.isfinite(points).all(axis=1)
    points, colors = points[finite], colors[finite]
    if points.shape[0] > max_points:
        idx = np.random.default_rng(0).choice(points.shape[0], max_points, replace=False)
        points, colors = points[idx], colors[idx]
    return points, colors


def write_ply(path: str | Path, points: np.ndarray, colors: np.ndarray) -> Path:
    """Write a binary_little_endian colored point cloud PLY."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    n = points.shape[0]
    header = (
        "ply\n"
        "format binary_little_endian 1.0\n"
        f"element vertex {n}\n"
        "property float x\nproperty float y\nproperty float z\n"
        "property uchar red\nproperty uchar green\nproperty uchar blue\n"
        "end_header\n"
    )
    verts = np.empty(
        n,
        dtype=[
            ("x", "<f4"),
            ("y", "<f4"),
            ("z", "<f4"),
            ("red", "u1"),
            ("green", "u1"),
            ("blue", "u1"),
        ],
    )
    pts = np.asarray(points, dtype=np.float32).reshape(-1, 3)
    cols = np.asarray(colors, dtype=np.uint8).reshape(-1, 3)
    verts["x"], verts["y"], verts["z"] = pts[:, 0], pts[:, 1], pts[:, 2]
    verts["red"], verts["green"], verts["blue"] = cols[:, 0], cols[:, 1], cols[:, 2]
    with open(path, "wb") as f:
        f.write(header.encode("ascii"))
        f.write(verts.tobytes())
    return path


def export_prediction(
    prediction: Any,
    output_dir: str | Path,
    image_paths: Optional[Sequence[str]] = None,
    *,
    conf_percentile: float = 40.0,
    max_points: int = 1_000_000,
) -> dict:
    """Write depth/ PNGs and pointcloud.ply from a DA3 Prediction object."""
    output_dir = Path(output_dir)
    depth_dir = output_dir / "depth"
    depth = np.asarray(prediction.depth)
    depth_paths = write_depth_pngs(depth, depth_dir, names=image_paths)

    intrinsics = np.asarray(prediction.intrinsics)
    extrinsics = np.asarray(prediction.extrinsics)
    images_u8 = getattr(prediction, "processed_images", None)
    conf = getattr(prediction, "conf", None)
    if conf is not None:
        conf = np.asarray(conf)

    points, colors = depths_to_world_points(
        depth,
        intrinsics,
        extrinsics,
        images_u8=images_u8,
        conf=conf,
        conf_percentile=conf_percentile,
        max_points=max_points,
    )
    ply_path = write_ply(output_dir / "pointcloud.ply", points, colors)
    return {
        "depth_dir": depth_dir,
        "depth_paths": depth_paths,
        "ply_path": ply_path,
        "num_points": int(points.shape[0]),
    }
