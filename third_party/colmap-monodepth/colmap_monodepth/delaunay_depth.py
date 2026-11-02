"""Screen-space Delaunay depth from COLMAP sparse anchors (DA3-free infer)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

import numba
import numpy as np
from scipy.spatial import Delaunay

from colmap_monodepth.colmap_io import (
    Point3D,
    camera_to_K,
    read_model,
    resolve_sparse_dir,
)
from colmap_monodepth.colmap_io import Image as ColmapImage
from colmap_monodepth.depth_io import save_depth_folder


@dataclass
class DelaunayConfig:
    """Locked mild defaults from the grocery spike (lean on TSDF for cleanup)."""

    min_track: int = 2
    max_error_q: float = 0.98
    max_rel_dz: float = 0.35
    edge_frac: float = 0.15
    scale: float = 0.5
    # Open3D laplacian on camera-space mesh Z; iters=0 disables.
    o3d_laplacian_iters: int = 2
    o3d_laplacian_lambda: float = 0.5
    max_images: int = 0
    stride: int = 1


def collect_anchors(
    image: ColmapImage, points3d: dict[int, Point3D]
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    R = image.qvec2rotmat()
    t = image.tvec
    xys, zs, errs, tracks = [], [], [], []
    for xy, pid in zip(image.xys, image.point3D_ids):
        pid = int(pid)
        if pid < 0 or pid not in points3d:
            continue
        p = points3d[pid]
        Xc = R @ p.xyz + t
        z = float(Xc[2])
        if not np.isfinite(z) or z <= 1e-4:
            continue
        xys.append(np.asarray(xy, dtype=np.float64))
        zs.append(z)
        errs.append(float(p.error))
        tracks.append(int(len(p.image_ids)))
    if not xys:
        return (
            np.zeros((0, 2)),
            np.zeros((0,), dtype=np.float32),
            np.zeros((0,), dtype=np.float32),
            np.zeros((0,), dtype=np.int32),
        )
    return (
        np.stack(xys, 0),
        np.asarray(zs, dtype=np.float32),
        np.asarray(errs, dtype=np.float32),
        np.asarray(tracks, dtype=np.int32),
    )


def filter_point_quality(
    xys: np.ndarray,
    zs: np.ndarray,
    errs: np.ndarray,
    tracks: np.ndarray,
    *,
    min_track: int,
    max_error_q: float,
) -> tuple[np.ndarray, np.ndarray, Optional[float]]:
    if len(zs) == 0:
        return xys, zs, None
    if max_error_q >= 1.0:
        err_cap = None
        ok_err = np.ones(len(zs), dtype=bool)
    else:
        err_cap = float(np.quantile(errs, max_error_q))
        ok_err = errs <= err_cap
    ok = (tracks >= min_track) & ok_err & np.isfinite(zs) & (zs > 1e-4)
    return xys[ok], zs[ok], err_cap


def unique_half_px(xys: np.ndarray, zs: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(zs) == 0:
        return xys, zs
    keys = np.round(xys * 2.0).astype(np.int32)
    _, idx = np.unique(keys, axis=0, return_index=True)
    idx = np.sort(idx)
    return xys[idx], zs[idx]


def tri_ok(
    pts: np.ndarray,
    zs: np.ndarray,
    face: np.ndarray,
    *,
    max_edge_px: float,
    max_rel_dz: float,
) -> bool:
    a, b, c = face
    p = pts[[a, b, c]]
    z = zs[[a, b, c]]
    if not np.all(np.isfinite(z)) or np.any(z <= 1e-4):
        return False
    edges = np.linalg.norm(p[[1, 2, 0]] - p[[0, 1, 2]], axis=1)
    if float(edges.max()) > max_edge_px:
        return False
    zmed = float(np.median(z))
    if zmed <= 1e-4:
        return False
    if float(z.max() - z.min()) / zmed > max_rel_dz:
        return False
    return True


@numba.njit(cache=True)
def _rasterize_depth_numba(
    h: int,
    w: int,
    pts: np.ndarray,
    zs: np.ndarray,
    faces: np.ndarray,
) -> np.ndarray:
    depth = np.zeros((h, w), dtype=np.float32)
    n_faces = faces.shape[0]
    for fi in range(n_faces):
        i0 = faces[fi, 0]
        i1 = faces[fi, 1]
        i2 = faces[fi, 2]
        ax = pts[i0, 0]
        ay = pts[i0, 1]
        bx = pts[i1, 0]
        by = pts[i1, 1]
        cx = pts[i2, 0]
        cy = pts[i2, 1]
        za = zs[i0]
        zb = zs[i1]
        zc = zs[i2]

        denom = (by - cy) * (ax - cx) + (cx - bx) * (ay - cy)
        if denom < 1e-8 and denom > -1e-8:
            continue
        inv_denom = 1.0 / denom

        min_x = int(np.floor(min(ax, bx, cx)))
        max_x = int(np.ceil(max(ax, bx, cx)))
        min_y = int(np.floor(min(ay, by, cy)))
        max_y = int(np.ceil(max(ay, by, cy)))
        if min_x < 0:
            min_x = 0
        if min_y < 0:
            min_y = 0
        if max_x > w - 1:
            max_x = w - 1
        if max_y > h - 1:
            max_y = h - 1
        if min_x > max_x or min_y > max_y:
            continue

        for y in range(min_y, max_y + 1):
            yf = y + 0.5
            for x in range(min_x, max_x + 1):
                xf = x + 0.5
                wa = ((by - cy) * (xf - cx) + (cx - bx) * (yf - cy)) * inv_denom
                wb = ((cy - ay) * (xf - cx) + (ax - cx) * (yf - cy)) * inv_denom
                wc = 1.0 - wa - wb
                if wa < 0.0 or wb < 0.0 or wc < 0.0:
                    continue
                z_pix = wa * za + wb * zb + wc * zc
                prev = depth[y, x]
                if prev <= 0.0 or z_pix < prev:
                    depth[y, x] = z_pix
    return depth


def rasterize_depth(
    h: int,
    w: int,
    pts: np.ndarray,
    zs: np.ndarray,
    faces: np.ndarray,
) -> np.ndarray:
    if faces.size == 0:
        return np.zeros((h, w), dtype=np.float32)
    return _rasterize_depth_numba(
        h,
        w,
        np.ascontiguousarray(pts, dtype=np.float64),
        np.ascontiguousarray(zs, dtype=np.float64),
        np.ascontiguousarray(faces, dtype=np.int32),
    )


def o3d_smooth_z(
    pts: np.ndarray,
    zs: np.ndarray,
    faces: np.ndarray,
    K: np.ndarray,
    *,
    iters: int,
    lam: float,
) -> np.ndarray:
    import open3d as o3d

    fx, fy = float(K[0, 0]), float(K[1, 1])
    cx, cy = float(K[0, 2]), float(K[1, 2])
    u = pts[:, 0].astype(np.float64)
    v = pts[:, 1].astype(np.float64)
    z = zs.astype(np.float64)
    xyz = np.column_stack([(u - cx) * z / fx, (v - cy) * z / fy, z])

    mesh = o3d.geometry.TriangleMesh()
    mesh.vertices = o3d.utility.Vector3dVector(xyz)
    mesh.triangles = o3d.utility.Vector3iVector(faces.astype(np.int32))
    mesh = mesh.filter_smooth_laplacian(number_of_iterations=iters, lambda_filter=float(lam))
    new_z = np.asarray(mesh.vertices)[:, 2]
    return np.maximum(new_z, 1e-4).astype(np.float32)


def depth_for_image(
    image: ColmapImage,
    camera,
    points3d: dict[int, Point3D],
    *,
    config: Optional[DelaunayConfig] = None,
) -> Optional[dict[str, Any]]:
    """Build one metric depth map. Returns None if too few anchors/tris."""
    config = config or DelaunayConfig()
    xys0, zs0, errs, tracks = collect_anchors(image, points3d)
    xys, zs, err_cap = filter_point_quality(
        xys0,
        zs0,
        errs,
        tracks,
        min_track=config.min_track,
        max_error_q=config.max_error_q,
    )
    xys, zs = unique_half_px(xys, zs)
    if len(zs) < 4:
        return None

    scale = float(config.scale)
    w = max(1, int(round(camera.width * scale)))
    h = max(1, int(round(camera.height * scale)))
    K = camera_to_K(camera).astype(np.float64)
    K_scaled = K.copy()
    K_scaled[0, :] *= scale
    K_scaled[1, :] *= scale

    pts = xys * scale
    max_edge = config.edge_frac * float(np.hypot(w, h))
    tri = Delaunay(pts)
    faces = [
        face
        for face in tri.simplices
        if tri_ok(pts, zs, face, max_edge_px=max_edge, max_rel_dz=config.max_rel_dz)
    ]
    faces_arr = np.asarray(faces, dtype=np.int32)
    if faces_arr.size == 0:
        return None

    zs_use = zs
    if config.o3d_laplacian_iters > 0:
        zs_use = o3d_smooth_z(
            pts,
            zs,
            faces_arr,
            K_scaled,
            iters=config.o3d_laplacian_iters,
            lam=config.o3d_laplacian_lambda,
        )

    depth = rasterize_depth(h, w, pts, zs_use, faces_arr)
    return {
        "depth": depth,
        "pts": pts,
        "zs": zs_use,
        "faces": faces_arr,
        "K_scaled": K_scaled,
        "err_cap": err_cap,
        "n_simplices": int(len(tri.simplices)),
        "cover": float(np.count_nonzero(depth > 0)) / float(h * w),
    }


def _select_images(images: dict, *, stride: int, max_images: int) -> list[ColmapImage]:
    items = sorted(images.values(), key=lambda im: im.name)
    if stride < 1:
        raise ValueError("stride must be >= 1")
    if stride > 1:
        items = items[::stride]
    if max_images and max_images > 0:
        items = items[:max_images]
    return items


def run_delaunay_depths(
    colmap_dir: str | Path,
    output_depth_dir: str | Path,
    *,
    config: Optional[DelaunayConfig] = None,
    sparse_subdir: str = "",
    image_names: Optional[Sequence[str]] = None,
) -> tuple[list[str], list[str], dict[str, Any]]:
    """Write metric depth PNGs for registered views. Returns (paths, names, meta)."""
    config = config or DelaunayConfig()
    colmap_dir = Path(colmap_dir)
    output_depth_dir = Path(output_depth_dir)
    output_depth_dir.mkdir(parents=True, exist_ok=True)

    sparse = resolve_sparse_dir(str(colmap_dir), sparse_subdir=sparse_subdir)
    cameras, images, points3d = read_model(str(sparse))

    if image_names is not None:
        want = {Path(n).name for n in image_names}
        frames = [im for im in images.values() if Path(im.name).name in want or im.name in want]
        frames.sort(key=lambda im: im.name)
    else:
        frames = _select_images(images, stride=config.stride, max_images=config.max_images)

    depths: dict[str, np.ndarray] = {}
    skipped = 0
    covers: list[float] = []
    for im in frames:
        cam = cameras[im.camera_id]
        result = depth_for_image(im, cam, points3d, config=config)
        if result is None:
            skipped += 1
            continue
        depths[im.name] = result["depth"]
        covers.append(float(result["cover"]))

    if not depths:
        raise RuntimeError("Delaunay produced no depth maps (too few anchors/tris)")

    save_depth_folder(depths, output_depth_dir)
    names = list(depths.keys())
    paths = [str(colmap_dir / "images" / Path(n).name) for n in names]
    # Prefer full paths when images live under colmap_dir/images
    resolved = []
    for n, p in zip(names, paths):
        cand = colmap_dir / "images" / Path(n).name
        if not cand.is_file():
            cand = colmap_dir / "images" / n
        resolved.append(str(cand if cand.is_file() else p))

    meta = {
        "method": "delaunay",
        "n_requested": len(frames),
        "n_written": len(names),
        "n_skipped": skipped,
        "mean_cover": float(np.mean(covers)) if covers else 0.0,
        "config": {
            "min_track": config.min_track,
            "max_error_q": config.max_error_q,
            "max_rel_dz": config.max_rel_dz,
            "edge_frac": config.edge_frac,
            "scale": config.scale,
            "o3d_laplacian_iters": config.o3d_laplacian_iters,
            "o3d_laplacian_lambda": config.o3d_laplacian_lambda,
            "stride": config.stride,
            "max_images": config.max_images,
        },
    }
    return resolved, names, meta
