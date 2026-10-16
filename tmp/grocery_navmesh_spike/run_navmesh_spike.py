#!/usr/bin/env python3
"""Offline grocery navmesh spike: floor evidence + planar enclosed-hole close."""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import open3d as o3d
from PIL import Image
from scipy import ndimage


@dataclass
class Knobs:
    grid_cell_m: float = 0.05
    up_normal_max_deg: float = 30.0
    floor_y_min: float = -0.15
    floor_y_max: float = 0.35
    layer_snap_m: float = 0.20
    use_layer_snap: bool = True
    wall_up_dot_max: float = 0.25  # |n·up| below this => near-vertical wall
    wall_base_y_max: float = 0.55  # centroid Y below this counts as "near floor"
    seed_dilate_m: float = 0.50  # diagnostic traj overlay only
    grid_pad_m: float = 0.25
    # Preprocess
    smooth_simple_iters: int = 2  # Open3D filter_smooth_taubin before normals (name kept)
    min_blocker_component_cells: int = 16  # drop tiny blocker CCs (~0.04 m² at 5 cm)
    # Ignore blockers near camera path (feet / passers-by) for hole-fill only
    traj_ignore_radius_m: float = 0.30  # horizontal XZ
    traj_ignore_height_m: float = 1.50  # |blocker_y - cam_y|
    # Enclosed hole close (evidence only — no invent outside the rim)
    max_hole_area_m2: float = 16.0  # scaled with 4 m extent
    max_hole_extent_m: float = 4.0
    hole_normal_max_deg: float = 25.0  # plane normal vs +Y
    hole_planar_rms_m: float = 0.06  # rim + fill coplanarity
    min_rim_cells: int = 8
    # Seal 1-cell cracks in evidence before hole detect so mid-aisle gaps can enclose
    evidence_bridge_cells: int = 1
    # Open gaps along traj (not a closed ring) — same planar gates, corridor-limited
    gap_corridor_radius_m: float = 0.75
    # Half-width to nearest evidence; with opposite sides → max bridge ~1 m
    gap_max_dist_from_evidence_m: float = 4.0  # max XZ distance from floor evidence
    gap_max_width_m: float = 4.0  # min bbox side (across the gap)
    gap_max_length_m: float = 4.0  # along the aisle
    gap_max_area_m2: float = 16.0
    gap_require_opposite_rim: bool = True  # evidence on both sides — not a void edge
    # Pure opposite = 180°; degrees off each across-axis pole (90 ≈ half-plane, looser)
    gap_opposite_cone_deg: float = 90.0
    # Contiguous floor mesh from walkable heightfield
    mesh_fill_hole_cells: int = 0  # legacy binary_close iters (0 = off; thin-hole rule used instead)
    # Enclosed ~walkable CCs: area < max_area and min(bbox_w, bbox_h) <= max_width
    mesh_thin_hole_max_area: int = 4  # exclusive: fill only if cell count < this (1×3 ok)
    mesh_thin_hole_max_width: int = 1  # at most this many cells wide in either axis
    mesh_min_island_cells: int = 12  # drop tiny leftover islands
    mesh_max_height_spread_m: float = 0.30  # skip cells/corners spanning shelves etc.
    # Soft flatten heightfield (median) before meshing — same-surface only
    mesh_flatten_radius_m: float = 0.55  # XZ neighborhood for median
    mesh_flatten_outlier_m: float = 0.06  # |Δy| vs cell; larger ⇒ shelf/edge excluded
    mesh_flatten_min_samples: int = 5  # need this many inliers to replace height
    mesh_flatten_iters: int = 2  # repeat median pass
    # 1.0 keeps exported tris on the heightfield grid; >1 merges cells (looks coarser)
    mesh_voxel_downsample_scale: float = 1.0  # voxel size = scale × grid_cell before Taubin
    mesh_smooth_iters: int = 2  # Taubin smooth on output floor mesh


def load_trajectory_positions(traj_path: Path) -> np.ndarray:
    data = json.loads(traj_path.read_text(encoding="utf-8"))
    pts: list[list[float]] = []
    for poly in data.get("polylines", []):
        pts.extend(poly.get("positions", []))
    if not pts:
        raise ValueError(f"No positions in trajectory: {traj_path}")
    return np.asarray(pts, dtype=np.float64), data


def remap_colmap_x_up_to_y_up(points: np.ndarray) -> np.ndarray:
    """Match gallery/traj remap: (x, y, z) → (−y, x, z)."""
    out = np.empty_like(points)
    out[:, 0] = -points[:, 1]
    out[:, 1] = points[:, 0]
    out[:, 2] = points[:, 2]
    return out


def mesh_looks_x_up(vertices: np.ndarray) -> bool:
    """Heuristic: COLMAP grocery meshes have ~room-height span on +X, longer on Y/Z."""
    span = vertices.max(axis=0) - vertices.min(axis=0)
    return bool(span[0] < span[1] and span[0] < span[2] and span[0] < 6.0)


def triangle_centroids(vertices: np.ndarray, triangles: np.ndarray) -> np.ndarray:
    return vertices[triangles].mean(axis=1)


def classify_floor_and_walls(
    centroids: np.ndarray,
    normals: np.ndarray,
    knobs: Knobs,
) -> tuple[np.ndarray, np.ndarray, float]:
    up = np.array([0.0, 1.0, 0.0], dtype=np.float64)
    ndot = np.clip(normals @ up, -1.0, 1.0)
    angles = np.degrees(np.arccos(ndot))

    floor_normal = angles <= knobs.up_normal_max_deg
    y = centroids[:, 1]
    floor_band = (y >= knobs.floor_y_min) & (y <= knobs.floor_y_max)
    floor_mask = floor_normal & floor_band

    if floor_mask.sum() == 0:
        # Fallback: RANSAC-ish peak among up-facing triangles near Y=0
        floor_normal_y = floor_normal
        ys = y[floor_normal_y]
        if ys.size == 0:
            raise RuntimeError("No up-facing triangles found for floor detection")
        hist, edges = np.histogram(ys, bins=80)
        peak = int(hist.argmax())
        y_lo, y_hi = float(edges[peak]), float(edges[peak + 1])
        pad = 0.15
        floor_mask = floor_normal & (y >= y_lo - pad) & (y <= y_hi + pad)

    floor_ys = y[floor_mask]
    snap_y = float(np.median(floor_ys))
    if knobs.use_layer_snap and knobs.layer_snap_m > 0:
        snap_y = round(snap_y / knobs.layer_snap_m) * knobs.layer_snap_m

    wall_mask = (np.abs(ndot) <= knobs.wall_up_dot_max) & (y <= knobs.wall_base_y_max)
    return floor_mask, wall_mask, snap_y


def build_grid_bounds(vertices: np.ndarray, traj_xz: np.ndarray, pad: float) -> tuple[np.ndarray, int, int]:
    xs = np.concatenate([vertices[:, 0], traj_xz[:, 0]])
    zs = np.concatenate([vertices[:, 2], traj_xz[:, 1]])
    x_min, x_max = float(xs.min() - pad), float(xs.max() + pad)
    z_min, z_max = float(zs.min() - pad), float(zs.max() + pad)
    return np.array([x_min, z_min, x_max, z_max], dtype=np.float64), 0, 0


def xz_to_ij(x: np.ndarray, z: np.ndarray, bounds: np.ndarray, cell: float) -> tuple[np.ndarray, np.ndarray]:
    x_min, z_min, _, _ = bounds
    i = np.floor((x - x_min) / cell).astype(np.int64)
    j = np.floor((z - z_min) / cell).astype(np.int64)
    return i, j


def grid_shape(bounds: np.ndarray, cell: float) -> tuple[int, int]:
    x_min, z_min, x_max, z_max = bounds
    ni = int(math.ceil((x_max - x_min) / cell)) + 1
    nj = int(math.ceil((z_max - z_min) / cell)) + 1
    return ni, nj


def stamp_triangles_fast(
    vertices: np.ndarray,
    triangles: np.ndarray,
    tri_mask: np.ndarray,
    bounds: np.ndarray,
    cell: float,
    ni: int,
    nj: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Stamp triangle coverage via centroids (fast) + optional vertex scatter."""
    grid = np.zeros((ni, nj), dtype=np.uint8)
    height_sum = np.zeros((ni, nj), dtype=np.float64)
    height_cnt = np.zeros((ni, nj), dtype=np.int32)

    tri_idx = np.nonzero(tri_mask)[0]
    tris = triangles[tri_idx]
    ctr = vertices[tris].mean(axis=1)
    i, j = xz_to_ij(ctr[:, 0], ctr[:, 2], bounds, cell)
    valid = (i >= 0) & (i < ni) & (j >= 0) & (j < nj)
    i, j = i[valid], j[valid]
    ys = ctr[valid, 1]
    grid[i, j] = 1
    np.add.at(height_sum, (i, j), ys)
    np.add.at(height_cnt, (i, j), 1)

    # Thicken stamps with triangle vertex projections
    sel = tris[valid]
    flat = sel.reshape(-1, 3)
    vi, vj = xz_to_ij(flat[:, 0], flat[:, 2], bounds, cell)
    vy = flat[:, 1]
    vvalid = (vi >= 0) & (vi < ni) & (vj >= 0) & (vj < nj)
    vi, vj, vy = vi[vvalid], vj[vvalid], vy[vvalid]
    grid[vi, vj] = 1
    np.add.at(height_sum, (vi, vj), vy)
    np.add.at(height_cnt, (vi, vj), 1)

    with np.errstate(divide="ignore", invalid="ignore"):
        height_avg = np.where(height_cnt > 0, height_sum / height_cnt, np.nan)
    return grid, height_avg


def dilate_disk(grid: np.ndarray, radius_cells: int) -> np.ndarray:
    if radius_cells <= 0:
        return grid.astype(bool)
    y, x = np.ogrid[-radius_cells : radius_cells + 1, -radius_cells : radius_cells + 1]
    struct = (x * x + y * y) <= radius_cells * radius_cells
    return ndimage.binary_dilation(grid.astype(bool), structure=struct)


def remove_small_components(mask: np.ndarray, min_cells: int) -> tuple[np.ndarray, dict]:
    """Keep only connected components with >= min_cells (4-connected)."""
    m = mask.astype(bool)
    if min_cells <= 1 or not m.any():
        return m.astype(np.uint8), {"components_in": 0, "components_kept": 0, "cells_removed": 0}
    labeled, nlab = ndimage.label(m)
    keep = np.zeros_like(m, dtype=bool)
    kept = 0
    removed_cells = 0
    for lab in range(1, nlab + 1):
        comp = labeled == lab
        n = int(comp.sum())
        if n >= min_cells:
            keep[comp] = True
            kept += 1
        else:
            removed_cells += n
    return keep.astype(np.uint8), {
        "components_in": int(nlab),
        "components_kept": kept,
        "cells_removed": removed_cells,
    }


def blockers_ignored_near_trajectory(
    blockers: np.ndarray,
    blocker_height: np.ndarray,
    traj_xyz: np.ndarray,
    bounds: np.ndarray,
    cell: float,
    radius_m: float,
    height_half_m: float,
) -> tuple[np.ndarray, dict]:
    """Blockers within radius_m (XZ) of traj and |y - cam_y| <= height_half_m → ignore for hole fill.

    Typical false positives: operator feet, nearby shoppers — vertical scraps along the path.
    """
    from scipy.spatial import cKDTree

    ignored = np.zeros_like(blockers, dtype=bool)
    blk = blockers.astype(bool)
    if not blk.any() or traj_xyz.size == 0 or radius_m <= 0:
        return ignored.astype(np.uint8), {"cells_ignored": 0, "candidates": 0}

    x_min, z_min, _, _ = bounds
    ni, nj = blockers.shape
    # Corridor candidates via dilate (cheap reject), then exact KD query
    stamp = np.zeros((ni, nj), dtype=np.uint8)
    ti, tj = xz_to_ij(traj_xyz[:, 0], traj_xyz[:, 2], bounds, cell)
    ok = (ti >= 0) & (ti < ni) & (tj >= 0) & (tj < nj)
    stamp[ti[ok], tj[ok]] = 1
    radius_cells = int(math.ceil(radius_m / cell))
    corridor = dilate_disk(stamp, radius_cells)
    cand = blk & corridor
    ci, cj = np.nonzero(cand)
    stats = {"cells_ignored": 0, "candidates": int(ci.size)}
    if ci.size == 0:
        return ignored.astype(np.uint8), stats

    cx = x_min + (ci + 0.5) * cell
    cz = z_min + (cj + 0.5) * cell
    tree = cKDTree(traj_xyz[:, [0, 2]])
    dist, idx = tree.query(np.column_stack([cx, cz]), distance_upper_bound=radius_m + 1e-6)
    near = np.isfinite(dist) & (dist <= radius_m)
    if not near.any():
        return ignored.astype(np.uint8), stats

    hy = blocker_height[ci[near], cj[near]]
    ty = traj_xyz[idx[near], 1]
    # Missing height: still treat as ignore if inside corridor (feet often sparse stamps)
    height_ok = np.isnan(hy) | (np.abs(hy - ty) <= height_half_m)
    sel_i = ci[near][height_ok]
    sel_j = cj[near][height_ok]
    ignored[sel_i, sel_j] = True
    stats["cells_ignored"] = int(ignored.sum())
    return ignored.astype(np.uint8), stats


def close_planar_enclosed_holes(
    evidence: np.ndarray,
    height_avg: np.ndarray,
    blockers: np.ndarray,
    bounds: np.ndarray,
    cell: float,
    knobs: Knobs,
) -> tuple[np.ndarray, np.ndarray, dict]:
    """Fill only fully enclosed holes in evidence that are small, up-facing, near-planar.

    Exterior background (touches grid border) is never filled — that is what stopped
    invent-flood from extruding outside the store / through unscanned wall gaps.
    """
    ev = evidence.astype(bool)
    blocked = blockers.astype(bool)
    # Light morphological close so tiny rim cracks do not open holes to "exterior"
    if knobs.evidence_bridge_cells > 0:
        ev_for_holes = ndimage.binary_closing(ev, iterations=int(knobs.evidence_bridge_cells))
    else:
        ev_for_holes = ev
    closed = ev_for_holes.copy()
    filled = np.zeros_like(ev, dtype=bool)
    x_min, z_min, _, _ = bounds

    bg = ~ev_for_holes
    labeled, nlab = ndimage.label(bg)
    border = set()
    border.update(labeled[0, :].tolist())
    border.update(labeled[-1, :].tolist())
    border.update(labeled[:, 0].tolist())
    border.update(labeled[:, -1].tolist())
    border.discard(0)

    stats = {
        "holes_seen": 0,
        "holes_closed": 0,
        "holes_rejected_size": 0,
        "holes_rejected_rim": 0,
        "holes_rejected_normal": 0,
        "holes_rejected_planar": 0,
        "holes_rejected_blocker": 0,
        "cells_filled": 0,
    }
    up_cos = math.cos(math.radians(knobs.hole_normal_max_deg))

    for lab in range(1, nlab + 1):
        if lab in border:
            continue
        hole = labeled == lab
        stats["holes_seen"] += 1
        n_cells = int(hole.sum())
        area_m2 = n_cells * cell * cell
        ii, jj = np.nonzero(hole)
        extent_m = max((ii.max() - ii.min() + 1) * cell, (jj.max() - jj.min() + 1) * cell)
        if area_m2 > knobs.max_hole_area_m2 or extent_m > knobs.max_hole_extent_m:
            stats["holes_rejected_size"] += 1
            continue

        # Do not fill through wall stamps sitting in the hole
        if blocked[hole].any():
            stats["holes_rejected_blocker"] += 1
            continue

        rim = ndimage.binary_dilation(hole, iterations=1) & ev_for_holes
        ri, rj = np.nonzero(rim)
        if ri.size < knobs.min_rim_cells:
            stats["holes_rejected_rim"] += 1
            continue
        ys = height_avg[ri, rj]
        ok = ~np.isnan(ys)
        if int(ok.sum()) < knobs.min_rim_cells:
            stats["holes_rejected_rim"] += 1
            continue
        ri, rj, ys = ri[ok], rj[ok], ys[ok]
        xs = x_min + (ri + 0.5) * cell
        zs = z_min + (rj + 0.5) * cell
        pts = np.stack([xs, ys, zs], axis=1)
        centroid = pts.mean(axis=0)
        _, _, vh = np.linalg.svd(pts - centroid, full_matrices=False)
        normal = vh[-1].astype(np.float64)
        if normal[1] < 0:
            normal = -normal
        if normal[1] < up_cos:
            stats["holes_rejected_normal"] += 1
            continue

        rim_res = np.abs((pts - centroid) @ normal)
        rim_rms = float(np.sqrt(np.mean(rim_res * rim_res)))
        if rim_rms > knobs.hole_planar_rms_m:
            stats["holes_rejected_planar"] += 1
            continue

        # Fill heights from the rim plane; require fill samples stay near-planar too
        hi, hj = np.nonzero(hole)
        hx = x_min + (hi + 0.5) * cell
        hz = z_min + (hj + 0.5) * cell
        # (p - c) · n = 0 → n_y (y - c_y) = -n_x (x-c_x) - n_z (z-c_z)
        if abs(normal[1]) < 1e-6:
            stats["holes_rejected_normal"] += 1
            continue
        hy = centroid[1] - (normal[0] * (hx - centroid[0]) + normal[2] * (hz - centroid[2])) / normal[1]
        fill_pts = np.stack([hx, hy, hz], axis=1)
        fill_res = np.abs((fill_pts - centroid) @ normal)
        fill_rms = float(np.sqrt(np.mean(fill_res * fill_res)))
        # fill lies on the fitted plane by construction; still gate if rim was marginal
        combo_rms = float(np.sqrt(0.5 * (rim_rms * rim_rms + fill_rms * fill_rms)))
        if combo_rms > knobs.hole_planar_rms_m:
            stats["holes_rejected_planar"] += 1
            continue

        closed[hole] = True
        filled[hole] = True
        height_avg[hi, hj] = hy
        stats["holes_closed"] += 1
        stats["cells_filled"] += n_cells

    walkable = (closed & ~blocked).astype(np.uint8)
    return walkable, filled.astype(np.uint8), stats


def _rim_opposite_across_gap(
    region: np.ndarray,
    rim: np.ndarray,
    bounds: np.ndarray,
    cell: float,
    opposite_cone_deg: float = 50.0,
    min_radial_m: float = 0.02,
) -> bool:
    """True if evidence rim brackets the gap across its short axis (angle-aware).

    PCA on the gap footprint gives the across-axis. Pure opposite = ±across (180°).
    A rim sample counts for a side if its bearing is within ``opposite_cone_deg``
    of that pole (e.g. 50° → accept 130°–230° separation, not only exact 180°).
    """
    ii, jj = np.nonzero(region)
    ri, rj = np.nonzero(rim)
    if ii.size < 3 or ri.size < 2:
        return False
    x_min, z_min, _, _ = bounds
    gap_xz = np.column_stack(
        [x_min + (ii + 0.5) * cell, z_min + (jj + 0.5) * cell]
    )
    rim_xz = np.column_stack(
        [x_min + (ri + 0.5) * cell, z_min + (rj + 0.5) * cell]
    )
    center = gap_xz.mean(axis=0)
    centered = gap_xz - center
    _, _, vh = np.linalg.svd(centered, full_matrices=False)
    if vh.shape[0] < 2:
        across = np.array([-vh[0, 1], vh[0, 0]], dtype=np.float64)
    else:
        across = vh[1].astype(np.float64)
    across_n = float(np.linalg.norm(across))
    if across_n < 1e-12:
        return False
    across = across / across_n

    rim_rel = rim_xz - center
    rad = np.linalg.norm(rim_rel, axis=1)
    ok = rad >= min_radial_m
    if int(ok.sum()) < 2:
        return False
    dirs = rim_rel[ok] / rad[ok, None]
    # angle to +across / -across ≤ cone ⇔ dot >= cos(cone)
    cos_lim = math.cos(math.radians(opposite_cone_deg))
    align_pos = dirs @ across
    align_neg = dirs @ (-across)
    return bool(np.any(align_pos >= cos_lim) and np.any(align_neg >= cos_lim))


def _try_fill_planar_region(
    region: np.ndarray,
    evidence: np.ndarray,
    height_avg: np.ndarray,
    bounds: np.ndarray,
    cell: float,
    knobs: Knobs,
    require_opposite_rim: bool,
) -> tuple[bool, str]:
    """Shared planar/up-facing fill. Mutates height_avg on success. Returns (ok, reason)."""
    x_min, z_min, _, _ = bounds
    up_cos = math.cos(math.radians(knobs.hole_normal_max_deg))
    rim = ndimage.binary_dilation(region, iterations=1) & evidence.astype(bool)
    if require_opposite_rim and not _rim_opposite_across_gap(
        region, rim, bounds, cell, knobs.gap_opposite_cone_deg
    ):
        return False, "rim_sides"
    ri, rj = np.nonzero(rim)
    if ri.size < knobs.min_rim_cells:
        return False, "rim"
    ys = height_avg[ri, rj]
    ok = ~np.isnan(ys)
    if int(ok.sum()) < knobs.min_rim_cells:
        return False, "rim"
    ri, rj, ys = ri[ok], rj[ok], ys[ok]
    xs = x_min + (ri + 0.5) * cell
    zs = z_min + (rj + 0.5) * cell
    pts = np.stack([xs, ys, zs], axis=1)
    centroid = pts.mean(axis=0)
    _, _, vh = np.linalg.svd(pts - centroid, full_matrices=False)
    normal = vh[-1].astype(np.float64)
    if normal[1] < 0:
        normal = -normal
    if normal[1] < up_cos or abs(normal[1]) < 1e-6:
        return False, "normal"
    rim_rms = float(np.sqrt(np.mean(((pts - centroid) @ normal) ** 2)))
    if rim_rms > knobs.hole_planar_rms_m:
        return False, "planar"
    hi, hj = np.nonzero(region)
    hx = x_min + (hi + 0.5) * cell
    hz = z_min + (hj + 0.5) * cell
    hy = centroid[1] - (normal[0] * (hx - centroid[0]) + normal[2] * (hz - centroid[2])) / normal[1]
    fill_pts = np.stack([hx, hy, hz], axis=1)
    fill_rms = float(np.sqrt(np.mean(((fill_pts - centroid) @ normal) ** 2)))
    combo_rms = float(np.sqrt(0.5 * (rim_rms * rim_rms + fill_rms * fill_rms)))
    if combo_rms > knobs.hole_planar_rms_m:
        return False, "planar"
    height_avg[hi, hj] = hy
    return True, "ok"


def fill_traj_open_gaps(
    walkable: np.ndarray,
    evidence: np.ndarray,
    height_avg: np.ndarray,
    blockers: np.ndarray,
    traj_xyz: np.ndarray,
    bounds: np.ndarray,
    cell: float,
    knobs: Knobs,
) -> tuple[np.ndarray, np.ndarray, dict]:
    """Fill open (non-enclosed) gaps under the traj corridor with the same planar gates.

    Unlike enclosed holes, the evidence rim need not form a closed ring — only enough
    neighboring floor on multiple sides, near the path, not crossing hard blockers.
    """
    walk = walkable.astype(bool).copy()
    ev = evidence.astype(bool)
    blocked = blockers.astype(bool)
    gaps_filled = np.zeros_like(walk, dtype=bool)
    ni, nj = walk.shape

    stamp = np.zeros((ni, nj), dtype=np.uint8)
    ti, tj = xz_to_ij(traj_xyz[:, 0], traj_xyz[:, 2], bounds, cell)
    ok = (ti >= 0) & (ti < ni) & (tj >= 0) & (tj < nj)
    stamp[ti[ok], tj[ok]] = 1
    corridor = dilate_disk(stamp, int(math.ceil(knobs.gap_corridor_radius_m / cell)))

    dist_ev = ndimage.distance_transform_edt(~ev) * cell
    near_ev = dist_ev <= knobs.gap_max_dist_from_evidence_m
    cand = corridor & near_ev & ~walk & ~blocked

    labeled, nlab = ndimage.label(cand)
    stats = {
        "gaps_seen": 0,
        "gaps_closed": 0,
        "gaps_rejected_size": 0,
        "gaps_rejected_rim": 0,
        "gaps_rejected_normal": 0,
        "gaps_rejected_planar": 0,
        "gaps_rejected_blocker": 0,
        "cells_filled": 0,
    }

    for lab in range(1, nlab + 1):
        region = labeled == lab
        stats["gaps_seen"] += 1
        n_cells = int(region.sum())
        area_m2 = n_cells * cell * cell
        ii, jj = np.nonzero(region)
        extent_i = (ii.max() - ii.min() + 1) * cell
        extent_j = (jj.max() - jj.min() + 1) * cell
        gap_width = min(extent_i, extent_j)
        gap_length = max(extent_i, extent_j)
        if (
            area_m2 > knobs.gap_max_area_m2
            or gap_width > knobs.gap_max_width_m
            or gap_length > knobs.gap_max_length_m
        ):
            stats["gaps_rejected_size"] += 1
            continue
        if blocked[region].any():
            stats["gaps_rejected_blocker"] += 1
            continue
        ok_fill, reason = _try_fill_planar_region(
            region,
            ev,
            height_avg,
            bounds,
            cell,
            knobs,
            knobs.gap_require_opposite_rim,
        )
        if not ok_fill:
            key = {
                "rim": "gaps_rejected_rim",
                "rim_sides": "gaps_rejected_rim",
                "normal": "gaps_rejected_normal",
                "planar": "gaps_rejected_planar",
            }.get(reason, "gaps_rejected_rim")
            stats[key] += 1
            continue
        walk[region] = True
        gaps_filled[region] = True
        stats["gaps_closed"] += 1
        stats["cells_filled"] += n_cells

    return walk.astype(np.uint8), gaps_filled.astype(np.uint8), stats


def fill_thin_enclosed_holes(
    walkable: np.ndarray,
    height_avg: np.ndarray,
    knobs: Knobs,
) -> tuple[np.ndarray, np.ndarray, dict]:
    """Fill enclosed holes that are thin in either axis and smaller than max area.

    A background (non-walkable) component is filled when:
    - it does not touch the grid border (enclosed),
    - cell count < ``mesh_thin_hole_max_area`` (default 4 ⇒ at most 3 cells),
    - min(bbox width, bbox height) <= ``mesh_thin_hole_max_width`` (default 1).

    So 1×1, 1×2, 1×3 close; 2×2 does not. Heights for filled cells are the mean
    of originally-walkable 4-neighbors (fallback: any finite neighbor height).
    """
    orig = walkable.astype(bool)
    m = orig.copy()
    h = height_avg.astype(np.float64).copy()
    max_area = int(knobs.mesh_thin_hole_max_area)
    max_width = int(knobs.mesh_thin_hole_max_width)
    stats = {
        "max_area_exclusive": max_area,
        "max_width_cells": max_width,
        "holes_seen": 0,
        "holes_filled": 0,
        "holes_rejected_size": 0,
        "holes_rejected_width": 0,
        "holes_rejected_border": 0,
        "cells_filled": 0,
    }
    if max_area <= 1:
        return m.astype(np.uint8), h, stats

    labeled, nlab = ndimage.label(~m)
    if nlab == 0:
        return m.astype(np.uint8), h, stats

    border_labs = set()
    border_labs.update(int(x) for x in labeled[0, :].ravel())
    border_labs.update(int(x) for x in labeled[-1, :].ravel())
    border_labs.update(int(x) for x in labeled[:, 0].ravel())
    border_labs.update(int(x) for x in labeled[:, -1].ravel())
    border_labs.discard(0)

    ni, nj = m.shape
    neighbors = ((1, 0), (-1, 0), (0, 1), (0, -1))
    for lab in range(1, nlab + 1):
        stats["holes_seen"] += 1
        if lab in border_labs:
            stats["holes_rejected_border"] += 1
            continue
        comp = labeled == lab
        n_cells = int(comp.sum())
        if n_cells >= max_area:
            stats["holes_rejected_size"] += 1
            continue
        ii, jj = np.nonzero(comp)
        bw = int(ii.max() - ii.min() + 1)
        bh = int(jj.max() - jj.min() + 1)
        if min(bw, bh) > max_width:
            stats["holes_rejected_width"] += 1
            continue

        # Heights from original walkable rim
        ys: list[float] = []
        for i, j in zip(ii.tolist(), jj.tolist()):
            for di, dj in neighbors:
                ni2, nj2 = i + di, j + dj
                if ni2 < 0 or ni2 >= ni or nj2 < 0 or nj2 >= nj:
                    continue
                if not orig[ni2, nj2]:
                    continue
                hy = h[ni2, nj2]
                if np.isfinite(hy):
                    ys.append(float(hy))
        fill_y = float(np.mean(ys)) if ys else np.nan
        m[comp] = True
        if np.isfinite(fill_y):
            h[comp] = fill_y
        stats["holes_filled"] += 1
        stats["cells_filled"] += n_cells

    return m.astype(np.uint8), h, stats


def clean_walkable_for_mesh(
    walkable: np.ndarray,
    height_avg: np.ndarray,
    knobs: Knobs,
) -> tuple[np.ndarray, np.ndarray, dict]:
    """Thin enclosed-hole fill, optional binary close, drop tiny islands."""
    m = walkable.astype(bool)
    before = int(m.sum())
    m_u8, h, thin_stats = fill_thin_enclosed_holes(m, height_avg, knobs)
    m = m_u8.astype(bool)
    after_thin = int(m.sum())
    if knobs.mesh_fill_hole_cells > 0:
        m = ndimage.binary_closing(m, iterations=int(knobs.mesh_fill_hole_cells))
    after_close = int(m.sum())
    cleaned, cc = remove_small_components(m, knobs.mesh_min_island_cells)
    return cleaned.astype(np.uint8), h, {
        "cells_before": before,
        "cells_after_thin_holes": after_thin,
        "cells_after_close": after_close,
        "cells_after_island_filter": int(cleaned.sum()),
        "thin_holes": thin_stats,
        "island_filter": cc,
    }


def soft_flatten_heights_median(
    height_avg: np.ndarray,
    walkable: np.ndarray,
    cell: float,
    knobs: Knobs,
) -> tuple[np.ndarray, dict]:
    """Median-filter walkable heights; neighbors must match the cell (exclude shelf edges).

    For each walkable cell, take a disk of radius ``mesh_flatten_radius_m``. Keep only
    neighbors with finite height and ``|h_n - h_cell| <= mesh_flatten_outlier_m`` (same
    surface). If at least ``mesh_flatten_min_samples`` inliers, replace with their median.
    Repeats ``mesh_flatten_iters`` times.
    """
    walk = walkable.astype(bool)
    h_cur = np.where(walk, height_avg.astype(np.float64), np.nan)
    iters = max(1, int(knobs.mesh_flatten_iters))
    stats = {
        "radius_m": float(knobs.mesh_flatten_radius_m),
        "outlier_m": float(knobs.mesh_flatten_outlier_m),
        "min_samples": int(knobs.mesh_flatten_min_samples),
        "iters": iters,
        "cells_considered": int(walk.sum()),
        "cells_updated": 0,
        "cells_skipped_few_samples": 0,
        "cells_skipped_nan": 0,
        "mean_abs_delta_m": 0.0,
        "p95_abs_delta_m": 0.0,
    }
    if knobs.mesh_flatten_radius_m <= 0 or not walk.any():
        return h_cur, stats

    ni, nj = h_cur.shape
    r_cells = max(1, int(math.ceil(knobs.mesh_flatten_radius_m / cell)))
    offs = [
        (di, dj)
        for di in range(-r_cells, r_cells + 1)
        for dj in range(-r_cells, r_cells + 1)
        if di * di + dj * dj <= r_cells * r_cells
    ]
    outlier = float(knobs.mesh_flatten_outlier_m)
    min_n = int(knobs.mesh_flatten_min_samples)
    ii, jj = np.nonzero(walk)
    deltas: list[float] = []
    chunk = 4096

    for _it in range(iters):
        h_in = h_cur
        h_out = h_in.copy()
        for start in range(0, len(ii), chunk):
            sl = slice(start, start + chunk)
            ci = ii[sl]
            cj = jj[sl]
            hc = h_in[ci, cj]
            nan_cell = ~np.isfinite(hc)
            stats["cells_skipped_nan"] += int(nan_cell.sum())
            stack = np.empty((len(offs), len(ci)), dtype=np.float64)
            for k, (di, dj) in enumerate(offs):
                ni2 = ci + di
                nj2 = cj + dj
                valid = (ni2 >= 0) & (ni2 < ni) & (nj2 >= 0) & (nj2 < nj)
                col = np.full(len(ci), np.nan, dtype=np.float64)
                if valid.any():
                    col[valid] = h_in[ni2[valid], nj2[valid]]
                stack[k] = col
            ok = np.isfinite(stack) & np.isfinite(hc) & (np.abs(stack - hc) <= outlier)
            counts = ok.sum(axis=0)
            med = np.full(len(ci), np.nan, dtype=np.float64)
            enough = counts >= min_n
            if enough.any():
                stack_m = np.where(ok[:, enough], stack[:, enough], np.nan)
                med[enough] = np.nanmedian(stack_m, axis=0)
            apply = (~nan_cell) & enough & np.isfinite(med)
            few = (~nan_cell) & ~apply
            stats["cells_skipped_few_samples"] += int(few.sum())
            if apply.any():
                h_out[ci[apply], cj[apply]] = med[apply]
                d = np.abs(med[apply] - hc[apply])
                deltas.extend(d.tolist())
                stats["cells_updated"] += int(apply.sum())
        h_cur = h_out

    if deltas:
        arr = np.asarray(deltas, dtype=np.float64)
        stats["mean_abs_delta_m"] = float(arr.mean())
        stats["p95_abs_delta_m"] = float(np.percentile(arr, 95))
    return h_cur, stats


def export_contiguous_floor_mesh(
    walkable: np.ndarray,
    bounds: np.ndarray,
    cell: float,
    snap_y: float,
    height_avg: np.ndarray,
    out_path: Path,
    knobs: Knobs,
) -> tuple[int, int, dict]:
    """Shared-vertex heightfield mesh; skip cells with corner height spread > threshold.

    Returns (n_triangles, n_vertices, stats).
    """
    x_min, z_min, _, _ = bounds
    ni, nj = walkable.shape
    walk = walkable.astype(bool)
    stats = {
        "cells_considered": int(walk.sum()),
        "cells_meshed": 0,
        "cells_skipped_height_spread": 0,
        "cells_skipped_bad_corner": 0,
    }

    # Per-cell height (fallback snap)
    h_cell = np.where(walk, height_avg, np.nan)
    h_cell = np.where(np.isnan(h_cell) & walk, snap_y, h_cell)

    # Corner heights: average of walkable cells touching each corner
    corner_sum = np.zeros((ni + 1, nj + 1), dtype=np.float64)
    corner_cnt = np.zeros((ni + 1, nj + 1), dtype=np.int32)
    ii, jj = np.nonzero(walk)
    for di, dj in ((0, 0), (1, 0), (0, 1), (1, 1)):
        np.add.at(corner_sum, (ii + di, jj + dj), h_cell[ii, jj])
        np.add.at(corner_cnt, (ii + di, jj + dj), 1)
    with np.errstate(invalid="ignore"):
        corner_h = np.where(corner_cnt > 0, corner_sum / corner_cnt, np.nan)

    # Assign vertex ids only for corners we actually use
    vert_id = np.full((ni + 1, nj + 1), -1, dtype=np.int32)
    vertices: list[list[float]] = []
    triangles: list[list[int]] = []

    def ensure_vert(ci: int, cj: int) -> int:
        vid = int(vert_id[ci, cj])
        if vid >= 0:
            return vid
        y = float(corner_h[ci, cj])
        vid = len(vertices)
        vertices.append([x_min + ci * cell, y, z_min + cj * cell])
        vert_id[ci, cj] = vid
        return vid

    max_spread = knobs.mesh_max_height_spread_m
    for i in range(ni):
        for j in range(nj):
            if not walk[i, j]:
                continue
            corners = (
                (i, j),
                (i + 1, j),
                (i + 1, j + 1),
                (i, j + 1),
            )
            ys = [corner_h[c] for c in corners]
            if any(np.isnan(y) for y in ys):
                stats["cells_skipped_bad_corner"] += 1
                continue
            if float(max(ys) - min(ys)) > max_spread:
                stats["cells_skipped_height_spread"] += 1
                continue
            # Also reject if a 4-neighbor walkable cell differs by more than spread
            # (avoids steep shared edges toward shelves)
            steep = False
            for di, dj in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                ni2, nj2 = i + di, j + dj
                if ni2 < 0 or ni2 >= ni or nj2 < 0 or nj2 >= nj:
                    continue
                if not walk[ni2, nj2]:
                    continue
                if abs(float(h_cell[i, j] - h_cell[ni2, nj2])) > max_spread:
                    steep = True
                    break
            if steep:
                stats["cells_skipped_height_spread"] += 1
                continue

            v0 = ensure_vert(*corners[0])
            v1 = ensure_vert(*corners[1])
            v2 = ensure_vert(*corners[2])
            v3 = ensure_vert(*corners[3])
            triangles.append([v0, v1, v2])
            triangles.append([v0, v2, v3])
            stats["cells_meshed"] += 1

    mesh = o3d.geometry.TriangleMesh()
    if not vertices:
        mesh.vertices = o3d.utility.Vector3dVector(np.zeros((0, 3)))
        mesh.triangles = o3d.utility.Vector3iVector(np.zeros((0, 3), dtype=np.int32))
    else:
        mesh.vertices = o3d.utility.Vector3dVector(np.asarray(vertices, dtype=np.float64))
        mesh.triangles = o3d.utility.Vector3iVector(np.asarray(triangles, dtype=np.int32))
        mesh.remove_duplicated_vertices()
        mesh.remove_degenerate_triangles()
        mesh.remove_unreferenced_vertices()
        stats["vertices_before_voxel"] = int(len(mesh.vertices))
        stats["triangles_before_voxel"] = int(len(mesh.triangles))
        if knobs.mesh_voxel_downsample_scale > 0 and len(mesh.triangles) > 0:
            voxel = float(cell) * float(knobs.mesh_voxel_downsample_scale)
            mesh = mesh.simplify_vertex_clustering(
                voxel_size=voxel,
                contraction=o3d.geometry.SimplificationContraction.Average,
            )
            mesh.remove_degenerate_triangles()
            mesh.remove_unreferenced_vertices()
            stats["voxel_size_m"] = voxel
        if knobs.mesh_smooth_iters > 0 and len(mesh.triangles) > 0:
            mesh = mesh.filter_smooth_taubin(number_of_iterations=int(knobs.mesh_smooth_iters))
        mesh.compute_vertex_normals()
    o3d.io.write_triangle_mesh(str(out_path), mesh)
    stats["vertices"] = int(len(mesh.vertices))
    stats["triangles"] = int(len(mesh.triangles))
    return stats["triangles"], stats["vertices"], stats


def grid_to_image_rgb(layer_rgb: np.ndarray) -> Image.Image:
    """Grid (ni=X, nj=Z) → image with +Z up; fliplr so X matches PLY viewer."""
    img = np.flipud(np.transpose(layer_rgb, (1, 0, 2)))
    return Image.fromarray(np.fliplr(img), mode="RGB")


def save_overlay_png(path: Path, layer: np.ndarray, color: tuple[int, int, int]) -> None:
    ni, nj = layer.shape
    rgb = np.zeros((ni, nj, 3), dtype=np.uint8)
    rgb[layer.astype(bool)] = color
    grid_to_image_rgb(rgb).save(path)


def save_combined_overlay(path: Path, layers: dict[str, np.ndarray], colors: dict[str, tuple[int, int, int]]) -> None:
    ni, nj = next(iter(layers.values())).shape
    rgb = np.zeros((ni, nj, 3), dtype=np.uint8)
    order = [
        "evidence",
        "walkable",
        "holes_filled",
        "gaps_filled",
        "seeds",
        "blockers_ignored",
        "blockers",
    ]
    for name in order:
        if name not in layers or name not in colors:
            continue
        rgb[layers[name].astype(bool)] = colors[name]
    grid_to_image_rgb(rgb).save(path)


def run(args: argparse.Namespace) -> dict:
    knobs = Knobs(
        grid_cell_m=args.grid_cell,
        up_normal_max_deg=args.up_normal_deg,
        floor_y_min=args.floor_y_min,
        floor_y_max=args.floor_y_max,
        seed_dilate_m=args.seed_dilate,
        max_hole_area_m2=args.max_hole_area,
        max_hole_extent_m=args.max_hole_extent,
        hole_normal_max_deg=args.hole_normal_deg,
        hole_planar_rms_m=args.hole_planar_rms,
        smooth_simple_iters=args.smooth_iters,
        min_blocker_component_cells=args.min_blocker_cells,
        traj_ignore_radius_m=args.traj_ignore_radius,
        traj_ignore_height_m=args.traj_ignore_height,
        gap_corridor_radius_m=args.gap_corridor_radius,
        gap_max_dist_from_evidence_m=args.gap_max_dist_evidence,
        gap_max_width_m=args.gap_max_width,
        gap_max_length_m=args.gap_max_length,
        gap_max_area_m2=args.gap_max_area,
        gap_opposite_cone_deg=args.gap_opposite_cone,
        mesh_fill_hole_cells=args.mesh_fill_holes,
        mesh_thin_hole_max_area=args.mesh_thin_hole_max_area,
        mesh_thin_hole_max_width=args.mesh_thin_hole_max_width,
        mesh_min_island_cells=args.mesh_min_island,
        mesh_max_height_spread_m=args.mesh_max_height_spread,
        mesh_flatten_radius_m=args.mesh_flatten_radius,
        mesh_flatten_outlier_m=args.mesh_flatten_outlier,
        mesh_flatten_min_samples=args.mesh_flatten_min_samples,
        mesh_flatten_iters=args.mesh_flatten_iters,
        mesh_voxel_downsample_scale=args.mesh_voxel_scale,
        mesh_smooth_iters=args.mesh_smooth_iters,
    )
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    mesh_path = Path(args.mesh)
    traj_path = Path(args.traj)

    mesh = o3d.io.read_triangle_mesh(str(mesh_path))
    if mesh.is_empty():
        raise RuntimeError(f"Empty mesh: {mesh_path}")

    vertices = np.asarray(mesh.vertices).copy()
    triangles = np.asarray(mesh.triangles)
    traj, traj_meta = load_trajectory_positions(traj_path)

    # Gallery PLYs stay COLMAP +X-up on disk; traj_global_aligned is already Y-up.
    # Viewer remaps at display time — spike must remap mesh the same way.
    mesh_remapped = False
    traj_frame = str(traj_meta.get("frame", ""))
    traj_up = str(traj_meta.get("up_axis", "")).lower()
    if traj_up == "y" or "y_up" in traj_frame or traj_meta.get("axis_remap") == "colmap_x_up_to_y_up":
        if mesh_looks_x_up(vertices):
            vertices = remap_colmap_x_up_to_y_up(vertices)
            mesh_remapped = True
            mesh.vertices = o3d.utility.Vector3dVector(vertices)

    if knobs.smooth_simple_iters > 0:
        mesh = mesh.filter_smooth_taubin(number_of_iterations=int(knobs.smooth_simple_iters))
        mesh.compute_vertex_normals()
        vertices = np.asarray(mesh.vertices)
        triangles = np.asarray(mesh.triangles)

    mesh.compute_triangle_normals()
    normals = np.asarray(mesh.triangle_normals)
    centroids = triangle_centroids(vertices, triangles)

    floor_tri, wall_tri, snap_y = classify_floor_and_walls(centroids, normals, knobs)
    traj_xz = traj[:, [0, 2]]

    bounds, _, _ = build_grid_bounds(vertices, traj_xz, knobs.grid_pad_m)
    ni, nj = grid_shape(bounds, knobs.grid_cell_m)

    evidence, height_avg = stamp_triangles_fast(
        vertices, triangles, floor_tri, bounds, knobs.grid_cell_m, ni, nj
    )
    blockers_raw, blocker_height = stamp_triangles_fast(
        vertices, triangles, wall_tri, bounds, knobs.grid_cell_m, ni, nj
    )
    # blockers should not erase evidence
    blockers_raw = blockers_raw & ~evidence
    blockers, blocker_cc_stats = remove_small_components(
        blockers_raw, knobs.min_blocker_component_cells
    )
    # Clear heights on removed outlier cells
    blocker_height = np.where(blockers.astype(bool), blocker_height, np.nan)

    ignored_near_traj, traj_ignore_stats = blockers_ignored_near_trajectory(
        blockers,
        blocker_height,
        traj,
        bounds,
        knobs.grid_cell_m,
        knobs.traj_ignore_radius_m,
        knobs.traj_ignore_height_m,
    )
    # Hard blockers for hole fill / walkable cut; feet-people corridor ignored
    blockers_hard = (blockers.astype(bool) & ~ignored_near_traj.astype(bool)).astype(np.uint8)

    seed_grid = np.zeros((ni, nj), dtype=np.uint8)
    ti, tj = xz_to_ij(traj[:, 0], traj[:, 2], bounds, knobs.grid_cell_m)
    valid = (ti >= 0) & (ti < ni) & (tj >= 0) & (tj < nj)
    seed_grid[ti[valid], tj[valid]] = 1
    radius_cells = int(math.ceil(knobs.seed_dilate_m / knobs.grid_cell_m))
    seeds = dilate_disk(seed_grid, radius_cells).astype(np.uint8)
    seeds = seeds & ~blockers_hard

    walkable, holes_filled, hole_stats = close_planar_enclosed_holes(
        evidence, height_avg, blockers_hard, bounds, knobs.grid_cell_m, knobs
    )
    walkable, gaps_filled, gap_stats = fill_traj_open_gaps(
        walkable,
        evidence,
        height_avg,
        blockers_hard,
        traj,
        bounds,
        knobs.grid_cell_m,
        knobs,
    )
    walkable_mesh, height_for_mesh, mesh_clean_stats = clean_walkable_for_mesh(
        walkable, height_avg, knobs
    )
    height_flat, mesh_flatten_stats = soft_flatten_heights_median(
        height_for_mesh, walkable_mesh, knobs.grid_cell_m, knobs
    )

    # Floor mesh for viewer + Recast input: thin holes + median flatten
    nav_ply = out_dir / "nav_floor.ply"
    n_tris, n_verts, mesh_build_stats = export_contiguous_floor_mesh(
        walkable_mesh,
        bounds,
        knobs.grid_cell_m,
        snap_y,
        height_flat,
        nav_ply,
        knobs,
    )

    colors = {
        "evidence": (80, 180, 80),
        "holes_filled": (0, 220, 200),
        "gaps_filled": (255, 170, 60),
        "blockers": (200, 60, 60),
        "blockers_ignored": (220, 120, 255),
        "seeds": (240, 220, 60),
        "walkable": (80, 140, 240),
        "walkable_mesh": (120, 200, 255),
    }
    save_overlay_png(out_dir / "overlay_evidence.png", evidence, colors["evidence"])
    save_overlay_png(out_dir / "overlay_holes_filled.png", holes_filled, colors["holes_filled"])
    save_overlay_png(out_dir / "overlay_gaps_filled.png", gaps_filled, colors["gaps_filled"])
    save_overlay_png(out_dir / "overlay_blockers_raw.png", blockers_raw, colors["blockers"])
    save_overlay_png(out_dir / "overlay_blockers.png", blockers_hard, colors["blockers"])
    save_overlay_png(
        out_dir / "overlay_blockers_ignored_traj.png",
        ignored_near_traj,
        colors["blockers_ignored"],
    )
    save_overlay_png(out_dir / "overlay_seeds.png", seeds, colors["seeds"])
    save_overlay_png(out_dir / "overlay_walkable.png", walkable, colors["walkable"])
    save_overlay_png(out_dir / "overlay_walkable_mesh.png", walkable_mesh, colors["walkable_mesh"])
    save_combined_overlay(
        out_dir / "overlay_all.png",
        {
            "evidence": evidence,
            "walkable": walkable_mesh,
            "holes_filled": holes_filled,
            "gaps_filled": gaps_filled,
            "seeds": seeds,
            "blockers_ignored": ignored_near_traj,
            "blockers": blockers_hard,
        },
        colors,
    )

    summary = {
        "mesh": str(mesh_path.resolve()),
        "trajectory": str(traj_path.resolve()),
        "out_dir": str(out_dir.resolve()),
        "up_axis": "Y",
        "mesh_remapped_x_up_to_y_up": mesh_remapped,
        "traj_frame": traj_frame,
        "knobs": asdict(knobs),
        "snap_floor_y": snap_y,
        "grid": {"ni": ni, "nj": nj, "bounds_xz": bounds.tolist(), "cell_m": knobs.grid_cell_m},
        "counts": {
            "mesh_vertices": int(len(vertices)),
            "mesh_triangles": int(len(triangles)),
            "floor_triangles": int(floor_tri.sum()),
            "wall_triangles": int(wall_tri.sum()),
            "evidence_cells": int(evidence.sum()),
            "blocker_cells_raw": int(blockers_raw.sum()),
            "blocker_cells_after_cc": int(blockers.sum()),
            "blocker_cells_hard": int(blockers_hard.sum()),
            "blocker_cells_ignored_traj": int(ignored_near_traj.sum()),
            "seed_cells": int(seeds.sum()),
            "holes_filled_cells": int(holes_filled.sum()),
            "gaps_filled_cells": int(gaps_filled.sum()),
            "walkable_cells": int(walkable.sum()),
            "walkable_mesh_cells": int(walkable_mesh.sum()),
            "nav_floor_vertices": n_verts,
            "nav_floor_triangles": n_tris,
        },
        "blocker_outlier_filter": blocker_cc_stats,
        "blocker_traj_ignore": traj_ignore_stats,
        "hole_close": hole_stats,
        "gap_fill": gap_stats,
        "mesh_clean": mesh_clean_stats,
        "mesh_flatten": mesh_flatten_stats,
        "mesh_build": mesh_build_stats,
        "fill_policy": "enclosed_holes_plus_traj_open_planar_gaps",
        "outputs": {
            "nav_floor_ply": str(nav_ply.resolve()),
            "overlays": [
                str((out_dir / n).resolve())
                for n in (
                    "overlay_evidence.png",
                    "overlay_holes_filled.png",
                    "overlay_gaps_filled.png",
                    "overlay_blockers_raw.png",
                    "overlay_blockers.png",
                    "overlay_blockers_ignored_traj.png",
                    "overlay_seeds.png",
                    "overlay_walkable.png",
                    "overlay_walkable_mesh.png",
                    "overlay_all.png",
                )
            ],
        },
    }
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> int:
    default_root = Path(__file__).resolve().parent
    default_mesh = (
        Path(r"D:\agent-workspaces\posemesh-harness\tmp\mono_depth_ux_v2\meshes")
        / "da3_global_stronger_carve_colored_nofloaters.ply"
    )
    default_traj = default_mesh.parent / "traj_global_aligned.json"

    p = argparse.ArgumentParser(description="Grocery navmesh offline spike")
    p.add_argument("--mesh", type=Path, default=default_mesh)
    p.add_argument("--traj", type=Path, default=default_traj)
    p.add_argument("--out-dir", type=Path, default=default_root)
    p.add_argument("--grid-cell", type=float, default=0.05)
    p.add_argument("--up-normal-deg", type=float, default=30.0)
    p.add_argument("--floor-y-min", type=float, default=-0.15)
    p.add_argument("--floor-y-max", type=float, default=0.35)
    p.add_argument("--seed-dilate", type=float, default=0.50, help="Traj overlay only")
    p.add_argument("--max-hole-area", type=float, default=16.0, help="Max enclosed hole area m^2")
    p.add_argument("--max-hole-extent", type=float, default=4.0, help="Max hole bbox extent m")
    p.add_argument("--hole-normal-deg", type=float, default=25.0, help="Max plane tilt from +Y")
    p.add_argument(
        "--gap-opposite-cone",
        type=float,
        default=90.0,
        help="Degrees off pure opposite (±across); 90≈half-plane (looser)",
    )
    p.add_argument("--hole-planar-rms", type=float, default=0.06, help="Max rim/fill plane RMS m")
    p.add_argument("--smooth-iters", type=int, default=2, help="Open3D Taubin smooth iters (input mesh)")
    p.add_argument(
        "--min-blocker-cells",
        type=int,
        default=16,
        help="Drop blocker connected components smaller than this",
    )
    p.add_argument(
        "--traj-ignore-radius",
        type=float,
        default=0.30,
        help="Ignore blockers within this XZ radius of camera traj (m)",
    )
    p.add_argument(
        "--traj-ignore-height",
        type=float,
        default=1.50,
        help="Ignore blockers if |y - cam_y| within this (m)",
    )
    p.add_argument("--gap-corridor-radius", type=float, default=0.75)
    p.add_argument(
        "--gap-max-dist-evidence",
        type=float,
        default=4.0,
        help="Max XZ distance to nearest floor evidence when filling traj gaps (m)",
    )
    p.add_argument("--gap-max-width", type=float, default=4.0, help="Max gap width (min bbox side)")
    p.add_argument("--gap-max-length", type=float, default=4.0, help="Max gap length along aisle")
    p.add_argument("--gap-max-area", type=float, default=16.0)
    p.add_argument(
        "--mesh-fill-holes",
        type=int,
        default=0,
        help="Legacy binary-close iters (0=off; thin enclosed-hole rule is preferred)",
    )
    p.add_argument(
        "--mesh-thin-hole-max-area",
        type=int,
        default=4,
        help="Fill enclosed pre-mesh holes with fewer than this many cells (4 ⇒ 1×3 ok)",
    )
    p.add_argument(
        "--mesh-thin-hole-max-width",
        type=int,
        default=1,
        help="Fill only if min(bbox width, height) <= this (cells)",
    )
    p.add_argument("--mesh-min-island", type=int, default=12, help="Drop walkable islands smaller than this")
    p.add_argument(
        "--mesh-max-height-spread",
        type=float,
        default=0.30,
        help="Max corner/neighbor height spread to keep a cell in the floor mesh",
    )
    p.add_argument(
        "--mesh-flatten-radius",
        type=float,
        default=0.55,
        help="Soft-flatten median neighborhood radius (m); 0 disables",
    )
    p.add_argument(
        "--mesh-flatten-outlier",
        type=float,
        default=0.06,
        help="Only median neighbors within this |Δy| of the cell (m) — skips shelf edges",
    )
    p.add_argument(
        "--mesh-flatten-min-samples",
        type=int,
        default=5,
        help="Min same-surface neighbors to apply median flatten",
    )
    p.add_argument(
        "--mesh-flatten-iters",
        type=int,
        default=2,
        help="Repeat median soft-flatten this many times",
    )
    p.add_argument(
        "--mesh-voxel-scale",
        type=float,
        default=1.0,
        help="Vertex-clustering voxel size as multiple of grid cell (1=keep 5cm cells)",
    )
    p.add_argument("--mesh-smooth-iters", type=int, default=2, help="Taubin smooth on nav floor mesh")
    args = p.parse_args()

    try:
        summary = run(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(summary["counts"], indent=2))
    print(f"Wrote summary to {args.out_dir / 'summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
