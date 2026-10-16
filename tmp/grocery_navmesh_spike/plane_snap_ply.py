#!/usr/bin/env python3
"""Median-plane snap on a triangle mesh PLY (hard ≤ snap_m, smoothstep ramp to outer_m)."""

from __future__ import annotations

import argparse
import json
import sys

import numpy as np
import open3d as o3d


def fit_median_plane(points: np.ndarray) -> tuple[np.ndarray, float]:
    pts = np.asarray(points, dtype=np.float64)
    if len(pts) < 3:
        return np.array([0.0, 1.0, 0.0]), 0.0
    center = np.median(pts, axis=0)
    _, _, vt = np.linalg.svd(pts - center, full_matrices=False)
    n = vt[-1].astype(np.float64)
    n /= max(float(np.linalg.norm(n)), 1e-12)
    if n[1] < 0:
        n = -n
    c = float(np.median(pts @ n))
    return n, c


def pull_factor(abs_dist: np.ndarray, snap_m: float, outer_m: float) -> np.ndarray:
    factor = np.ones_like(abs_dist, dtype=np.float64)
    if outer_m <= snap_m:
        factor[abs_dist > snap_m] = 0.0
        return factor
    mid = (abs_dist > snap_m) & (abs_dist < outer_m)
    t = (abs_dist[mid] - snap_m) / (outer_m - snap_m)
    s = t * t * (3.0 - 2.0 * t)
    factor[mid] = 1.0 - s
    factor[abs_dist >= outer_m] = 0.0
    return factor


def plane_snap_mesh(
    mesh: o3d.geometry.TriangleMesh,
    snap_m: float,
    outer_m: float,
) -> tuple[o3d.geometry.TriangleMesh, dict]:
    stats: dict = {
        "snap_m": float(snap_m),
        "outer_m": float(outer_m),
        "ramp": "smoothstep",
        "vertices_in": int(len(mesh.vertices)),
        "triangles_in": int(len(mesh.triangles)),
    }
    if mesh.is_empty() or len(mesh.vertices) == 0:
        return mesh, stats

    verts = np.asarray(mesh.vertices, dtype=np.float64)
    n, c = fit_median_plane(verts)
    dist = verts @ n - c
    abs_d = np.abs(dist)
    factor = pull_factor(abs_d, float(snap_m), float(outer_m))
    out = verts - (factor * dist)[:, None] * n[None, :]
    mesh.vertices = o3d.utility.Vector3dVector(out)
    mesh.compute_vertex_normals()

    hard = abs_d <= snap_m
    ramp = (abs_d > snap_m) & (abs_d < outer_m)
    stats["plane_normal"] = n.tolist()
    stats["plane_offset"] = c
    stats["vertices_hard_snap"] = int(hard.sum())
    stats["vertices_ramped"] = int(ramp.sum())
    stats["vertices_untouched"] = int((factor <= 0).sum())
    stats["factor_mean_ramped"] = float(factor[ramp].mean()) if ramp.any() else 0.0
    stats["dist_abs_mean_before"] = float(np.mean(abs_d))
    stats["dist_abs_p95_before"] = float(np.percentile(abs_d, 95))
    dist_after = np.abs(out @ n - c)
    stats["dist_abs_mean_after"] = float(np.mean(dist_after))
    stats["dist_abs_p95_after"] = float(np.percentile(dist_after, 95))
    stats["vertices_out"] = int(len(mesh.vertices))
    stats["triangles_out"] = int(len(mesh.triangles))
    return mesh, stats


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("ply", type=str, help="Input/output PLY path (overwritten)")
    p.add_argument("--snap", type=float, default=0.05, help="Hard snap radius (m)")
    p.add_argument("--outer", type=float, default=0.15, help="Ramp outer radius (m)")
    p.add_argument("--stats-json", action="store_true", help="Print stats JSON to stdout")
    args = p.parse_args()

    mesh = o3d.io.read_triangle_mesh(args.ply)
    if mesh.is_empty():
        print(f"ERROR: empty mesh {args.ply}", file=sys.stderr)
        return 1
    mesh, stats = plane_snap_mesh(mesh, args.snap, args.outer)
    o3d.io.write_triangle_mesh(args.ply, mesh)
    if args.stats_json:
        print(json.dumps(stats))
    else:
        print(
            f"plane-snap {args.ply}: hard={stats['vertices_hard_snap']} "
            f"ramp={stats['vertices_ramped']} untouched={stats['vertices_untouched']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
