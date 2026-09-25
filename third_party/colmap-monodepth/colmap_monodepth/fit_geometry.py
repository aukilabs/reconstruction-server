"""Geometry helpers for joint depth fitting (numpy; no torch)."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any, Sequence

import numpy as np

from colmap_monodepth.colmap_io import Camera, Image, Point3D
from colmap_monodepth.types import FitConfig, camera_center_from_w2c

if TYPE_CHECKING:
    pass


def fit_scale_np(pred: np.ndarray, gt: np.ndarray, trim: float = 0.2, min_n: int = 30) -> float:
    """Robust median scale ratio gt/pred with trimmed quantiles."""
    eps = 1e-6
    ok = np.isfinite(pred) & np.isfinite(gt) & (pred > eps) & (gt > eps)
    if np.count_nonzero(ok) < min_n:
        return float("nan")
    ratios = gt[ok] / pred[ok]
    lo, hi = np.quantile(ratios, trim), np.quantile(ratios, 1.0 - trim)
    ratios = ratios[(ratios > lo) & (ratios < hi)]
    if ratios.size < min_n:
        return float("nan")
    return float(np.median(ratios))


def colmap_anchors(image: Image, points3d: dict[int, Point3D]) -> tuple[np.ndarray, np.ndarray]:
    """2D feature locations and camera-space Z for COLMAP triangulated points."""
    R = image.qvec2rotmat()
    t = image.tvec
    xys_list: list[np.ndarray] = []
    zs_list: list[float] = []
    for xy, pid in zip(image.xys, image.point3D_ids):
        pid = int(pid)
        if pid < 0 or pid not in points3d:
            continue
        Xw = points3d[pid].xyz
        Xc = R @ Xw + t
        xys_list.append(np.asarray(xy, dtype=np.float64))
        zs_list.append(float(Xc[2]))
    if not xys_list:
        return np.zeros((0, 2)), np.zeros((0,))
    return np.stack(xys_list, 0), np.asarray(zs_list, dtype=np.float32)


def collect_track_obs(
    image_names: Sequence[str],
    name_to_image: dict[str, Image],
    cameras: dict[int, Camera],
    points3d: dict[int, Point3D],
    depth_hw: tuple[int, int],
) -> dict[int, list[tuple[int, float, float]]]:
    """Map point3D id → unique (view_idx, u, v) in depth pixel coordinates."""
    obs: dict[int, list[tuple[int, float, float]]] = {}
    H, W = depth_hw
    for i, name in enumerate(image_names):
        im = name_to_image[name]
        cam = cameras[im.camera_id]
        sx = W / float(cam.width)
        sy = H / float(cam.height)
        seen: set[int] = set()
        for xy, pid in zip(im.xys, im.point3D_ids):
            pid = int(pid)
            if pid < 0 or pid not in points3d or pid in seen:
                continue
            seen.add(pid)
            xy_arr = np.asarray(xy, dtype=np.float64)
            obs.setdefault(pid, []).append((i, float(xy_arr[0] * sx), float(xy_arr[1] * sy)))
    return obs


def _pair_key(i: int, j: int) -> tuple[int, int]:
    return (i, j) if i < j else (j, i)


def _triangulation_angle_deg(
    ci: np.ndarray,
    cj: np.ndarray,
    fi: np.ndarray,
    fj: np.ndarray,
) -> float:
    """Approx triangulation angle (deg) at a proxy scene point ahead of both cameras."""
    fwd = 0.5 * (fi + fj)
    norm = float(np.linalg.norm(fwd))
    if norm < 1e-9:
        fwd = np.array([0.0, 0.0, 1.0], dtype=np.float64)
    else:
        fwd = fwd / norm
    baseline = float(np.linalg.norm(ci - cj))
    depth = max(baseline, 0.5)
    p = 0.5 * (ci + cj) + fwd * depth
    v1 = ci - p
    v2 = cj - p
    n1 = float(np.linalg.norm(v1))
    n2 = float(np.linalg.norm(v2))
    if n1 < 1e-9 or n2 < 1e-9:
        return 0.0
    cos_a = float(np.dot(v1, v2) / (n1 * n2))
    cos_a = float(np.clip(cos_a, -1.0, 1.0))
    return float(np.degrees(np.arccos(cos_a)))


def _angle_band_score(angle_deg: float, lo: float, hi: float) -> float:
    """1 inside [lo, hi]; linear falloff to 0 by 0° / 2*hi."""
    if lo <= angle_deg <= hi:
        return 1.0
    if angle_deg < lo:
        return float(np.clip(angle_deg / max(lo, 1e-6), 0.0, 1.0))
    # above hi: decay to 0 by 2*hi
    return float(np.clip(1.0 - (angle_deg - hi) / max(hi, 1e-6), 0.0, 1.0))


def build_geo_pairs(
    n: int,
    centers: np.ndarray,
    obs: dict[int, list[tuple[int, float, float]]],
    config: FitConfig,
    *,
    forwards: np.ndarray | None = None,
    timestamps: np.ndarray | None = None,
) -> tuple[list[tuple[int, int]], dict[str, Any]]:
    """Build geo neighbor pairs for the multi-view depth consistency loss.

    Always computes spatial / temporal / covis candidate sets for ``pair_stats``.
    Cost list mode (``FitConfig.geo_cost_mode``):

    - ``temporal``: temporal neighbors only
    - ``union``: capped spatial ∪ temporal ∪ covis (legacy)
    - ``temporal_plus_ranked`` (default): all temporal + ranked wide/revisit covis
      pairs (prefer mid triangulation angle and larger time gaps)

    ``geo_cost_temporal_only=True`` forces ``temporal`` (back-compat).
    """
    spatial: list[tuple[int, int]] = []
    for i in range(n):
        d = np.linalg.norm(centers - centers[i], axis=1)
        d[i] = 1e9
        added = 0
        for j in np.argsort(d):
            if d[j] > config.geo_max_baseline_m:
                break
            if i < int(j):
                spatial.append((i, int(j)))
            added += 1
            if added >= config.geo_max_pairs_per_view:
                break

    temporal: list[tuple[int, int]] = []
    for i in range(n):
        for k in range(1, config.temporal_radius + 1):
            j = i + k
            if j < n:
                temporal.append((i, j))

    shared: dict[tuple[int, int], int] = defaultdict(int)
    for lst in obs.values():
        views = [v for v, _, _ in lst]
        if len(views) < 2:
            continue
        for a in range(len(views)):
            for b in range(a + 1, len(views)):
                va, vb = views[a], views[b]
                shared[_pair_key(va, vb)] += 1

    neigh: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for (a, b), c in shared.items():
        if c < config.covis_min_shared:
            continue
        neigh[a].append((c, b))
        neigh[b].append((c, a))
    covis_weight: dict[tuple[int, int], int] = {}
    for i, lst in neigh.items():
        lst.sort(reverse=True)
        for c, j in lst[: config.covis_max_per_view]:
            pair = _pair_key(i, j)
            covis_weight[pair] = max(covis_weight.get(pair, 0), c)

    # Full candidate union (temporal first, then covis by weight, then spatial).
    report_union = set(temporal)
    for pair, _c in sorted(covis_weight.items(), key=lambda kv: -kv[1]):
        if len(report_union) >= config.max_geo_pairs:
            break
        report_union.add(pair)
    for pair in spatial:
        if len(report_union) >= config.max_geo_pairs:
            break
        report_union.add(pair)

    temporal_set = set(temporal)
    if config.geo_cost_temporal_only:
        cost_mode = "temporal"
    else:
        cost_mode = str(getattr(config, "geo_cost_mode", "temporal_plus_ranked") or "temporal_plus_ranked")

    if cost_mode == "temporal":
        cost_pairs = sorted(temporal_set)
    elif cost_mode == "union":
        cost_pairs = sorted(report_union)
    else:
        # temporal_plus_ranked
        if forwards is None:
            forwards = np.zeros((n, 3), dtype=np.float64)
            forwards[:, 2] = 1.0
        else:
            forwards = np.asarray(forwards, dtype=np.float64)
            if forwards.shape != (n, 3):
                raise ValueError(f"forwards must be ({n}, 3), got {forwards.shape}")

        if timestamps is not None:
            timestamps = np.asarray(timestamps, dtype=np.float64)
            if timestamps.shape != (n,):
                raise ValueError(f"timestamps must be ({n},), got {timestamps.shape}")

        scored: list[tuple[float, tuple[int, int]]] = []
        for (i, j), shared_n in covis_weight.items():
            if (i, j) in temporal_set:
                continue
            baseline = float(np.linalg.norm(centers[i] - centers[j]))
            if baseline > config.geo_max_baseline_m or baseline < 1e-6:
                continue
            angle = _triangulation_angle_deg(
                centers[i], centers[j], forwards[i], forwards[j]
            )
            angle_s = _angle_band_score(
                angle, config.geo_wide_min_angle_deg, config.geo_wide_max_angle_deg
            )
            if timestamps is not None:
                dt = abs(float(timestamps[i] - timestamps[j]))
            else:
                dt = abs(i - j) * float(config.geo_index_dt_s)
            time_s = 1.0 - float(np.exp(-dt / max(config.geo_wide_time_tau_s, 1e-6)))
            score = angle_s * time_s * float(np.log1p(shared_n))
            if score <= 0.0:
                continue
            scored.append((score, (i, j)))

        scored.sort(key=lambda t: -t[0])
        cost_set = set(temporal_set)
        budget = int(config.geo_wide_pairs_budget)
        for _score, pair in scored:
            if len(cost_set) >= config.max_geo_pairs:
                break
            if budget <= 0:
                break
            if pair in cost_set:
                continue
            cost_set.add(pair)
            budget -= 1
        cost_pairs = sorted(cost_set)
        cost_mode = "temporal_plus_ranked"

    stats = {
        "spatial": len(set(spatial)),
        "temporal": len(temporal_set),
        "covis": len(covis_weight),
        "union": len(report_union),
        "cost": len(cost_pairs),
        "cost_mode": cost_mode,
    }
    return cost_pairs, stats


def pack_track_pairs(obs: dict[int, list[tuple[int, float, float]]], config: FitConfig):
    """Sample multi-view tracks into per-(i,j) pixel coordinate lists."""
    rng = np.random.default_rng(0)
    pids = [pid for pid, lst in obs.items() if len(lst) >= 2]
    if len(pids) > config.track_max_points:
        pick = rng.choice(len(pids), config.track_max_points, replace=False)
        pids = [pids[int(i)] for i in pick]
    buckets: dict[tuple[int, int], list[list]] = defaultdict(lambda: [[], []])
    n_tracks = 0
    for pid in pids:
        lst = obs[pid][: config.track_max_views]
        if len(lst) < 2:
            continue
        n_tracks += 1
        for a in range(len(lst)):
            for b in range(a + 1, len(lst)):
                ia, ua, va = lst[a]
                ib, ub, vb = lst[b]
                if ia > ib:
                    ia, ua, va, ib, ub, vb = ib, ub, vb, ia, ua, va
                buckets[(ia, ib)][0].append((ua, va))
                buckets[(ia, ib)][1].append((ub, vb))
    packed = [(i, j, xyi, xyj) for (i, j), (xyi, xyj) in buckets.items()]
    return packed, n_tracks


def centers_from_frames(extrinsics_w2c: np.ndarray) -> np.ndarray:
    """World-space camera centers for each view."""
    return np.stack([camera_center_from_w2c(T) for T in extrinsics_w2c], axis=0)


def forwards_from_w2c(extrinsics_w2c: np.ndarray) -> np.ndarray:
    """World-space camera +Z (optical axis) for each w2c pose."""
    out = np.zeros((len(extrinsics_w2c), 3), dtype=np.float64)
    for i, T in enumerate(extrinsics_w2c):
        R = T[:3, :3]
        out[i] = R.T @ np.array([0.0, 0.0, 1.0], dtype=np.float64)
    return out
