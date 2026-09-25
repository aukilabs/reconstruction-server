"""Joint scale + residual depth fit with ICP rematch and confident temporal mask."""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np

try:
    import torch
    import torch.nn.functional as F
except ImportError:  # pragma: no cover
    torch = None  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

from colmap_monodepth.colmap_io import Camera, Image, Point3D, read_model, resolve_sparse_dir
from colmap_monodepth.depth_io import depth_png_path, load_depth_folder, save_depth_folder
from colmap_monodepth.fit_geometry import (
    build_geo_pairs,
    centers_from_frames,
    colmap_anchors,
    collect_track_obs,
    fit_scale_np,
    forwards_from_w2c,
    pack_track_pairs,
)
from colmap_monodepth.types import (
    FitConfig,
    FitResult,
    FrameSet,
    frameset_from_colmap,
    scale_intrinsics_for_depth,
)


def _resolve_device(device: str):
    if torch is None:
        raise ImportError("torch is required for run_fit; install with: pip install colmap-monodepth[fit]")
    if device == "auto":
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")
    return torch.device(device)


def _huber(x, delta: float = 0.05):
    ax = x.abs()
    quad = torch.clamp(ax, max=delta)
    lin = ax - quad
    return 0.5 * quad * quad / delta + lin


class JointDepthFitter:
    """Joint per-view scale (+ optional residual grid) with COLMAP / geo / track losses."""

    def __init__(
        self,
        frames: FrameSet,
        depths: dict[str, np.ndarray],
        images: dict[str, Image],
        cameras: dict[int, Camera],
        points3d: dict[int, Point3D],
        config: FitConfig,
        device,
    ) -> None:
        if torch is None or F is None:
            raise ImportError("torch is required for JointDepthFitter")
        self.frames = frames
        self.config = config
        self.device = device
        self.selected = list(frames.image_names)
        self.n = len(self.selected)

        depth_stack = []
        Ks = []
        Ts = []
        full_hw = []
        anchors_xy = []
        anchors_z = []

        for i, name in enumerate(self.selected):
            im = images[name]
            cam = cameras[im.camera_id]
            wh = (int(cam.width), int(cam.height))
            d = depths[name].astype(np.float32)
            full_hw.append(d.shape)
            depth_stack.append(d)
            K_full = frames.intrinsics[i]
            Ks.append(
                scale_intrinsics_for_depth(K_full, wh[0], wh[1], d.shape[1], d.shape[0])
            )
            Ts.append(frames.extrinsics_w2c[i])
            xy, z = colmap_anchors(im, points3d)
            sx = d.shape[1] / wh[0]
            sy = d.shape[0] / wh[1]
            if xy.size:
                xy = xy.copy()
                xy[:, 0] *= sx
                xy[:, 1] *= sy
            anchors_xy.append(xy)
            anchors_z.append(z)

        self.H, self.W = depth_stack[0].shape
        self.full_hw = full_hw
        self.depth = torch.from_numpy(np.stack(depth_stack, 0)).to(device)
        self.K = torch.from_numpy(np.stack(Ks, 0)).float().to(device)
        self.T = torch.from_numpy(np.stack(Ts, 0)).float().to(device)
        self.centers = centers_from_frames(frames.extrinsics_w2c)
        self.forwards = forwards_from_w2c(frames.extrinsics_w2c)

        zmin, zmax = config.zmin, config.zmax
        self.col_i: list[int] = []
        self.col_xy: list = []
        self.col_z: list = []
        for i, (xy, z) in enumerate(zip(anchors_xy, anchors_z)):
            if z.size < 30:
                continue
            pred = self.depth[i]
            xs = np.clip(np.round(xy[:, 0]).astype(np.int32), 0, self.W - 1)
            ys = np.clip(np.round(xy[:, 1]).astype(np.int32), 0, self.H - 1)
            p = pred[ys, xs].detach().cpu().numpy()
            ok = np.isfinite(p) & np.isfinite(z) & (p > 1e-4) & (z > zmin) & (z < zmax)
            if ok.sum() < 30:
                continue
            self.col_i.append(i)
            self.col_xy.append(torch.from_numpy(xy[ok].astype(np.float32)).to(device))
            self.col_z.append(torch.from_numpy(z[ok].astype(np.float32)).to(device))

        a_lo, a_hi = config.scale_clamp
        a0 = np.ones(self.n, np.float32)
        for i, (_xy, z) in enumerate(zip(anchors_xy, anchors_z)):
            xy = anchors_xy[i]
            if z.size < 30:
                continue
            xs = np.clip(np.round(xy[:, 0]).astype(np.int32), 0, self.W - 1)
            ys = np.clip(np.round(xy[:, 1]).astype(np.int32), 0, self.H - 1)
            pred = depth_stack[i][ys, xs]
            a = fit_scale_np(pred, z)
            if np.isfinite(a) and a > 0.05:
                a0[i] = float(np.clip(a, a_lo, a_hi))
        self.a = torch.nn.Parameter(torch.from_numpy(a0).to(device))
        self.R = None
        self.s = None

        self._geo_min_src = 50
        self._geo_min_front = 40
        self._geo_min_inb = 30
        self._geo_min_ok = 20

        us = torch.arange(0, self.W, config.geo_stride, device=device, dtype=torch.float32)
        vs = torch.arange(0, self.H, config.geo_stride, device=device, dtype=torch.float32)
        uu, vv = torch.meshgrid(us, vs, indexing="xy")
        self.sample_uv = torch.stack([uu.reshape(-1), vv.reshape(-1)], 1)

        obs = collect_track_obs(self.selected, images, cameras, points3d, (self.H, self.W))
        pairs, pair_stats = build_geo_pairs(
            self.n,
            self.centers,
            obs,
            config,
            forwards=self.forwards,
        )
        self._set_geo_pairs(pairs)
        self.pair_stats = pair_stats
        track_pairs, n_tracks = pack_track_pairs(obs, config)
        self._set_track_pairs(track_pairs)
        self._n_tracks = n_tracks

    def enable_residual(self) -> None:
        cfg = self.config
        self.R = torch.nn.Parameter(
            torch.zeros(self.n, 1, cfg.res_gh, cfg.res_gw, device=self.device)
        )

    def _set_geo_pairs(self, pairs: list[tuple[int, int]]) -> None:
        self.pairs = pairs
        if not pairs:
            self.pair_i = torch.zeros(0, dtype=torch.long, device=self.device)
            self.pair_j = torch.zeros(0, dtype=torch.long, device=self.device)
            return
        self.pair_i = torch.tensor([p[0] for p in pairs], dtype=torch.long, device=self.device)
        self.pair_j = torch.tensor([p[1] for p in pairs], dtype=torch.long, device=self.device)

    def _set_track_pairs(self, packed: list) -> None:
        """Store track edges as padded tensors for batched loss (plus list for rematch export)."""
        self.track_pairs = packed
        if not packed:
            self.track_i = None
            self.track_j = None
            self.track_xyi = None
            self.track_xyj = None
            self.track_mask = None
            return
        lengths = [len(xyi) for _i, _j, xyi, _xyj in packed]
        m = max(lengths)
        p = len(packed)
        xyi_t = torch.zeros(p, m, 2, dtype=torch.float32, device=self.device)
        xyj_t = torch.zeros(p, m, 2, dtype=torch.float32, device=self.device)
        mask = torch.zeros(p, m, dtype=torch.bool, device=self.device)
        ii = torch.zeros(p, dtype=torch.long, device=self.device)
        jj = torch.zeros(p, dtype=torch.long, device=self.device)
        for e, (i, j, xyi, xyj) in enumerate(packed):
            n = len(xyi)
            xyi_t[e, :n] = torch.tensor(xyi, dtype=torch.float32, device=self.device)
            xyj_t[e, :n] = torch.tensor(xyj, dtype=torch.float32, device=self.device)
            mask[e, :n] = True
            ii[e] = int(i)
            jj[e] = int(j)
        self.track_i = ii
        self.track_j = jj
        self.track_xyi = xyi_t
        self.track_xyj = xyj_t
        self.track_mask = mask

    def depth_maps(self):
        a = self.a if self.s is None else (self.s[0] * self.a)
        d = a[:, None, None] * self.depth
        if self.R is not None:
            r = F.interpolate(
                self.R, size=(self.H, self.W), mode="bilinear", align_corners=True
            )
            d = d + r[:, 0]
        return d

    def sample_at(self, depth_i, xy):
        H, W = depth_i.shape
        grid = xy.clone()
        grid[:, 0] = grid[:, 0] / (W - 1) * 2 - 1
        grid[:, 1] = grid[:, 1] / (H - 1) * 2 - 1
        g = grid.view(1, 1, -1, 2)
        v = F.grid_sample(
            depth_i.view(1, 1, H, W), g, mode="bilinear", padding_mode="zeros", align_corners=True
        )
        return v.view(-1)

    def sample_batch(self, depth_b, xy_b):
        """Batched bilinear depth sample. depth_b: (B,H,W), xy_b: (B,S,2) → (B,S)."""
        b, h, w = depth_b.shape
        s = xy_b.shape[1]
        grid = xy_b.clone()
        grid[..., 0] = grid[..., 0] / (w - 1) * 2 - 1
        grid[..., 1] = grid[..., 1] / (h - 1) * 2 - 1
        v = F.grid_sample(
            depth_b[:, None],
            grid.view(b, 1, s, 2),
            mode="bilinear",
            padding_mode="zeros",
            align_corners=True,
        )
        return v.view(b, s)

    def loss_colmap(self, D):
        cfg = self.config
        losses = []
        for i, xy, z in zip(self.col_i, self.col_xy, self.col_z):
            pred = self.sample_at(D[i], xy)
            ok = (pred > cfg.zmin) & (pred < cfg.zmax) & (z > cfg.zmin) & (z < cfg.zmax)
            if ok.sum() < 10:
                continue
            losses.append(_huber(pred[ok] - z[ok], 0.08).mean())
        if not losses:
            return torch.zeros((), device=self.device)
        return torch.stack(losses).mean()

    def unproject_world(self, view: int, xy, z):
        K = self.K[view]
        T = self.T[view]
        pix = torch.stack([xy[:, 0] * z, xy[:, 1] * z, z], 0)
        Xc = torch.linalg.solve(K, pix)
        R, t = T[:3, :3], T[:3, 3]
        return R.T @ (Xc - t[:, None])

    def unproject_world_batch(self, K, T, xy, z):
        """K/T: (B,3,3)/(B,4,4), xy/z: (B,S,2)/(B,S) → world (B,3,S)."""
        pix = torch.stack([xy[..., 0] * z, xy[..., 1] * z, z], dim=1)
        xc = torch.linalg.solve(K, pix)
        r = T[:, :3, :3]
        t = T[:, :3, 3]
        return r.transpose(1, 2) @ (xc - t[:, :, None])

    def loss_track(self, D):
        cfg = self.config
        if self.track_i is None or self.track_i.numel() == 0:
            return torch.zeros((), device=self.device)
        i, j = self.track_i, self.track_j
        xyi, xyj, mask = self.track_xyi, self.track_xyj, self.track_mask
        di = self.sample_batch(D[i], xyi)
        dj = self.sample_batch(D[j], xyj)
        ok = mask & (di > cfg.zmin) & (di < cfg.zmax) & (dj > cfg.zmin) & (dj < cfg.zmax)
        n_ok = ok.sum(dim=1)
        edge_ok = n_ok >= 8
        if not edge_ok.any():
            return torch.zeros((), device=self.device)
        xwi = self.unproject_world_batch(self.K[i], self.T[i], xyi, di)
        xwj = self.unproject_world_batch(self.K[j], self.T[j], xyj, dj)
        dist = torch.linalg.norm(xwi - xwj, dim=1)
        h = _huber(dist, 0.05)
        h = h.masked_fill(~ok, 0.0)
        per_edge = h.sum(dim=1) / n_ok.clamp(min=1).float()
        return per_edge[edge_ok].mean()

    def loss_geo(self, D):
        if self.pair_i.numel() == 0:
            return torch.zeros((), device=self.device)
        return self._geo_pair_loss(D, self.pair_i, self.pair_j, reduce="mean")

    def _geo_pair_loss(self, D, pair_i, pair_j, reduce: str = "mean"):
        """Batched multi-view depth consistency on a strided UV grid.

        reduce='mean' → scalar for training; 'per_pair' → (P,) median |rel| for rematch.
        """
        cfg = self.config
        p = int(pair_i.shape[0])
        if p == 0:
            if reduce == "per_pair":
                return torch.zeros(0, device=self.device)
            return torch.zeros((), device=self.device)

        uv = self.sample_uv
        s = int(uv.shape[0])
        uv_b = uv.view(1, s, 2).expand(p, -1, -1)
        di = self.sample_batch(D[pair_i], uv_b)
        valid = (di > cfg.zmin) & (di < cfg.zmax)

        ki = self.K[pair_i]
        kj = self.K[pair_j]
        ti = self.T[pair_i]
        tj = self.T[pair_j]
        pix = torch.stack([uv_b[..., 0] * di, uv_b[..., 1] * di, di], dim=1)
        # Invalid depths → replace with 1 so solve stays finite; masked out later.
        pix = torch.where(valid[:, None, :], pix, torch.ones_like(pix))
        xc_i = torch.linalg.solve(ki, pix)
        r_i = ti[:, :3, :3]
        t_i = ti[:, :3, 3]
        xw = r_i.transpose(1, 2) @ (xc_i - t_i[:, :, None])
        r_j = tj[:, :3, :3]
        t_j = tj[:, :3, 3]
        xc_j = r_j @ xw + t_j[:, :, None]
        zj = xc_j[:, 2]
        front = valid & (zj > cfg.zmin)

        pix_j = kj @ xc_j
        uj = pix_j[:, 0] / pix_j[:, 2].clamp(min=1e-6)
        vj = pix_j[:, 1] / pix_j[:, 2].clamp(min=1e-6)
        inb = (
            front
            & (uj > 1)
            & (uj < self.W - 2)
            & (vj > 1)
            & (vj < self.H - 2)
            & (zj < cfg.zmax)
        )
        xy_j = torch.stack([uj, vj], dim=-1)
        dj = self.sample_batch(D[pair_j], xy_j)
        ok = inb & (dj > cfg.zmin) & (dj < cfg.zmax)

        n_src = valid.sum(dim=1)
        n_front = front.sum(dim=1)
        n_inb = inb.sum(dim=1)
        n_ok = ok.sum(dim=1)
        pair_ok = (
            (n_src >= self._geo_min_src)
            & (n_front >= self._geo_min_front)
            & (n_inb >= self._geo_min_inb)
            & (n_ok >= self._geo_min_ok)
        )

        if reduce == "per_pair":
            rel_abs = ((dj - zj) / (zj + 1e-3)).abs()
            rel_abs = rel_abs.masked_fill(~ok, float("nan"))
            # nanmedian over samples; dead pairs → +inf for rematch sorting
            med = torch.nanmedian(rel_abs, dim=1).values
            med = torch.where(pair_ok & torch.isfinite(med), med, torch.full_like(med, 1e9))
            return med

        rel = (dj - zj) / (zj.detach() + 1e-3)
        h = _huber(rel, 0.08)
        h = h.masked_fill(~ok, 0.0)
        per = h.sum(dim=1) / n_ok.clamp(min=1).float()
        if not pair_ok.any():
            return torch.zeros((), device=self.device)
        return per[pair_ok].mean()

    def loss_reg(self):
        cfg = self.config
        a_med = self.a.detach().median()
        loss = cfg.w_reg_a * ((self.a - a_med) ** 2).mean()
        if self.R is not None:
            loss = loss + cfg.w_reg_r * (self.R ** 2).mean()
            dx = (self.R[:, :, :, 1:] - self.R[:, :, :, :-1]).abs().mean()
            dy = (self.R[:, :, 1:, :] - self.R[:, :, :-1, :]).abs().mean()
            loss = loss + cfg.w_reg_r_tv * (dx + dy)
        return loss

    def total_loss(self):
        cfg = self.config
        D = self.depth_maps()
        lc = self.loss_colmap(D)
        lt = self.loss_track(D)
        lg = self.loss_geo(D)
        lr = self.loss_reg()
        total = cfg.w_colmap * lc + cfg.w_track * lt + cfg.w_geo * lg + lr
        return total, {
            "colmap": float(lc.detach()),
            "track": float(lt.detach()),
            "geo": float(lg.detach()),
            "reg": float(lr.detach()),
        }

    def optimize(self, steps: int, lr: float, name: str) -> tuple[float, list[dict[str, Any]]]:
        params = [self.a] + ([self.R] if self.R is not None else []) + ([self.s] if self.s is not None else [])
        opt = torch.optim.Adam(params, lr=lr)
        t0 = time.perf_counter()
        hist: list[dict[str, Any]] = []
        a_lo, a_hi = self.config.scale_clamp
        for s in range(steps):
            opt.zero_grad()
            loss, parts = self.total_loss()
            loss.backward()
            opt.step()
            with torch.no_grad():
                self.a.clamp_(a_lo, a_hi)
                if self.s is not None:
                    self.s.clamp_(0.5, 1.5)
            if s % 25 == 0 or s == steps - 1:
                hist.append({"step": s, "loss": float(loss.detach()), "name": name, **parts})
        return time.perf_counter() - t0, hist

    def rematch_outliers(self) -> dict[str, Any]:
        cfg = self.config
        with torch.no_grad():
            D = self.depth_maps()
            new_i, new_xy, new_z = [], [], []
            n_col_before = sum(int(z.numel()) for z in self.col_z)
            n_col_after = 0
            for i, xy, z in zip(self.col_i, self.col_xy, self.col_z):
                pred = self.sample_at(D[i], xy)
                err = (pred - z).abs()
                ok = (
                    (pred > cfg.zmin)
                    & (pred < cfg.zmax)
                    & (z > cfg.zmin)
                    & (z < cfg.zmax)
                    & torch.isfinite(err)
                )
                if ok.sum() < 20:
                    continue
                err_ok = err[ok]
                xy_ok = xy[ok]
                z_ok = z[ok]
                thr = torch.quantile(err_ok, cfg.colmap_keep_frac)
                thr = torch.minimum(thr, torch.tensor(cfg.colmap_max_abs_m, device=self.device))
                keep = err_ok <= thr
                if keep.sum() < 20:
                    k = min(20, int(err_ok.numel()))
                    idx = torch.topk(err_ok, k, largest=False).indices
                    keep = torch.zeros_like(err_ok, dtype=torch.bool)
                    keep[idx] = True
                new_i.append(i)
                new_xy.append(xy_ok[keep])
                new_z.append(z_ok[keep])
                n_col_after += int(keep.sum())
            self.col_i, self.col_xy, self.col_z = new_i, new_xy, new_z

            kept_tracks = []
            n_track_before = len(self.track_pairs)
            if self.track_i is not None and self.track_i.numel() > 0:
                i, j = self.track_i, self.track_j
                xyi, xyj, mask = self.track_xyi, self.track_xyj, self.track_mask
                di = self.sample_batch(D[i], xyi)
                dj = self.sample_batch(D[j], xyj)
                ok = mask & (di > cfg.zmin) & (di < cfg.zmax) & (dj > cfg.zmin) & (dj < cfg.zmax)
                xwi = self.unproject_world_batch(self.K[i], self.T[i], xyi, di)
                xwj = self.unproject_world_batch(self.K[j], self.T[j], xyj, dj)
                dist = torch.linalg.norm(xwi - xwj, dim=1)
                keep_pt = ok & (dist <= cfg.track_max_dist_m)
                for e in range(int(i.shape[0])):
                    kp = keep_pt[e]
                    if int(kp.sum()) < 8:
                        continue
                    kept_tracks.append(
                        (
                            int(i[e]),
                            int(j[e]),
                            [tuple(p) for p in xyi[e][kp].cpu().tolist()],
                            [tuple(p) for p in xyj[e][kp].cpu().tolist()],
                        )
                    )
            self._set_track_pairs(kept_tracks)

            n_geo_before = len(self.pairs)
            if self.pair_i.numel() > 0:
                scores = self._geo_pair_loss(D, self.pair_i, self.pair_j, reduce="per_pair")
                order = torch.argsort(scores)
                n_keep = max(8, int(round(len(self.pairs) * (1.0 - cfg.geo_drop_frac))))
                keep_idx = set(int(x) for x in order[:n_keep].cpu().tolist())
                kept_pairs = [p for pi, p in enumerate(self.pairs) if pi in keep_idx]
                self._set_geo_pairs(kept_pairs)
            else:
                keep_idx = set()

        return {
            "colmap_anchors_before": n_col_before,
            "colmap_anchors_after": n_col_after,
            "track_edges_before": n_track_before,
            "track_edges_after": len(self.track_pairs),
            "geo_pairs_before": n_geo_before,
            "geo_pairs_after": len(self.pairs),
        }

    def export_fitted_full(self) -> dict[str, np.ndarray]:
        import cv2

        with torch.no_grad():
            D = self.depth_maps().clamp(min=0).cpu().numpy()
        out: dict[str, np.ndarray] = {}
        for i, name in enumerate(self.selected):
            hf, wf = self.full_hw[i]
            d = cv2.resize(D[i].astype(np.float32), (wf, hf), interpolation=cv2.INTER_LINEAR)
            out[name] = d
        return out

    def consistency_mask_full(self, view_i: int, depth_full: np.ndarray, max_abs_m: float) -> np.ndarray:
        import cv2

        cfg = self.config
        hf, wf = depth_full.shape
        ok = np.isfinite(depth_full) & (depth_full > 0.05) & (depth_full < 10.0)
        if self.R is not None:
            with torch.no_grad():
                r = F.interpolate(
                    self.R[view_i : view_i + 1], size=(self.H, self.W), mode="bilinear", align_corners=True
                )
                r = r[0, 0].cpu().numpy()
            r_full = cv2.resize(np.abs(r), (wf, hf), interpolation=cv2.INTER_LINEAR)
            ok &= r_full < 0.25
        neighbors = []
        if view_i > 0:
            neighbors.append(view_i - 1)
        if view_i + 1 < self.n:
            neighbors.append(view_i + 1)
        if not neighbors:
            return ok
        with torch.no_grad():
            D = self.depth_maps().clamp(min=0)
            Di = D[view_i]
            agree = torch.zeros_like(Di, dtype=torch.bool)
            counted = torch.zeros_like(Di, dtype=torch.bool)
            us = torch.arange(0, self.W, device=self.device, dtype=torch.float32)
            vs = torch.arange(0, self.H, device=self.device, dtype=torch.float32)
            uu, vv = torch.meshgrid(us, vs, indexing="xy")
            uv = torch.stack([uu.reshape(-1), vv.reshape(-1)], 1)
            for j in neighbors:
                di = self.sample_at(Di, uv)
                valid = (di > cfg.zmin) & (di < cfg.zmax)
                if valid.sum() < 50:
                    continue
                u = uv[valid]
                z = di[valid]
                Ki, Kj = self.K[view_i], self.K[j]
                Ti, Tj = self.T[view_i], self.T[j]
                pix = torch.stack([u[:, 0] * z, u[:, 1] * z, z], 0)
                Xc_i = torch.linalg.solve(Ki, pix)
                R_i, t_i = Ti[:3, :3], Ti[:3, 3]
                Xw = R_i.T @ (Xc_i - t_i[:, None])
                R_j, t_j = Tj[:3, :3], Tj[:3, 3]
                Xc_j = R_j @ Xw + t_j[:, None]
                zj = Xc_j[2]
                front = zj > cfg.zmin
                if front.sum() < 40:
                    continue
                Xc_j = Xc_j[:, front]
                zj = zj[front]
                u = u[front]
                pix_j = Kj @ Xc_j
                uj = pix_j[0] / pix_j[2]
                vj = pix_j[1] / pix_j[2]
                inb = (
                    (uj > 1)
                    & (uj < self.W - 2)
                    & (vj > 1)
                    & (vj < self.H - 2)
                    & (zj < cfg.zmax)
                )
                if inb.sum() < 30:
                    continue
                uj, vj, zj, u = uj[inb], vj[inb], zj[inb], u[inb]
                dj = self.sample_at(D[j], torch.stack([uj, vj], 1))
                good = (dj > cfg.zmin) & (dj < cfg.zmax) & ((dj - zj).abs() <= max_abs_m)
                xs = u[:, 0].round().long().clamp(0, self.W - 1)
                ys = u[:, 1].round().long().clamp(0, self.H - 1)
                counted[ys, xs] = True
                agree[ys[good], xs[good]] = True
            low = Di.cpu().numpy()
            mask_da3 = np.ones_like(low, dtype=bool)
            c = counted.cpu().numpy()
            a = agree.cpu().numpy()
            mask_da3[c & ~a] = False
        mask_full = cv2.resize(mask_da3.astype(np.uint8), (wf, hf), interpolation=cv2.INTER_NEAREST).astype(bool)
        return ok & mask_full

    def export_fitted_full_confident(self, agree_max_abs_m: float) -> dict[str, np.ndarray]:
        fitted = self.export_fitted_full()
        for i, name in enumerate(self.selected):
            d = fitted[name]
            m = self.consistency_mask_full(i, d, max_abs_m=agree_max_abs_m)
            d = d.copy()
            d[~m] = 0.0
            fitted[name] = d
        return fitted

    def save_params(self, path: Path) -> None:
        rows = []
        a = self.a.detach().cpu().numpy()
        s = float(self.s.detach()) if self.s is not None else 1.0
        for i, name in enumerate(self.selected):
            rows.append(
                {
                    "image_name": name,
                    "scale_a": float(a[i]),
                    "global_s": s,
                    "scale_effective": float(a[i] * s),
                    "shift_b": 0.0,
                }
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            wri = csv.DictWriter(
                f, fieldnames=["image_name", "scale_a", "global_s", "scale_effective", "shift_b"]
            )
            wri.writeheader()
            wri.writerows(rows)
        if self.R is not None:
            np.save(path.with_suffix(".residual.npy"), self.R.detach().cpu().numpy())


def _run_fit_mode(fitter: JointDepthFitter, config: FitConfig) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray], dict[str, Any]]:
    """Optimize and return fitted + confident depth dicts and timing meta."""
    mode = config.mode
    hist: list[dict[str, Any]] = []
    rematch_stats = None
    use_confident = False
    t_opt = 0.0

    if mode == "affine":
        t_opt, h = fitter.optimize(config.steps_affine, config.lr_affine, "affine")
        hist.extend(h)
        fitted = fitter.export_fitted_full()
        confident = fitted
    elif mode == "residual_icp":
        n_rounds = max(1, int(config.icp_rounds))
        n_phases = n_rounds + 1
        # Split steps_res evenly across residual phases (last gets the remainder).
        base = max(1, config.steps_res // n_phases)
        phase_steps = [base] * n_phases
        phase_steps[-1] = max(1, config.steps_res - base * (n_phases - 1))

        rematch_stats_list: list[Any] = []
        t_opt = 0.0
        t1, h1 = fitter.optimize(config.steps_affine, config.lr_affine, "affine-warm")
        hist.extend(h1)
        t_opt += t1
        fitter.enable_residual()
        labels = "abcdefghijklmnopqrstuvwxyz"
        for i, n_steps in enumerate(phase_steps):
            label = f"residual-{labels[i] if i < len(labels) else i}"
            ti, hi = fitter.optimize(n_steps, config.lr_residual, label)
            hist.extend(hi)
            t_opt += ti
            if i < n_rounds:
                rematch_stats_list.append(fitter.rematch_outliers())
        rematch_stats = {
            "rounds": rematch_stats_list,
            "n_rounds": n_rounds,
            "phase_steps": phase_steps,
            "last": rematch_stats_list[-1] if rematch_stats_list else None,
        }
        fitted = fitter.export_fitted_full()
        confident = fitter.export_fitted_full_confident(agree_max_abs_m=config.agree_max_abs_m)
        use_confident = True
    elif mode == "residual":
        t1, h1 = fitter.optimize(config.steps_affine, config.lr_affine, "affine-warm")
        hist.extend(h1)
        fitter.enable_residual()
        t2, h2 = fitter.optimize(config.steps_res, config.lr_residual, "residual")
        hist.extend(h2)
        t_opt = t1 + t2
        fitted = fitter.export_fitted_full()
        confident = fitted
    else:
        raise ValueError(f"Unknown fit mode: {mode}")

    meta = {
        "mode": mode,
        "n_frames": fitter.n,
        "opt_seconds": t_opt,
        "use_confident_mask": use_confident,
        "pair_stats": fitter.pair_stats,
        "n_tracks": fitter._n_tracks,
        "n_track_edges": len(fitter.track_pairs),
        "rematch": rematch_stats,
        "scale_clamp": list(config.scale_clamp),
        "residual_grid": [config.res_gh, config.res_gw] if fitter.R is not None else None,
        "geo_stride": config.geo_stride,
        "geo_uv_samples": int(fitter.sample_uv.shape[0]),
        "opt_hist_tail": hist[-12:],
    }
    return fitted, confident, meta


def run_fit(
    colmap_dir: str | Path,
    depth_dir: str | Path,
    output_dir: str | Path,
    *,
    config: Optional[FitConfig] = None,
    image_names: Optional[Sequence[str]] = None,
    sparse_subdir: str = "",
) -> FitResult:
    """Joint scale + residual fit with optional ICP rematch; writes fitted/confident depth PNGs.

    Inputs are COLMAP sparse model + raw depth folder (no DA3 inference, carve, or TSDF).
    """
    config = config or FitConfig()
    colmap_dir = Path(colmap_dir)
    depth_dir = Path(depth_dir)
    output_dir = Path(output_dir)
    fitted_dir = output_dir / "fitted"
    confident_dir = output_dir / "confident"

    frames = frameset_from_colmap(colmap_dir, sparse_subdir=sparse_subdir, image_names=image_names)
    depths = load_depth_folder(depth_dir, frames.image_names)

    sparse_dir = resolve_sparse_dir(str(colmap_dir), sparse_subdir)
    cameras, images_by_id, points3d = read_model(str(sparse_dir))
    name_to_image = {im.name: im for im in images_by_id.values()}

    device = _resolve_device(config.device)
    t0 = time.perf_counter()
    fitter = JointDepthFitter(frames, depths, name_to_image, cameras, points3d, config, device)
    fitted, confident, fit_meta = _run_fit_mode(fitter, config)

    fitted_dir.mkdir(parents=True, exist_ok=True)
    confident_dir.mkdir(parents=True, exist_ok=True)
    save_depth_folder(fitted, fitted_dir)
    save_depth_folder(confident, confident_dir)

    params_path = output_dir / "params.csv"
    fitter.save_params(params_path)

    fit_meta["total_seconds"] = time.perf_counter() - t0
    fit_meta["device"] = str(device)
    meta_path = output_dir / "meta.json"
    meta_path.write_text(json.dumps(fit_meta, indent=2), encoding="utf-8")

    return FitResult(
        fitted_depth_dir=fitted_dir,
        confident_depth_dir=confident_dir,
        params_path=params_path,
        meta=fit_meta,
    )
