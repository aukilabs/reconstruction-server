"""Shared dataclasses and configs for mesh-refine stages (carve, TSDF, fit)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np

from colmap_monodepth.colmap_io import (
    camera_to_K,
    image_to_extrinsic,
    read_model,
    resolve_sparse_dir,
)


@dataclass
class FrameSet:
    """Per-view geometry for depth fusion / carve / TSDF."""

    image_names: list[str]
    intrinsics: np.ndarray  # (N, 3, 3) float64
    extrinsics_w2c: np.ndarray  # (N, 4, 4) float64 world-to-camera
    widths: np.ndarray  # (N,) int
    heights: np.ndarray  # (N,) int

    def __post_init__(self) -> None:
        n = len(self.image_names)
        if self.intrinsics.shape != (n, 3, 3):
            raise ValueError(f"intrinsics must be ({n}, 3, 3), got {self.intrinsics.shape}")
        if self.extrinsics_w2c.shape != (n, 4, 4):
            raise ValueError(f"extrinsics_w2c must be ({n}, 4, 4), got {self.extrinsics_w2c.shape}")
        if self.widths.shape != (n,) or self.heights.shape != (n,):
            raise ValueError("widths and heights must have shape (N,)")

    @property
    def num_frames(self) -> int:
        return len(self.image_names)

    def name_to_index(self) -> dict[str, int]:
        return {name: i for i, name in enumerate(self.image_names)}


@dataclass
class CarveConfig:
    """Subtractive carve + multi-view voxel gate (production defaults).

    E1 kept subtractive_value=-0.2; voxel_size was loosened to 0.07 (2026-09-25).
    Gate tightened vs E1: min_points=3, min_confidence=2.0 (prefer empty over floaters).
    """

    voxel_size: float = 0.07
    subtractive_value: float = -0.2
    min_points: int = 3
    min_confidence: float = 2.0
    max_depth: float = 8.0
    unproject_stride: int = 4
    mask_stride: int = 2
    min_depth: float = 0.05
    mask_max_depth: float = 10.0


@dataclass
class CarveResult:
    output_depth_dir: Path
    meta: dict[str, Any]
    kept_voxel_keys: set[tuple[int, int, int]] = field(default_factory=set)


@dataclass
class TsdfConfig:
    """Open3D ScalableTSDFVolume defaults (coarser than E1 for flatter surfaces)."""

    voxel_length: float = 0.04
    sdf_trunc: float = 0.16
    depth_trunc: float = 10.0
    min_depth: float = 0.05
    # ``rgb8`` integrates color when image paths are provided; ``nocolor`` is geometry-only.
    color_type: str = "rgb8"
    postprocess: bool = True
    postprocess_taubin_iterations: int = 2
    postprocess_taubin_lambda: float = 0.5
    postprocess_taubin_mu: float = -0.52
    postprocess_max_geometric_error: float = 0.008
    postprocess_min_triangle_ratio: float = 0.5


@dataclass
class TsdfResult:
    mesh_path: Path
    points_path: Path
    meta: dict[str, Any]


@dataclass
class FitConfig:
    """Joint scale + residual fit with ICP rematch (E1 locked experiment defaults)."""

    mode: str = "residual_icp"  # affine | residual | residual_icp
    # residual_icp: affine + (residual → rematch) × icp_rounds + final residual.
    # steps_res is split evenly across (icp_rounds + 1) residual phases.
    # Default icp_rounds=1 → affine + half + rematch + half (50+50+50 with defaults).
    steps_affine: int = 50
    steps_res: int = 100
    icp_rounds: int = 1
    lr_affine: float = 0.02
    lr_residual: float = 0.01
    w_colmap: float = 1.0
    w_geo: float = 0.35
    w_track: float = 0.5
    w_reg_a: float = 0.02
    w_reg_r: float = 0.15
    w_reg_r_tv: float = 0.05
    geo_stride: int = 8
    geo_max_pairs_per_view: int = 3
    geo_max_baseline_m: float = 1.25
    covis_min_shared: int = 25
    covis_max_per_view: int = 4
    temporal_radius: int = 2
    max_geo_pairs: int = 120
    # Cost uses temporal neighbors only; pair_stats still reports spatial/covis/union.
    geo_cost_temporal_only: bool = True
    track_max_points: int = 2500
    track_max_views: int = 4
    res_gh: int = 12
    res_gw: int = 16
    zmin: float = 0.15
    zmax: float = 8.0
    scale_clamp: tuple[float, float] = (0.85, 1.15)
    colmap_keep_frac: float = 0.75
    colmap_max_abs_m: float = 0.20
    track_max_dist_m: float = 0.10
    geo_drop_frac: float = 0.25
    agree_max_abs_m: float = 0.05
    device: str = "auto"


@dataclass
class FitResult:
    fitted_depth_dir: Path
    confident_depth_dir: Path
    params_path: Path
    meta: dict[str, Any]


def scale_intrinsics_for_depth(
    K: np.ndarray,
    camera_width: int,
    camera_height: int,
    depth_width: int,
    depth_height: int,
) -> np.ndarray:
    """Scale pinhole intrinsics when depth resolution differs from the COLMAP camera."""
    if depth_width == camera_width and depth_height == camera_height:
        return K
    K_scaled = K.copy()
    K_scaled[0, :] *= depth_width / float(camera_width)
    K_scaled[1, :] *= depth_height / float(camera_height)
    return K_scaled


def camera_center_from_w2c(w2c: np.ndarray) -> np.ndarray:
    """World-space camera center from a 4x4 world-to-camera matrix."""
    R, t = w2c[:3, :3], w2c[:3, 3]
    return (-R.T @ t).astype(np.float64)


def frameset_from_colmap(
    colmap_dir: str | Path,
    *,
    sparse_subdir: str = "",
    image_names: Optional[Sequence[str]] = None,
) -> FrameSet:
    """Build a :class:`FrameSet` from a COLMAP reconstruction directory."""
    colmap_dir = Path(colmap_dir)
    sparse_dir = resolve_sparse_dir(str(colmap_dir), sparse_subdir)
    cameras, images, _points3D = read_model(str(sparse_dir))

    names: list[str] = []
    Ks: list[np.ndarray] = []
    Ts: list[np.ndarray] = []
    widths: list[int] = []
    heights: list[int] = []

    name_filter = set(image_names) if image_names is not None else None
    for _image_id, image_data in sorted(images.items(), key=lambda kv: kv[0]):
        if name_filter is not None and image_data.name not in name_filter:
            continue
        camera = cameras[image_data.camera_id]
        names.append(image_data.name)
        Ks.append(camera_to_K(camera))
        Ts.append(image_to_extrinsic(image_data))
        widths.append(int(camera.width))
        heights.append(int(camera.height))

    if not names:
        raise ValueError("No images matched for FrameSet")

    return FrameSet(
        image_names=names,
        intrinsics=np.stack(Ks, axis=0),
        extrinsics_w2c=np.stack(Ts, axis=0),
        widths=np.asarray(widths, dtype=np.int32),
        heights=np.asarray(heights, dtype=np.int32),
    )
