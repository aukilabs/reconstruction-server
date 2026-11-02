"""Thin stage composers: infer → fit → carve → tsdf → mesh helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

from colmap_monodepth.carve import run_carve as _run_carve
from colmap_monodepth.colmap_io import load_colmap_scene
from colmap_monodepth.export import export_prediction
from colmap_monodepth.fit import run_fit as _run_fit
from colmap_monodepth.inference import load_model, run_pose_conditioned
from colmap_monodepth.tsdf import run_tsdf as _run_tsdf
from colmap_monodepth.types import (
    CarveConfig,
    CarveResult,
    FitConfig,
    FitResult,
    TsdfConfig,
    TsdfResult,
    frameset_from_colmap,
)


@dataclass
class InferResult:
    output_dir: Path
    depth_dir: Path
    ply_path: Path
    image_paths: list[str]
    image_names: list[str]
    num_points: int
    meta: dict[str, Any]


@dataclass
class MeshResult:
    output_dir: Path
    infer: InferResult
    tsdf: TsdfResult
    fit: Optional[FitResult] = None
    carve: Optional[CarveResult] = None


def _select_views(
    image_paths: list[str],
    extrinsics,
    intrinsics,
    *,
    stride: int = 1,
    max_images: int = 0,
) -> tuple[list[str], Any, Any]:
    if stride < 1:
        raise ValueError("stride must be >= 1")
    if stride > 1:
        image_paths = image_paths[::stride]
        extrinsics = extrinsics[::stride]
        intrinsics = intrinsics[::stride]
    if max_images and max_images > 0:
        image_paths = image_paths[:max_images]
        extrinsics = extrinsics[:max_images]
        intrinsics = intrinsics[:max_images]
    return image_paths, extrinsics, intrinsics


def _run_infer_da3(
    colmap_dir: Path,
    output_dir: Path,
    *,
    model: str,
    device: str,
    sparse_subdir: str,
    process_res: int,
    max_points: int,
    max_images: int,
    stride: int,
) -> InferResult:
    image_paths, extrinsics, intrinsics = load_colmap_scene(
        str(colmap_dir), sparse_subdir=sparse_subdir
    )
    n_total = len(image_paths)
    image_paths, extrinsics, intrinsics = _select_views(
        image_paths, extrinsics, intrinsics, stride=stride, max_images=max_images
    )
    image_names = [Path(p).name for p in image_paths]

    model_obj = load_model(model, device=device)
    prediction = run_pose_conditioned(
        model_obj,
        image_paths,
        extrinsics,
        intrinsics,
        process_res=process_res,
    )
    export = export_prediction(
        prediction,
        output_dir,
        image_paths=image_names,
        max_points=max_points,
    )
    meta = {
        "method": "da3",
        "n_registered": n_total,
        "n_processed": len(image_paths),
        "stride": stride,
        "model": model,
        "device": device,
        "process_res": process_res,
    }
    return InferResult(
        output_dir=output_dir,
        depth_dir=Path(export["depth_dir"]),
        ply_path=Path(export["ply_path"]),
        image_paths=image_paths,
        image_names=image_names,
        num_points=int(export["num_points"]),
        meta=meta,
    )


def _run_infer_delaunay(
    colmap_dir: Path,
    output_dir: Path,
    *,
    sparse_subdir: str,
    max_images: int,
    stride: int,
    delaunay_config: Any,
) -> InferResult:
    from dataclasses import replace

    from colmap_monodepth.delaunay_depth import DelaunayConfig, run_delaunay_depths

    cfg = delaunay_config or DelaunayConfig()
    cfg = replace(
        cfg,
        max_images=max_images if max_images else cfg.max_images,
        stride=stride if stride != 1 else cfg.stride,
    )

    depth_dir = output_dir / "depth"
    image_paths, image_names, meta = run_delaunay_depths(
        colmap_dir,
        depth_dir,
        config=cfg,
        sparse_subdir=sparse_subdir,
    )
    ply_path = output_dir / "pointcloud.ply"
    return InferResult(
        output_dir=output_dir,
        depth_dir=depth_dir,
        ply_path=ply_path,
        image_paths=list(image_paths),
        image_names=list(image_names),
        num_points=0,
        meta=meta,
    )


def run_infer(
    colmap_dir: str | Path,
    output_dir: str | Path,
    *,
    method: str = "da3",
    model: str = "depth-anything/DA3-BASE",
    device: str = "cuda",
    sparse_subdir: str = "",
    process_res: int = 504,
    max_points: int = 1_000_000,
    max_images: int = 0,
    stride: int = 1,
    delaunay_config: Any = None,
) -> InferResult:
    """COLMAP sparse → depth PNGs (``method``: ``da3`` or ``delaunay``)."""
    colmap_dir = Path(colmap_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    method = method.lower().strip()
    if method == "da3":
        return _run_infer_da3(
            colmap_dir,
            output_dir,
            model=model,
            device=device,
            sparse_subdir=sparse_subdir,
            process_res=process_res,
            max_points=max_points,
            max_images=max_images,
            stride=stride,
        )
    if method == "delaunay":
        return _run_infer_delaunay(
            colmap_dir,
            output_dir,
            sparse_subdir=sparse_subdir,
            max_images=max_images,
            stride=stride,
            delaunay_config=delaunay_config,
        )
    raise ValueError(f"Unknown infer method: {method!r} (expected 'da3' or 'delaunay')")


def run_fit(
    colmap_dir: str | Path,
    depth_dir: str | Path,
    output_dir: str | Path,
    *,
    config: Optional[FitConfig] = None,
    image_names: Optional[Sequence[str]] = None,
    sparse_subdir: str = "",
) -> FitResult:
    """Joint scale + residual fit; writes fitted/ and confident/ depth folders."""
    return _run_fit(
        colmap_dir,
        depth_dir,
        output_dir,
        config=config,
        image_names=image_names,
        sparse_subdir=sparse_subdir,
    )


def run_carve(
    colmap_dir: str | Path,
    depth_dir: str | Path,
    output_dir: str | Path,
    *,
    config: Optional[CarveConfig] = None,
    image_names: Optional[Sequence[str]] = None,
    sparse_subdir: str = "",
) -> CarveResult:
    """E1 subtractive carve → masked depth folder."""
    frames = frameset_from_colmap(
        colmap_dir,
        sparse_subdir=sparse_subdir,
        image_names=image_names,
    )
    return _run_carve(depth_dir, output_dir, frames=frames, config=config)


def run_tsdf(
    colmap_dir: str | Path,
    depth_dir: str | Path,
    output_dir: str | Path,
    image_paths: Optional[Sequence[str]] = None,
    *,
    config: Optional[TsdfConfig] = None,
    image_names: Optional[Sequence[str]] = None,
    sparse_subdir: str = "",
) -> TsdfResult:
    """Integrate masked depths into TSDF mesh + points PLY."""
    frames = frameset_from_colmap(
        colmap_dir,
        sparse_subdir=sparse_subdir,
        image_names=image_names,
    )
    return _run_tsdf(
        depth_dir,
        output_dir,
        image_paths,
        frames=frames,
        config=config,
    )


def run_mesh_da3(
    colmap_dir: str | Path,
    output_dir: str | Path,
    *,
    model: str = "depth-anything/DA3-BASE",
    device: str = "cuda",
    sparse_subdir: str = "",
    process_res: int = 504,
    max_points: int = 1_000_000,
    max_images: int = 0,
    stride: int = 1,
    fit_config: Optional[FitConfig] = None,
    carve_config: Optional[CarveConfig] = None,
    tsdf_config: Optional[TsdfConfig] = None,
) -> MeshResult:
    """DA3 full pipeline defaults: infer → fit → carve(fitted) → tsdf."""
    colmap_dir = Path(colmap_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    infer = run_infer(
        colmap_dir,
        output_dir / "infer",
        method="da3",
        model=model,
        device=device,
        sparse_subdir=sparse_subdir,
        process_res=process_res,
        max_points=max_points,
        max_images=max_images,
        stride=stride,
    )
    fit = run_fit(
        colmap_dir,
        infer.depth_dir,
        output_dir / "fit",
        config=fit_config,
        image_names=infer.image_names,
        sparse_subdir=sparse_subdir,
    )
    carve = run_carve(
        colmap_dir,
        fit.fitted_depth_dir,
        output_dir / "carve",
        config=carve_config,
        image_names=infer.image_names,
        sparse_subdir=sparse_subdir,
    )
    tsdf = run_tsdf(
        colmap_dir,
        carve.output_depth_dir,
        output_dir / "mesh",
        infer.image_paths,
        config=tsdf_config,
        image_names=infer.image_names,
        sparse_subdir=sparse_subdir,
    )
    return MeshResult(output_dir=output_dir, infer=infer, fit=fit, carve=carve, tsdf=tsdf)


def run_mesh_delaunay(
    colmap_dir: str | Path,
    output_dir: str | Path,
    *,
    sparse_subdir: str = "",
    max_images: int = 0,
    stride: int = 1,
    delaunay_config: Any = None,
    do_fit: bool = False,
    do_carve: bool = False,
    fit_config: Optional[FitConfig] = None,
    carve_config: Optional[CarveConfig] = None,
    tsdf_config: Optional[TsdfConfig] = None,
) -> MeshResult:
    """Delaunay pipeline defaults: infer → tsdf (fit/carve off; opt-in)."""
    colmap_dir = Path(colmap_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    infer = run_infer(
        colmap_dir,
        output_dir / "infer",
        method="delaunay",
        sparse_subdir=sparse_subdir,
        max_images=max_images,
        stride=stride,
        delaunay_config=delaunay_config,
    )

    depth_for_tsdf = infer.depth_dir
    fit: Optional[FitResult] = None
    carve: Optional[CarveResult] = None

    if do_fit:
        fit = run_fit(
            colmap_dir,
            infer.depth_dir,
            output_dir / "fit",
            config=fit_config,
            image_names=infer.image_names,
            sparse_subdir=sparse_subdir,
        )
        depth_for_tsdf = fit.confident_depth_dir

    if do_carve:
        carve = run_carve(
            colmap_dir,
            depth_for_tsdf,
            output_dir / "carve",
            config=carve_config,
            image_names=infer.image_names,
            sparse_subdir=sparse_subdir,
        )
        depth_for_tsdf = carve.output_depth_dir

    tsdf = run_tsdf(
        colmap_dir,
        depth_for_tsdf,
        output_dir / "mesh",
        infer.image_paths,
        config=tsdf_config,
        image_names=infer.image_names,
        sparse_subdir=sparse_subdir,
    )
    return MeshResult(output_dir=output_dir, infer=infer, fit=fit, carve=carve, tsdf=tsdf)


def run_mesh(
    colmap_dir: str | Path,
    output_dir: str | Path,
    *,
    method: str = "da3",
    model: str = "depth-anything/DA3-BASE",
    device: str = "cuda",
    sparse_subdir: str = "",
    process_res: int = 504,
    max_points: int = 1_000_000,
    max_images: int = 0,
    stride: int = 1,
    delaunay_config: Any = None,
    do_fit: Optional[bool] = None,
    do_carve: Optional[bool] = None,
    fit_config: Optional[FitConfig] = None,
    carve_config: Optional[CarveConfig] = None,
    tsdf_config: Optional[TsdfConfig] = None,
) -> MeshResult:
    """Dispatch to ``run_mesh_da3`` or ``run_mesh_delaunay`` (good defaults each)."""
    method = method.lower().strip()
    if method == "da3":
        return run_mesh_da3(
            colmap_dir,
            output_dir,
            model=model,
            device=device,
            sparse_subdir=sparse_subdir,
            process_res=process_res,
            max_points=max_points,
            max_images=max_images,
            stride=stride,
            fit_config=fit_config,
            carve_config=carve_config,
            tsdf_config=tsdf_config,
        )
    if method == "delaunay":
        return run_mesh_delaunay(
            colmap_dir,
            output_dir,
            sparse_subdir=sparse_subdir,
            max_images=max_images,
            stride=stride,
            delaunay_config=delaunay_config,
            do_fit=False if do_fit is None else do_fit,
            do_carve=False if do_carve is None else do_carve,
            fit_config=fit_config,
            carve_config=carve_config,
            tsdf_config=tsdf_config,
        )
    raise ValueError(f"Unknown mesh method: {method!r} (expected 'da3' or 'delaunay')")
