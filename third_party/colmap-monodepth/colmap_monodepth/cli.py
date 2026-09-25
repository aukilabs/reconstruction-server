"""CLI: COLMAP sparse → DA3 depth + mesh-refine stages (infer / fit / carve / tsdf / mesh)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SUBCMDS = frozenset({"infer", "fit", "carve", "tsdf", "mesh"})


def _add_colmap_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--colmap-dir",
        required=True,
        help="COLMAP dataset root with images/ and sparse/, sparse/0/, or model files in the root",
    )
    p.add_argument(
        "--sparse-subdir",
        default="",
        help="Optional sparse subdirectory, e.g. '0' for sparse/0/",
    )


def _add_infer_args(p: argparse.ArgumentParser) -> None:
    _add_colmap_args(p)
    p.add_argument("--output-dir", required=True, help="Directory for depth PNGs and pointcloud.ply")
    p.add_argument(
        "--model",
        default="depth-anything/DA3-BASE",
        help="Hugging Face model id or local path (default: depth-anything/DA3-BASE)",
    )
    p.add_argument("--device", default="cuda", help="Torch device (default: cuda)")
    p.add_argument("--process-res", type=int, default=504, help="DA3 process_res")
    p.add_argument("--max-points", type=int, default=1_000_000, help="Max points in exported PLY")
    p.add_argument(
        "--max-images",
        type=int,
        default=0,
        help="If >0, only use the first N registered images after --stride",
    )
    p.add_argument(
        "--stride",
        type=int,
        default=1,
        help="Use every Nth registered image (default: 1 = all)",
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="colmap-monodepth",
        description=(
            "COLMAP sparse to Depth Anything 3 pose-conditioned depth and mesh-refine pipeline. "
            "Stages: infer | fit | carve | tsdf | mesh (E1: infer->fit->carve(confident)->tsdf)."
        ),
    )
    sub = p.add_subparsers(dest="command", metavar="COMMAND")

    infer_p = sub.add_parser(
        "infer",
        help="Run DA3 pose-conditioned depth inference (depth PNGs + pointcloud.ply)",
    )
    _add_infer_args(infer_p)

    fit_p = sub.add_parser(
        "fit",
        help="Joint scale + residual depth fit (writes fitted/ and confident/)",
    )
    _add_colmap_args(fit_p)
    fit_p.add_argument("--depth-dir", required=True, help="Input depth folder (e.g. infer/depth)")
    fit_p.add_argument("--output-dir", required=True, help="Output directory for fit artifacts")
    fit_p.add_argument(
        "--fit-mode",
        default="residual_icp",
        choices=["affine", "residual", "residual_icp"],
        help="Fit mode (default: residual_icp, E1)",
    )
    fit_p.add_argument("--device", default="auto", help="Torch device for fit (default: auto)")

    carve_p = sub.add_parser("carve", help="E1 subtractive carve to masked depth folder")
    _add_colmap_args(carve_p)
    carve_p.add_argument("--depth-dir", required=True, help="Input depth folder (use fit confident/)")
    carve_p.add_argument("--output-dir", required=True, help="Output directory for carved depths")

    tsdf_p = sub.add_parser("tsdf", help="Integrate masked depths into TSDF mesh + points PLY")
    _add_colmap_args(tsdf_p)
    tsdf_p.add_argument("--depth-dir", required=True, help="Input masked depth folder (e.g. carve/)")
    tsdf_p.add_argument("--output-dir", required=True, help="Output directory for tsdf_mesh.ply")

    mesh_p = sub.add_parser(
        "mesh",
        help="E1 full pipeline: infer -> fit -> carve(confident) -> tsdf",
    )
    _add_infer_args(mesh_p)

    return p


def _cmd_infer(args: argparse.Namespace) -> int:
    from colmap_monodepth.pipeline import run_infer

    result = run_infer(
        args.colmap_dir,
        args.output_dir,
        model=args.model,
        device=args.device,
        sparse_subdir=args.sparse_subdir,
        process_res=args.process_res,
        max_points=args.max_points,
        max_images=args.max_images,
        stride=args.stride,
    )
    print(f"  wrote {len(result.image_names)} depth PNGs -> {result.depth_dir}")
    print(f"  wrote PLY ({result.num_points} points) -> {result.ply_path}")
    return 0


def _cmd_fit(args: argparse.Namespace) -> int:
    from colmap_monodepth.pipeline import run_fit
    from colmap_monodepth.types import FitConfig

    config = FitConfig(mode=args.fit_mode, device=args.device)
    result = run_fit(
        args.colmap_dir,
        args.depth_dir,
        args.output_dir,
        config=config,
        sparse_subdir=args.sparse_subdir,
    )
    print(f"  wrote fitted depths -> {result.fitted_depth_dir}")
    print(f"  wrote confident depths -> {result.confident_depth_dir}")
    print(f"  params -> {result.params_path}")
    return 0


def _cmd_carve(args: argparse.Namespace) -> int:
    from colmap_monodepth.pipeline import run_carve

    result = run_carve(
        args.colmap_dir,
        args.depth_dir,
        args.output_dir,
        sparse_subdir=args.sparse_subdir,
    )
    print(f"  wrote carved depths -> {result.output_depth_dir}")
    print(f"  kept {result.meta.get('n_kept_cells', '?')} voxels")
    return 0


def _cmd_tsdf(args: argparse.Namespace) -> int:
    from colmap_monodepth.colmap_io import load_colmap_scene
    from colmap_monodepth.pipeline import run_tsdf

    image_paths, _ext, _intr = load_colmap_scene(
        args.colmap_dir, sparse_subdir=args.sparse_subdir
    )
    image_names = [Path(p).name for p in image_paths]
    result = run_tsdf(
        args.colmap_dir,
        args.depth_dir,
        args.output_dir,
        image_paths,
        image_names=image_names,
        sparse_subdir=args.sparse_subdir,
    )
    print(f"  wrote mesh -> {result.mesh_path}")
    print(f"  wrote points -> {result.points_path}")
    return 0


def _cmd_mesh(args: argparse.Namespace) -> int:
    from colmap_monodepth.pipeline import run_mesh

    result = run_mesh(
        args.colmap_dir,
        args.output_dir,
        model=args.model,
        device=args.device,
        sparse_subdir=args.sparse_subdir,
        process_res=args.process_res,
        max_points=args.max_points,
        max_images=args.max_images,
        stride=args.stride,
    )
    print(f"  infer depths -> {result.infer.depth_dir}")
    print(f"  fit confident -> {result.fit.confident_depth_dir}")
    print(f"  carved depths -> {result.carve.output_depth_dir}")
    print(f"  mesh -> {result.tsdf.mesh_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    raw = list(argv if argv is not None else sys.argv[1:])
    # Backward compatibility: bare flags map to the infer subcommand.
    if raw and raw[0] not in _SUBCMDS and raw[0] not in ("-h", "--help"):
        raw = ["infer"] + raw

    args = build_parser().parse_args(raw)
    if args.command is None:
        build_parser().print_help()
        return 0

    handlers = {
        "infer": _cmd_infer,
        "fit": _cmd_fit,
        "carve": _cmd_carve,
        "tsdf": _cmd_tsdf,
        "mesh": _cmd_mesh,
    }
    print(f"Running {args.command} ...")
    return handlers[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
