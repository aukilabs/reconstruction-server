"""End-to-end: prepare 7Scenes COLMAP dir and run pose-conditioned DA3 CLI."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scene", default="chess")
    p.add_argument("--data-root", type=Path, default=Path("data/7scenes"))
    p.add_argument("--colmap-dir", type=Path, default=Path("data/7scenes_colmap/chess"))
    p.add_argument("--output-dir", type=Path, default=Path("outputs/7scenes_chess"))
    p.add_argument("--model", default="depth-anything/DA3-BASE")
    p.add_argument("--device", default="cuda")
    p.add_argument("--max-frames", type=int, default=8)
    p.add_argument("--stride", type=int, default=30)
    p.add_argument(
        "--max-points",
        type=int,
        default=10_000_000,
        help="Max points in exported PLY (default: 10M, higher than CLI default)",
    )
    p.add_argument(
        "--process-res",
        type=int,
        default=504,
        help="DA3 process_res (max processing resolution)",
    )
    p.add_argument("--skip-prepare", action="store_true")
    args = p.parse_args()

    root = Path(__file__).resolve().parents[1]
    if not args.skip_prepare:
        prep = [
            sys.executable,
            str(root / "scripts" / "prepare_7scenes_colmap.py"),
            "--scene",
            args.scene,
            "--data-root",
            str(args.data_root),
            "--out-dir",
            str(args.colmap_dir),
            "--max-frames",
            str(args.max_frames),
            "--stride",
            str(args.stride),
        ]
        subprocess.check_call(prep, cwd=root)

    cmd = [
        sys.executable,
        "-m",
        "colmap_monodepth.cli",
        "--colmap-dir",
        str(args.colmap_dir),
        "--output-dir",
        str(args.output_dir),
        "--model",
        args.model,
        "--device",
        args.device,
        "--max-points",
        str(args.max_points),
        "--process-res",
        str(args.process_res),
    ]
    subprocess.check_call(cmd, cwd=root)

    depth_dir = args.output_dir / "depth"
    ply = args.output_dir / "pointcloud.ply"
    depth_pngs = list(depth_dir.glob("*_depth.png")) if depth_dir.is_dir() else []
    if not depth_pngs:
        raise SystemExit(f"E2E failed: no depth PNGs under {depth_dir}")
    if not ply.is_file() or ply.stat().st_size < 100:
        raise SystemExit(f"E2E failed: missing/empty PLY at {ply}")
    print(f"E2E OK: {len(depth_pngs)} depth PNGs, PLY={ply} ({ply.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
