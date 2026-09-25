"""Unpack DMT scan MP4s to JPEG frames exactly as reconstruction-server does.

Source of truth:
  https://github.com/aukilabs/reconstruction-server/blob/main/utils/data_utils.py
  (`mp4_to_frames`, `process_frames`)

OpenCV note (from reconstruction-server Dockerfile history, e.g. hotfix e733abd0):
  opencv-python 4.11.0.86 mis-rotates unpacked MP4 frames 180°. Use 4.10.0.84
  (RS pin) or >=4.12 (fixed). This env pins opencv-python-headless>=4.12,<5.

RS layout:
  - looks for ``Frames.mp4`` under the scan folder
  - writes into ``<scan>/Frames/``
  - names ``{scan_folder.name}_{frame_index:06d}.jpg``
  - skips write if the JPEG already exists

This script also accepts ``dmt_recording_*.mp4`` (as in ``data/dmt_test``). In that
case the filename prefix is the recording timestamp so names match the COLMAP
``images.bin`` entries (e.g. ``2026-05-21_09-18-14_000000.jpg``).
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import cv2


def mp4_to_frames(mp4_path, frames_path, filename_prefix=""):
    """Mirror reconstruction-server ``utils.data_utils.mp4_to_frames``."""
    capture = cv2.VideoCapture(str(mp4_path))
    frame_count = 0
    print("Unpacking mp4 to frames:", mp4_path, "->", frames_path)
    while capture.isOpened():
        ret, frame = capture.read()
        if not ret:
            break
        img_path = f"{frames_path}/{filename_prefix}{frame_count:06d}.jpg"
        if not os.path.exists(img_path):
            cv2.imwrite(img_path, frame)
        frame_count += 1
    print(f"Unpacked {frame_count} frames from mp4")
    capture.release()
    return frame_count


_DMT_RECORDING_RE = re.compile(r"^dmt_recording_(.+)\.mp4$", re.IGNORECASE)


def find_scan_mp4(scan_folder: Path) -> Path | None:
    frames_mp4 = scan_folder / "Frames.mp4"
    if frames_mp4.is_file():
        return frames_mp4
    recordings = sorted(scan_folder.glob("dmt_recording_*.mp4"))
    if recordings:
        return recordings[0]
    others = sorted(p for p in scan_folder.glob("*.mp4") if p.is_file())
    return others[0] if others else None


def filename_prefix_for(scan_folder: Path, mp4_path: Path) -> str:
    """RS uses ``{scan_folder.name}_`` for Frames.mp4; DMT exports use timestamp."""
    if mp4_path.name.lower() == "frames.mp4":
        return f"{scan_folder.name}_"
    m = _DMT_RECORDING_RE.match(mp4_path.name)
    if m:
        return f"{m.group(1)}_"
    return f"{scan_folder.name}_"


def ensure_images_link(scan_folder: Path, frames_dir: Path) -> None:
    """Point ``images/`` at ``Frames/`` so the COLMAP CLI default layout works."""
    images = scan_folder / "images"
    if images.exists():
        return
    try:
        images.symlink_to(frames_dir.resolve(), target_is_directory=True)
    except OSError:
        # Windows without symlink privilege: directory junction via cmd
        import subprocess

        subprocess.check_call(
            ["cmd", "/c", "mklink", "/J", str(images), str(frames_dir.resolve())],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def unpack_scan(scan_folder: Path, *, link_images: bool = True, force: bool = False) -> int:
    mp4 = find_scan_mp4(scan_folder)
    if mp4 is None:
        print(f"No mp4 found under {scan_folder}")
        return 0

    frames_dir = scan_folder / "Frames"
    images_dir = scan_folder / "images"
    if force:
        import shutil

        # images/ may be a junction/symlink onto Frames/; remove link first.
        if images_dir.exists() or images_dir.is_symlink():
            images_dir.unlink(missing_ok=True) if images_dir.is_symlink() else None
            if images_dir.exists():
                # junction or real dir
                try:
                    images_dir.rmdir()
                except OSError:
                    shutil.rmtree(images_dir)
        if frames_dir.is_dir():
            print(f"Removing existing frames: {frames_dir}")
            shutil.rmtree(frames_dir)
    frames_dir.mkdir(parents=True, exist_ok=True)
    prefix = filename_prefix_for(scan_folder, mp4)
    n = mp4_to_frames(mp4, frames_dir, filename_prefix=prefix)
    if link_images:
        ensure_images_link(scan_folder, frames_dir)
    return n


def iter_scan_folders(root: Path) -> list[Path]:
    if find_scan_mp4(root) is not None:
        return [root]
    return sorted(
        p for p in root.iterdir() if p.is_dir() and find_scan_mp4(p) is not None
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/dmt_test"),
        help="DMT test root or a single scan folder",
    )
    p.add_argument(
        "--no-images-link",
        action="store_true",
        help="Do not create images/ -> Frames/ link",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Delete existing Frames/ before unpacking (needed after OpenCV version fix)",
    )
    args = p.parse_args()

    root = args.data_dir
    if not root.is_dir():
        raise SystemExit(f"Not a directory: {root}")

    import cv2

    print(f"OpenCV {cv2.__version__}")
    if cv2.__version__.startswith("4.11"):
        raise SystemExit(
            "OpenCV 4.11.x rotates MP4 frames wrong; install "
            "opencv-python-headless==4.10.0.84 or >=4.12,<5"
        )

    scans = iter_scan_folders(root)
    if not scans:
        raise SystemExit(f"No scan folders with mp4 under {root}")

    total = 0
    for scan in scans:
        print(f"=== {scan} ===")
        total += unpack_scan(
            scan, link_images=not args.no_images_link, force=args.force
        )
    print(f"Done. Total frames (across scans): {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
