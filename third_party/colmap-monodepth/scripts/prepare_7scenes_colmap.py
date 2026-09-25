"""Prepare a COLMAP-layout dataset from Microsoft 7Scenes GT poses.

Note: 7Scenes has no 'dining_table' scene. Public scenes are:
chess, fire, heads, office, pumpkin, redkitchen, stairs.
Default: chess (smallest common e2e choice).
"""

from __future__ import annotations

import argparse
import shutil
import urllib.request
import zipfile
from pathlib import Path

import numpy as np
from PIL import Image

from colmap_monodepth.colmap_io import Camera, Image as ColmapImage, rotmat2qvec, write_model

# Kinect RGB intrinsics commonly used for 7Scenes (640x480).
FX = FY = 525.0
CX, CY = 320.0, 240.0
WIDTH, HEIGHT = 640, 480

SCENE_URL = (
    "http://download.microsoft.com/download/2/8/5/28564B23-0828-408F-8631-23B1EFF1DAC8/{scene}.zip"
)


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.is_file():
        print(f"Using cached {dest}")
        return
    print(f"Downloading {url} ...")
    urllib.request.urlretrieve(url, dest)


def _read_pose_c2w(path: Path) -> np.ndarray:
    """7Scenes frame-XXXXXX.pose.txt is a 4x4 camera-to-world matrix."""
    mat = np.loadtxt(path)
    if mat.shape != (4, 4):
        raise ValueError(f"Expected 4x4 pose in {path}, got {mat.shape}")
    return mat


def prepare_scene(
    scene: str,
    data_root: Path,
    out_dir: Path,
    sequence: str = "seq-01",
    stride: int = 30,
    max_frames: int = 8,
) -> Path:
    zip_path = data_root / f"{scene}.zip"
    extract_dir = data_root / scene
    _download(SCENE_URL.format(scene=scene), zip_path)

    if not extract_dir.is_dir():
        print(f"Extracting {zip_path} ...")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(data_root)

    # Nested sequence zips (seq-01.zip etc.)
    seq_zip = extract_dir / f"{sequence}.zip"
    seq_dir = extract_dir / sequence
    if seq_zip.is_file() and not seq_dir.is_dir():
        print(f"Extracting {seq_zip} ...")
        with zipfile.ZipFile(seq_zip, "r") as zf:
            zf.extractall(extract_dir)

    if not seq_dir.is_dir():
        raise FileNotFoundError(f"Sequence directory not found: {seq_dir}")

    color_files = sorted(seq_dir.glob("frame-*.color.png"))
    if not color_files:
        # Some dumps use .color.jpg
        color_files = sorted(seq_dir.glob("frame-*.color.jpg"))
    if not color_files:
        raise FileNotFoundError(f"No color frames in {seq_dir}")

    selected = color_files[::stride][:max_frames]
    if not selected:
        raise RuntimeError("No frames selected; lower --stride or raise --max-frames")

    if out_dir.exists():
        shutil.rmtree(out_dir)
    images_out = out_dir / "images"
    sparse_out = out_dir / "sparse" / "0"
    images_out.mkdir(parents=True)
    sparse_out.mkdir(parents=True)

    cameras = {
        1: Camera(
            id=1,
            model="PINHOLE",
            width=WIDTH,
            height=HEIGHT,
            params=np.array([FX, FY, CX, CY], dtype=np.float64),
        )
    }
    images = {}

    for i, color_path in enumerate(selected):
        stem = color_path.name.replace(".color.png", "").replace(".color.jpg", "")
        pose_path = seq_dir / f"{stem}.pose.txt"
        if not pose_path.is_file():
            raise FileNotFoundError(pose_path)

        c2w = _read_pose_c2w(pose_path)
        w2c = np.linalg.inv(c2w)
        R = w2c[:3, :3]
        t = w2c[:3, 3]
        qvec = rotmat2qvec(R)

        out_name = f"{stem}.png"
        # Resize / convert to RGB PNG for a uniform pipeline
        img = Image.open(color_path).convert("RGB")
        if img.size != (WIDTH, HEIGHT):
            img = img.resize((WIDTH, HEIGHT), Image.BILINEAR)
        img.save(images_out / out_name)

        images[i + 1] = ColmapImage(
            id=i + 1,
            qvec=qvec,
            tvec=t.astype(np.float64),
            camera_id=1,
            name=out_name,
            xys=np.zeros((0, 2)),
            point3D_ids=np.zeros((0,), dtype=np.int64),
        )

    write_model(cameras, images, {}, str(sparse_out), ext=".txt")
    print(f"Wrote COLMAP scene with {len(images)} frames -> {out_dir}")
    return out_dir


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--scene", default="chess", help="7Scenes scene name (default: chess)")
    p.add_argument("--sequence", default="seq-01")
    p.add_argument("--data-root", type=Path, default=Path("data/7scenes"))
    p.add_argument("--out-dir", type=Path, default=Path("data/7scenes_colmap/chess"))
    p.add_argument("--stride", type=int, default=30)
    p.add_argument("--max-frames", type=int, default=8)
    args = p.parse_args()
    prepare_scene(
        args.scene,
        args.data_root,
        args.out_dir,
        sequence=args.sequence,
        stride=args.stride,
        max_frames=args.max_frames,
    )


if __name__ == "__main__":
    main()
