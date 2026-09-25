"""Load/save 16-bit millimeter depth PNGs shared by fit, carve, and TSDF."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import numpy as np

MM_PER_METER = 1000.0
UINT16_MAX = 65535


def depth_to_uint16_mm(depth: np.ndarray, scale_mm: float = MM_PER_METER) -> np.ndarray:
    """Map metric depth (meters) to 16-bit PNG values in millimeters."""
    out = np.zeros(depth.shape, dtype=np.uint16)
    valid = np.isfinite(depth) & (depth > 0)
    mm = np.clip(depth[valid] * scale_mm, 0, UINT16_MAX)
    out[valid] = mm.astype(np.uint16)
    return out


def uint16_mm_to_depth_m(arr: np.ndarray, scale_mm: float = MM_PER_METER) -> np.ndarray:
    """Convert 16-bit millimeter PNG values to metric depth (meters)."""
    return arr.astype(np.float32) / scale_mm


def depth_png_stem(image_name: str) -> str:
    """Stem used for ``{stem}_depth.png`` files."""
    return Path(image_name).stem


def depth_png_path(depth_dir: str | Path, image_name: str) -> Path:
    """Path to the depth PNG for a given image name."""
    return Path(depth_dir) / f"{depth_png_stem(image_name)}_depth.png"


def load_depth_png(path: str | Path) -> np.ndarray:
    """Load a single 16-bit mm depth PNG as metric depth (meters)."""
    try:
        from PIL import Image
    except ImportError as exc:  # pragma: no cover
        raise ImportError("Pillow is required to load depth PNGs") from exc

    arr = np.array(Image.open(path))
    return uint16_mm_to_depth_m(arr)


def save_depth_png(depth_m: np.ndarray, path: str | Path) -> Path:
    """Write metric depth (meters) to a 16-bit millimeter PNG."""
    try:
        from PIL import Image
    except ImportError as exc:  # pragma: no cover
        raise ImportError("Pillow is required to save depth PNGs") from exc

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(depth_to_uint16_mm(depth_m)).save(path)
    return path


def load_depth_folder(depth_dir: str | Path, image_names: Sequence[str]) -> dict[str, np.ndarray]:
    """Load depth maps for each image name from ``{stem}_depth.png`` files."""
    depth_dir = Path(depth_dir)
    return {name: load_depth_png(depth_png_path(depth_dir, name)) for name in image_names}


def save_depth_folder(
    depths: dict[str, np.ndarray],
    output_dir: str | Path,
) -> list[Path]:
    """Write depth maps keyed by image name into ``output_dir``."""
    output_dir = Path(output_dir)
    paths: list[Path] = []
    for name, depth in depths.items():
        paths.append(save_depth_png(depth, depth_png_path(output_dir, name)))
    return paths
