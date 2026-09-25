"""Spatial-hash voxel grid for accumulating multi-view point clouds."""

from __future__ import annotations

from typing import Optional, Tuple

import numpy as np
import numba


@numba.jit(nopython=True, parallel=True)
def compute_voxel_indices(points: np.ndarray, voxel_size: float) -> np.ndarray:
    """Map each point to its nearest voxel index. Returns ``(N, 3)`` int32."""
    n = len(points)
    result = np.empty((n, 3), dtype=np.int32)
    for i in numba.prange(n):
        result[i, 0] = int(np.round(points[i, 0] / voxel_size))
        result[i, 1] = int(np.round(points[i, 1] / voxel_size))
        result[i, 2] = int(np.round(points[i, 2] / voxel_size))
    return result


@numba.jit(nopython=True)
def compute_empty_voxel_indices(
    camera_pos: np.ndarray,
    ray_dirs_norm: np.ndarray,
    ray_lengths: np.ndarray,
    valid_rays: np.ndarray,
    voxel_size: float,
    max_depth: float,
) -> np.ndarray:
    """Walk rays from *camera_pos* and collect voxel indices in free space.

    For each valid ray the function steps along the direction in increments of
    *voxel_size* and records every voxel that lies *before* the depth surface
    (with a half-voxel safety margin).

    Returns ``(M, 3)`` int32 array of voxel indices.
    """
    max_steps = int(max_depth / voxel_size)
    n_rays = len(ray_lengths)

    max_entries = n_rays * max_steps
    result = np.empty((max_entries, 3), dtype=np.int32)
    count = 0

    for step in range(1, max_steps + 1):
        t = step * voxel_size
        margin = voxel_size * 0.5

        any_valid = False
        for i in range(n_rays):
            if valid_rays[i] and t < ray_lengths[i] - margin:
                any_valid = True
                px = camera_pos[0] + t * ray_dirs_norm[i, 0]
                py = camera_pos[1] + t * ray_dirs_norm[i, 1]
                pz = camera_pos[2] + t * ray_dirs_norm[i, 2]
                result[count, 0] = int(np.round(px / voxel_size))
                result[count, 1] = int(np.round(py / voxel_size))
                result[count, 2] = int(np.round(pz / voxel_size))
                count += 1

        if not any_valid:
            break

    return result[:count]


class GridCell:
    """Running sums for points, normals, colours, and a confidence score."""

    __slots__ = (
        "point_sum",
        "point_count",
        "normal_sum",
        "normal_count",
        "color_sum",
        "color_count",
        "confidence",
    )

    def __init__(self) -> None:
        self.point_sum = np.zeros(3, dtype=np.float64)
        self.point_count: int = 0
        self.normal_sum = np.zeros(3, dtype=np.float64)
        self.normal_count: int = 0
        self.color_sum = np.zeros(3, dtype=np.float64)
        self.color_count: int = 0
        self.confidence: float = 0.0

    def get_means(self) -> Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[np.ndarray]]:
        pt = self.point_sum / self.point_count if self.point_count > 0 else None
        nm = self.normal_sum / self.normal_count if self.normal_count > 0 else None
        cl = self.color_sum / self.color_count if self.color_count > 0 else None
        return pt, nm, cl


class VoxelGrid:
    """Spatial-hash grid that accumulates per-voxel point / normal / colour means."""

    def __init__(self, voxel_size: float = 0.1) -> None:
        self.voxel_size = voxel_size
        self.cells: dict[tuple, GridCell] = {}

    def add_points_batch(
        self,
        points: np.ndarray,
        normals: Optional[np.ndarray] = None,
        colors: Optional[np.ndarray] = None,
        confidence: float = 1.0,
    ) -> None:
        """Insert *points* ``(N, 3)`` into the grid (with optional normals / colours)."""
        voxel_indices = compute_voxel_indices(points, self.voxel_size)
        cells = self.cells

        for i in range(len(points)):
            key = (int(voxel_indices[i, 0]), int(voxel_indices[i, 1]), int(voxel_indices[i, 2]))
            if key not in cells:
                cells[key] = GridCell()
            cell = cells[key]
            cell.point_sum[0] += points[i, 0]
            cell.point_sum[1] += points[i, 1]
            cell.point_sum[2] += points[i, 2]
            cell.point_count += 1
            cell.confidence += confidence

        if normals is not None:
            for i in range(len(normals)):
                key = (int(voxel_indices[i, 0]), int(voxel_indices[i, 1]), int(voxel_indices[i, 2]))
                cell = cells[key]
                cell.normal_sum[0] += normals[i, 0]
                cell.normal_sum[1] += normals[i, 1]
                cell.normal_sum[2] += normals[i, 2]
                cell.normal_count += 1

        if colors is not None:
            for i in range(len(colors)):
                key = (int(voxel_indices[i, 0]), int(voxel_indices[i, 1]), int(voxel_indices[i, 2]))
                cell = cells[key]
                cell.color_sum[0] += colors[i, 0]
                cell.color_sum[1] += colors[i, 1]
                cell.color_sum[2] += colors[i, 2]
                cell.color_count += 1

    def add_empty_batch(self, voxel_indices: np.ndarray, subtractive_value: float) -> None:
        """Mark voxels along free-space rays with negative confidence."""
        cells = self.cells
        for i in range(len(voxel_indices)):
            key = (int(voxel_indices[i, 0]), int(voxel_indices[i, 1]), int(voxel_indices[i, 2]))
            if key not in cells:
                cells[key] = GridCell()
            cells[key].confidence += subtractive_value
