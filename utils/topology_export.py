"""Shared topology mesh export (full + downsampled LODs) for alpha-shape and TSDF paths."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def write_topology_lod_meshes(
    mesh,
    output_dir: Path,
    *,
    basename: str = "topology",
    log: Optional[logging.Logger] = None,
) -> None:
    """Write ``{basename}.{obj,glb}`` and two quadric-decimated LOD meshes (÷3, ÷9 triangles)."""
    import open3d as o3d

    log = log or logger
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    o3d.io.write_triangle_mesh(str(output_dir / f"{basename}.glb"), mesh)
    o3d.io.write_triangle_mesh(str(output_dir / f"{basename}.obj"), mesh)

    full_tri_count = len(mesh.triangles)
    downsampled = mesh
    ratio = 1.0
    for _ in range(2):
        downsampled = downsampled.filter_smooth_laplacian(number_of_iterations=1)
        ratio /= 3
        downsampled = downsampled.simplify_quadric_decimation(
            target_number_of_triangles=int(full_tri_count * ratio)
        )
        o3d.io.write_triangle_mesh(
            str(output_dir / f"{basename}_downsampled_{ratio:.3f}.glb"),
            downsampled,
        )
        o3d.io.write_triangle_mesh(
            str(output_dir / f"{basename}_downsampled_{ratio:.3f}.obj"),
            downsampled,
        )
    log.info(
        "topology export: wrote %s + downsampled LODs under %s",
        basename,
        output_dir,
    )


def promote_tsdf_ply_to_topology(
    tsdf_ply: Path,
    topology_dir: Path,
    *,
    log: Optional[logging.Logger] = None,
) -> bool:
    """Load cleaned global TSDF mesh and write primary ``topology.*`` artifacts."""
    import open3d as o3d

    log = log or logger
    tsdf_ply = Path(tsdf_ply)
    if not tsdf_ply.is_file():
        log.warning("topology promote: missing tsdf mesh %s", tsdf_ply)
        return False

    mesh = o3d.io.read_triangle_mesh(str(tsdf_ply))
    if len(mesh.triangles) == 0:
        log.warning("topology promote: empty mesh in %s", tsdf_ply)
        return False

    write_topology_lod_meshes(mesh, topology_dir, basename="topology", log=log)
    return True
