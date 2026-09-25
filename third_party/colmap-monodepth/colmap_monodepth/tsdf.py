"""Open3D ScalableTSDFVolume integration → tsdf_mesh.ply / tsdf_points.ply."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional, Sequence

import numpy as np

from colmap_monodepth.depth_io import load_depth_png, depth_png_path
from colmap_monodepth.mesh_postprocess import postprocess_tsdf_mesh
from colmap_monodepth.types import (
    FrameSet,
    TsdfConfig,
    TsdfResult,
    frameset_from_colmap,
)


def _tsdf_volume_color_type(config: TsdfConfig, o3d):
    mode = (config.color_type or "rgb8").lower()
    if mode in ("nocolor", "no_color", "none"):
        return o3d.pipelines.integration.TSDFVolumeColorType.NoColor
    if mode in ("rgb8", "rgb"):
        return o3d.pipelines.integration.TSDFVolumeColorType.RGB8
    raise ValueError(f"Unsupported TsdfConfig.color_type: {config.color_type!r}")


def _use_nocolor(config: TsdfConfig) -> bool:
    return (config.color_type or "rgb8").lower() in ("nocolor", "no_color", "none")


def run_tsdf(
    depth_dir: str | Path,
    output_dir: str | Path,
    image_paths: Optional[Sequence[str]] = None,
    *,
    frames: Optional[FrameSet] = None,
    colmap_dir: Optional[str | Path] = None,
    image_names: Optional[list[str]] = None,
    config: Optional[TsdfConfig] = None,
) -> TsdfResult:
    """Integrate masked depths into a scalable TSDF volume and export mesh + points.

    For ``color_type=rgb8``, ``image_paths`` must align with ``frames.image_names``.
    For ``color_type=nocolor``, RGB files are not read; ``image_paths`` may be omitted.
    Provide either ``frames`` or ``colmap_dir``.
    """
    try:
        import cv2
        import open3d as o3d
    except ImportError as exc:
        raise ImportError(
            "run_tsdf requires open3d and opencv-python (install the mesh extra)"
        ) from exc

    if frames is None:
        if colmap_dir is None:
            raise ValueError("run_tsdf requires frames or colmap_dir")
        frames = frameset_from_colmap(colmap_dir, image_names=image_names)
    elif image_names is not None:
        name_to_idx = frames.name_to_index()
        indices = [name_to_idx[n] for n in image_names]
        frames = FrameSet(
            image_names=list(image_names),
            intrinsics=frames.intrinsics[indices],
            extrinsics_w2c=frames.extrinsics_w2c[indices],
            widths=frames.widths[indices],
            heights=frames.heights[indices],
        )

    config = config or TsdfConfig()
    nocolor = _use_nocolor(config)
    paths = list(image_paths) if image_paths is not None else []

    if nocolor:
        if paths and len(paths) != frames.num_frames:
            raise ValueError(
                f"image_paths length ({len(paths)}) must match frames ({frames.num_frames})"
            )
    else:
        if len(paths) != frames.num_frames:
            raise ValueError(
                f"image_paths length ({len(paths)}) must match frames ({frames.num_frames})"
            )

    depth_dir = Path(depth_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    volume = o3d.pipelines.integration.ScalableTSDFVolume(
        voxel_length=config.voxel_length,
        sdf_trunc=config.sdf_trunc,
        color_type=_tsdf_volume_color_type(config, o3d),
    )

    t0 = time.perf_counter()
    for idx, name in enumerate(frames.image_names):
        h, w = int(frames.heights[idx]), int(frames.widths[idx])
        K = frames.intrinsics[idx]
        w2c = frames.extrinsics_w2c[idx]
        depth = load_depth_png(depth_png_path(depth_dir, name)).astype(np.float32)
        if depth.shape != (h, w):
            depth = cv2.resize(depth, (w, h), interpolation=cv2.INTER_NEAREST)

        depth[~np.isfinite(depth)] = 0
        depth[depth < config.min_depth] = 0
        depth[depth > config.depth_trunc] = 0

        depth_img = o3d.geometry.Image(np.ascontiguousarray(depth))
        if nocolor:
            # Open3D 0.20 has no depth-only RGBD factory; dummy color is ignored when volume is NoColor.
            dummy_rgb = o3d.geometry.Image(
                np.zeros((h, w, 3), dtype=np.uint8)
            )
            rgbd = o3d.geometry.RGBDImage.create_from_color_and_depth(
                dummy_rgb,
                depth_img,
                depth_scale=1.0,
                depth_trunc=config.depth_trunc,
                convert_rgb_to_intensity=False,
            )
        else:
            bgr = cv2.imread(str(paths[idx]), cv2.IMREAD_COLOR)
            if bgr is None:
                raise FileNotFoundError(f"Could not read image: {paths[idx]}")
            rgb = cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)
            if rgb.shape[0] != h or rgb.shape[1] != w:
                rgb = cv2.resize(rgb, (w, h), interpolation=cv2.INTER_AREA)
            rgbd = o3d.geometry.RGBDImage.create_from_color_and_depth(
                o3d.geometry.Image(np.ascontiguousarray(rgb.astype(np.uint8))),
                depth_img,
                depth_scale=1.0,
                depth_trunc=config.depth_trunc,
                convert_rgb_to_intensity=False,
            )

        intrinsic = o3d.camera.PinholeCameraIntrinsic(
            w,
            h,
            float(K[0, 0]),
            float(K[1, 1]),
            float(K[0, 2]),
            float(K[1, 2]),
        )
        volume.integrate(rgbd, intrinsic, w2c)

    mesh = volume.extract_triangle_mesh()
    mesh.compute_vertex_normals()
    post_meta: dict = {}
    if config.postprocess and len(mesh.triangles) > 0:
        mesh, post_meta = postprocess_tsdf_mesh(mesh, config=config)

    pcd = volume.extract_point_cloud()

    mesh_path = output_dir / "tsdf_mesh.ply"
    points_path = output_dir / "tsdf_points.ply"
    o3d.io.write_triangle_mesh(str(mesh_path), mesh)
    o3d.io.write_point_cloud(str(points_path), pcd, write_ascii=False)

    meta = {
        "tsdf_seconds": time.perf_counter() - t0,
        "mesh_vertices": len(mesh.vertices),
        "mesh_triangles": len(mesh.triangles),
        "tsdf_points": len(pcd.points),
        "voxel_length": config.voxel_length,
        "sdf_trunc": config.sdf_trunc,
        "color_type": config.color_type,
        **post_meta,
    }
    return TsdfResult(mesh_path=mesh_path, points_path=points_path, meta=meta)
