"""Thin glue to run colmap-monodepth mesh after local SfM refinement."""

from __future__ import annotations

import logging
import os
import subprocess
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

DEFAULT_STRIDE = 3
DEFAULT_PROCESS_RES = 504

_TRUTHY_ENV = frozenset({"1", "true", "yes", "on"})


def mono_depth_mesh_enabled_from_env() -> bool:
    return os.environ.get("MONO_DEPTH_MESH", "").strip().lower() in _TRUTHY_ENV


def mono_depth_mesh_enabled(cli_flag: bool = False) -> bool:
    return bool(cli_flag) or mono_depth_mesh_enabled_from_env()


def ensure_colmap_dataset_layout(scan_output: Path, images_dir: Path, sparse_dir: Path) -> Path:
    """Expose images/ + sparse/ under the scan output dir for colmap-monodepth."""
    scan_output = Path(scan_output)
    images_dir = Path(images_dir)
    sparse_dir = Path(sparse_dir)

    images_link = scan_output / "images"
    sparse_link = scan_output / "sparse"

    _ensure_dir_symlink(images_link, images_dir)
    _ensure_dir_symlink(sparse_link, sparse_dir)
    return scan_output


def _ensure_dir_symlink(link: Path, target: Path) -> None:
    target = target.resolve()
    if link.is_symlink():
        if link.resolve() == target:
            return
        link.unlink()
    elif link.exists():
        raise FileExistsError(
            f"Cannot link {link}: path exists and is not a symlink to {target}"
        )
    link.parent.mkdir(parents=True, exist_ok=True)
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        if link.resolve() == target:
            return
        if os.name == "nt":
            _windows_directory_junction(link, target)
            return
        raise


def _windows_directory_junction(link: Path, target: Path) -> None:
    """Directory junction (mklink /J) when symlinks require elevated privileges."""
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],
        check=True,
        capture_output=True,
        text=True,
    )


def mesh_output_dir(scan_output: Path) -> Path:
    """Final tsdf_mesh.ply path: ``<scan_output>/mesh/tsdf_mesh.ply``."""
    return Path(scan_output) / "mesh"


def global_mesh_output_dir(global_output: Path) -> Path:
    """Global fused mesh directory: ``<global_output>/mesh/``."""
    return Path(global_output) / "mesh"


def global_mesh_ply_path(global_output: Path) -> Path:
    return global_mesh_output_dir(global_output) / "tsdf_mesh.ply"


def local_scan_depth_dir(scan_output: Path) -> Optional[Path]:
    """Prefer carved depths; fall back to fit/confident (pipeline layout under scan root).

    ``run_mesh(colmap_dir, scan_output)`` writes ``carve/``, ``fit/confident/``, ``infer/depth/``
    as siblings of ``mesh/`` and ``sfm/`` — including when unpacked from RefinedScan.zip.
    """
    scan_output = Path(scan_output)
    candidates = (
        scan_output / "carve",
        scan_output / "fit" / "confident",
        scan_output / "infer" / "depth",
        # Legacy mistaken paths (pre-fix); keep for any in-workspace leftovers
        scan_output / "mesh" / "carve",
        scan_output / "mesh" / "fit" / "confident",
    )
    for depth_dir in candidates:
        if depth_dir.is_dir() and any(depth_dir.glob("*_depth.png")):
            return depth_dir
    return None


def _fuse_image_key(scan_id: str, image_name: str) -> str:
    return f"{scan_id}__{image_name}"


def _pycolmap_camera_to_K(camera):
    import numpy as np

    params = list(camera.params)
    model = camera.model.name
    if model == "PINHOLE":
        fx, fy, cx, cy = params
    elif model == "SIMPLE_PINHOLE":
        fx = fy = params[0]
        cx, cy = params[1], params[2]
    else:
        fx = fy = float(params[0]) if params else 1000.0
        cx = camera.width / 2.0
        cy = camera.height / 2.0
    return np.array([[fx, 0.0, cx], [0.0, fy, cy], [0.0, 0.0, 1.0]], dtype=np.float64)


def _cam_from_world_to_w2c(cam_from_world):
    """4x4 world-to-camera, matching COLMAP / colmap_monodepth ``image_to_extrinsic`` layout."""
    import numpy as np

    mat = np.asarray(cam_from_world.matrix(), dtype=np.float64)
    if mat.shape == (4, 4):
        return mat
    w2c = np.eye(4, dtype=np.float64)
    w2c[:3, :] = mat
    return w2c



def _collect_aligned_scan_views(
    scan_id: str,
    job_root_path: Path,
    alignment: "pycolmap.Sim3d",
    *,
    log: logging.Logger,
):
    """Return fuse keys, Ks, w2cs, widths, heights for one scan (domain frame).

    Depth + pose + intrinsics are enough for no-color global TSDF; RGB/Frames are optional.
    """
    import numpy as np
    import pycolmap

    scan_output = job_root_path / "refined" / "local" / scan_id
    depth_dir = local_scan_depth_dir(scan_output)
    if depth_dir is None:
        log.warning("mono_depth_mesh global: no local carved/confident depths for scan %s", scan_id)
        return [], np.zeros((0, 3, 3)), np.zeros((0, 4, 4)), np.zeros(0, dtype=np.int32), np.zeros(0, dtype=np.int32)

    sfm_dir = scan_output / "sfm"
    if not sfm_dir.is_dir():
        log.warning("mono_depth_mesh global: missing sfm for scan %s", scan_id)
        return [], np.zeros((0, 3, 3)), np.zeros((0, 4, 4)), np.zeros(0, dtype=np.int32), np.zeros(0, dtype=np.int32)

    rec = pycolmap.Reconstruction()
    rec.read(sfm_dir)
    rec.transform(alignment)

    stems_with_depth = {
        p.name[: -len("_depth.png")]
        for p in depth_dir.glob("*_depth.png")
        if p.name.endswith("_depth.png")
    }

    fuse_keys: List[str] = []
    Ks: List[np.ndarray] = []
    w2cs: List[np.ndarray] = []
    widths: List[int] = []
    heights: List[int] = []

    for image in sorted(rec.images.values(), key=lambda img: img.image_id):
        stem = Path(image.name).stem
        if stem not in stems_with_depth:
            continue
        src_depth = depth_dir / f"{stem}_depth.png"
        if not src_depth.is_file():
            continue

        fuse_key = _fuse_image_key(scan_id, image.name)
        fuse_keys.append(fuse_key)
        camera = rec.cameras[image.camera_id]
        Ks.append(_pycolmap_camera_to_K(camera))
        w2cs.append(_cam_from_world_to_w2c(image.cam_from_world()))
        widths.append(int(camera.width))
        heights.append(int(camera.height))

    if not fuse_keys:
        log.warning("mono_depth_mesh global: no depth/pose overlap for scan %s", scan_id)

    return (
        fuse_keys,
        np.stack(Ks, axis=0) if Ks else np.zeros((0, 3, 3)),
        np.stack(w2cs, axis=0) if w2cs else np.zeros((0, 4, 4)),
        np.asarray(widths, dtype=np.int32),
        np.asarray(heights, dtype=np.int32),
    )


def _stage_fused_depths(
    fuse_keys: Sequence[str],
    source_depth_dir: Path,
    staging_depth_dir: Path,
    depth_scale: float = 1.0,
) -> None:
    """Stage per-view depths for global TSDF, keyed by fuse image names.

    ``depth_scale`` is the Sim3 alignment scale (local → domain); metric depths are
    multiplied so they match poses from ``rec.transform(alignment)``.
    """
    from colmap_monodepth.depth_io import depth_png_path, load_depth_png, save_depth_png

    staging_depth_dir.mkdir(parents=True, exist_ok=True)
    depth_scale = float(depth_scale)
    for fuse_key in fuse_keys:
        stem = Path(fuse_key.split("__", 1)[-1]).stem
        src = source_depth_dir / f"{stem}_depth.png"
        dst = depth_png_path(staging_depth_dir, fuse_key)
        if dst.is_file():
            continue
        if depth_scale == 1.0:
            shutil.copy2(src, dst)
            continue
        depth_m = load_depth_png(src)
        save_depth_png(depth_m * depth_scale, dst)


def run_global_mono_depth_mesh(
    job_root_path: Path,
    global_output: Path,
    scan_ids: Sequence[str],
    alignment_transforms: Dict[str, "pycolmap.Sim3d"],
    *,
    log: Optional[logging.Logger] = None,
) -> Optional[Path]:
    """Fuse per-scan carved depths in domain frame via monodepth TSDF; soft-fail on error."""
    log = log or logger
    job_root_path = Path(job_root_path)
    global_output = Path(global_output)
    mesh_dir = global_mesh_output_dir(global_output)
    mesh_ply = mesh_dir / "tsdf_mesh.ply"

    try:
        import numpy as np
        from colmap_monodepth.tsdf import run_tsdf
        from colmap_monodepth.types import FrameSet, TsdfConfig
        from utils.topology_export import promote_tsdf_ply_to_topology

        all_keys: List[str] = []
        all_K: List[np.ndarray] = []
        all_w2c: List[np.ndarray] = []
        all_w: List[int] = []
        all_h: List[int] = []
        scans_used: List[str] = []

        staging_depth = mesh_dir / "_fuse_depths"
        if staging_depth.exists():
            shutil.rmtree(staging_depth)

        for scan_id in scan_ids:
            alignment = alignment_transforms.get(scan_id)
            if alignment is None:
                log.warning("mono_depth_mesh global: no alignment for scan %s", scan_id)
                continue
            scan_output = job_root_path / "refined" / "local" / scan_id
            depth_dir = local_scan_depth_dir(scan_output)
            if depth_dir is None:
                continue

            fuse_keys, Ks, w2cs, widths, heights = _collect_aligned_scan_views(
                scan_id,
                job_root_path,
                alignment,
                log=log,
            )
            if not fuse_keys:
                continue

            _stage_fused_depths(fuse_keys, depth_dir, staging_depth, alignment.scale)
            all_keys.extend(fuse_keys)
            all_K.append(Ks)
            all_w2c.append(w2cs)
            all_w.extend(list(widths))
            all_h.extend(list(heights))
            scans_used.append(scan_id)

        if not all_keys:
            log.warning("mono_depth_mesh global: no scans with local depths; skipping fuse")
            return None

        frames = FrameSet(
            image_names=all_keys,
            intrinsics=np.concatenate(all_K, axis=0),
            extrinsics_w2c=np.concatenate(all_w2c, axis=0),
            widths=np.asarray(all_w, dtype=np.int32),
            heights=np.asarray(all_h, dtype=np.int32),
        )

        log.info(
            "mono_depth_mesh global: fusing %d views from %d scans -> %s",
            len(all_keys),
            len(scans_used),
            mesh_dir,
        )
        mesh_dir.mkdir(parents=True, exist_ok=True)
        tsdf_config = TsdfConfig(color_type="nocolor")
        result = run_tsdf(
            staging_depth,
            mesh_dir,
            frames=frames,
            config=tsdf_config,
        )
        if mesh_ply.is_file():
            log.info("mono_depth_mesh global: wrote %s", mesh_ply)
            topology_dir = Path(global_output) / "topology"
            promote_tsdf_ply_to_topology(mesh_ply, topology_dir, log=log)
            return mesh_ply
        log.warning("mono_depth_mesh global: tsdf finished but %s is missing", mesh_ply)
        return result.mesh_path if result.mesh_path.is_file() else None
    except Exception:
        log.exception("mono_depth_mesh global fuse failed; continuing without global mesh")
        return None


def maybe_run_global_mono_depth_mesh(
    enabled: bool,
    job_root_path: Path,
    global_output: Path,
    scan_ids: Sequence[str],
    alignment_transforms: Dict[str, "pycolmap.Sim3d"],
    *,
    log: Optional[logging.Logger] = None,
) -> None:
    if not enabled:
        return
    run_global_mono_depth_mesh(
        job_root_path,
        global_output,
        scan_ids,
        alignment_transforms,
        log=log,
    )


def run_mono_depth_mesh(
    scan_output: Path,
    images_dir: Path,
    sparse_dir: Path,
    *,
    stride: int = DEFAULT_STRIDE,
    process_res: int = DEFAULT_PROCESS_RES,
    device: str = "cuda",
    log: Optional[logging.Logger] = None,
) -> Optional[Path]:
    """Run E1 mesh pipeline; soft-fail on any error."""
    log = log or logger
    scan_output = Path(scan_output)
    mesh_ply = mesh_output_dir(scan_output) / "tsdf_mesh.ply"

    try:
        from colmap_monodepth.pipeline import run_mesh

        colmap_dir = ensure_colmap_dataset_layout(scan_output, images_dir, sparse_dir)
        log.info(
            "mono_depth_mesh: colmap_dir=%s stride=%s process_res=%s",
            colmap_dir,
            stride,
            process_res,
        )
        run_mesh(
            colmap_dir,
            scan_output,
            stride=stride,
            process_res=process_res,
            device=device,
        )
        if mesh_ply.is_file():
            log.info("mono_depth_mesh: wrote %s", mesh_ply)
            return mesh_ply
        log.warning("mono_depth_mesh: run_mesh finished but %s is missing", mesh_ply)
        return None
    except Exception:
        log.exception("mono_depth_mesh failed; continuing without mesh")
        return None


def maybe_run_mono_depth_mesh(
    enabled: bool,
    scan_output: Path,
    images_dir: Path,
    sparse_dir: Path,
    *,
    stride: int = DEFAULT_STRIDE,
    process_res: int = DEFAULT_PROCESS_RES,
    device: str = "cuda",
    log: Optional[logging.Logger] = None,
) -> None:
    if not enabled:
        return
    run_mono_depth_mesh(
        scan_output,
        images_dir,
        sparse_dir,
        stride=stride,
        process_res=process_res,
        device=device,
        log=log,
    )
