"""Single-image localization against a prior COLMAP reconstruction.

Design intent (see docs/single-image-localization-design.md): downstream use
often cares more about **knowing when not to trust** a localization than about
every frame achieving low error. Expose enough diagnostics (pairs, names,
robust-fit signals) for callers to threshold uncertain results for pose
refinement and related pipelines.

Usage:
    # One-time setup (reusable across queries):
    loc = SingleImageLocalizer.from_reconstruction_dir(
        reconstruction_dir=Path("refined/global/refined_sfm_combined"),
        image_dir=Path("datasets/scan_id/Frames"),
        features_h5=Path("refined/global/refined_sfm_combined/features.h5"),
    )

    # Per-query:
    result = loc.localize(
        image_path=Path("query.jpg"),
        camera=pycolmap.Camera(...),
        approximate_cam_from_world=pycolmap.Rigid3d(...),
    )
    print(result.refined_cam_from_world)
"""

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Tuple, Dict
import json
import logging
import re
import tempfile
import threading
import time
import cv2
import numpy as np
import pycolmap

from hloc import extract_features, match_features
from hloc.utils.io import get_keypoints, get_matches
#from hloc.utils.inference_device import select_inference_device, use_hloc_device

from utils.pointcloud_covisibility import PointcloudCovisibilityIndex
from utils.data_utils import convert_pose_opengl_to_colmap

logger = logging.getLogger(__name__)

_inductor_cudagraph_tls = threading.local()


def ensure_inductor_cudagraph_tls() -> None:
    """Initialize torch inductor CUDA-graph TLS for the current thread.

    LightGlue uses ``torch.compile`` (``compile_network=True``). Compiled inductor
    kernels expect per-thread TLS that is only set up on import in the main thread.
    Background workers (e.g. pose_tracking_server) hit::

        assert torch._C._is_key_in_tls("tree_manager_containers")

    without this. See https://github.com/pytorch/pytorch/issues/123177
    """
    if getattr(_inductor_cudagraph_tls, "ready", False):
        return
    import torch
    from torch._inductor import cudagraph_trees

    cudagraph_trees.local.tree_manager_containers = {}
    cudagraph_trees.local.tree_manager_locks = defaultdict(threading.Lock)
    torch._C._stash_obj_in_tls(
        "tree_manager_containers", cudagraph_trees.local.tree_manager_containers
    )
    torch._C._stash_obj_in_tls(
        "tree_manager_locks", cudagraph_trees.local.tree_manager_locks
    )
    _inductor_cudagraph_tls.ready = True


# Portal QR payloads are case-insensitive on scheme/host; short id is alphanumeric.
_PORTAL_QR_RE = re.compile(r"^https://r8\.hr/([A-Z0-9]+)\s*$", re.IGNORECASE)


@dataclass
class RefinedPortal:
    """Portal from refined_manifest.json, pose already in COLMAP / refinement world."""

    short_id: str
    world_T_portal: pycolmap.Rigid3d
    physical_size_m: float


def load_refined_manifest_portals(manifest_path: Path) -> Dict[str, RefinedPortal]:
    """Load portals[].pose (OpenGL) + physicalSize; return COLMAP world_T_portal."""
    with open(manifest_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    out: Dict[str, RefinedPortal] = {}
    for entry in data.get("portals", []):
        sid = str(entry["shortId"])
        pose = entry["pose"]
        pos_gl = np.array(
            [pose["position"]["x"], pose["position"]["y"], pose["position"]["z"]],
            dtype=np.float64,
        )
        rot = pose["rotation"]
        quat_gl = np.array(
            [rot["w"], rot["x"], rot["y"], rot["z"]], dtype=np.float64
        )
        pos_c, quat_c = convert_pose_opengl_to_colmap(pos_gl, quat_gl)
        world_T_portal = pycolmap.Rigid3d(
            pycolmap.Rotation3d(quat_c), np.array(pos_c, dtype=np.float64)
        )
        out[sid] = RefinedPortal(
            short_id=sid,
            world_T_portal=world_T_portal,
            physical_size_m=float(entry["physicalSize"]),
        )
    logger.info("Loaded %d refined portal(s) from %s", len(out), manifest_path)
    return out


def _parse_portal_qr_short_id(decoded: str) -> Optional[str]:
    m = _PORTAL_QR_RE.match(decoded.strip())
    return m.group(1).upper() if m else None


def inlier_2d3d_ratio(num_inliers: int, num_2d3d: int) -> float:
    """num_inliers / max(num_2d3d, 1); same definition as holdout analyze_only."""
    return float(num_inliers) / float(max(int(num_2d3d), 1))


def passes_inlier_ratio_gate(
    num_inliers: int, num_2d3d: int, min_ratio: float
) -> bool:
    return inlier_2d3d_ratio(num_inliers, num_2d3d) >= min_ratio


def _qr_payload_preview(text: str, max_len: int = 72) -> str:
    t = text.replace("\n", " ").strip()
    if len(t) <= max_len:
        return repr(t)
    return repr(t[: max_len - 3] + "...")


def _opencv_K_from_pycolmap_camera(camera: pycolmap.Camera) -> np.ndarray:
    """3x3 K matching pycolmap camera params (fx, fy, cx, cy)."""
    p = camera.params
    if camera.model.name == "SIMPLE_PINHOLE":
        fx, cx, cy = float(p[0]), float(p[1]), float(p[2])
        fy = fx
    else:
        fx, fy, cx, cy = float(p[0]), float(p[1]), float(p[2]), float(p[3])
    return np.array([[fx, 0.0, cx], [0.0, fy, cy], [0.0, 0.0, 1.0]], dtype=np.float64)


def _opencv_qr_object_points(qr_size_m: float) -> np.ndarray:
    """3D square for solvePnP; must match OpenCV QR corner order (TL, TR, BR, BL)."""
    s = 0.5 * float(qr_size_m)
    return np.array(
        [
            [-s, -s, 0.0],
            [s, -s, 0.0],
            [s, s, 0.0],
            [-s, s, 0.0],
        ],
        dtype=np.float64,
    )


def _mean_reproj_portal_local(
    obj_local: np.ndarray,
    img_pts: np.ndarray,
    rvec: np.ndarray,
    tvec: np.ndarray,
    K: np.ndarray,
    dist: np.ndarray,
) -> float:
    """Reprojection error for solvePnP pose (object = portal-local square)."""
    proj, _ = cv2.projectPoints(
        obj_local, rvec, tvec, K, dist
    )
    proj = proj.reshape(-1, 2)
    return float(np.mean(np.linalg.norm(proj - img_pts, axis=1)))


@dataclass
class PortalPnPOutcome:
    """Successful portal QR PnP (COLMAP cam_from_world) plus debug fields."""

    cam_from_world: pycolmap.Rigid3d
    short_id: str
    decoded_text: str
    physical_size_m: float
    world_T_portal: pycolmap.Rigid3d
    rvec_cv: np.ndarray
    tvec_cv: np.ndarray
    mean_reproj_px: float


def _log_portal_pnp_debug(
    short_id: str,
    *,
    img_pts: np.ndarray,
    obj_local: np.ndarray,
    physical_size_m: float,
    world_T_portal: pycolmap.Rigid3d,
    K: np.ndarray,
    pnp_ok: bool,
    rvec: Optional[np.ndarray] = None,
    tvec: Optional[np.ndarray] = None,
    cam_from_world: Optional[pycolmap.Rigid3d] = None,
    ippe_fit_px: Optional[float] = None,
) -> None:
    """Verbose portal PnP diagnostics (INFO) for corner / object / pose verification."""
    wtp_t = np.asarray(world_T_portal.translation, dtype=np.float64).reshape(3)
    wtp_q = np.asarray(world_T_portal.rotation.quat, dtype=np.float64)
    lines = [
        f"[qr-pnp] short_id={short_id} solvePnP_ok={pnp_ok}",
        f"[qr-pnp]   physical_size_m={physical_size_m:.6f}",
        f"[qr-pnp]   image_points_px (OpenCV TL,TR,BR,BL): "
        + " ".join(f"({p[0]:.2f},{p[1]:.2f})" for p in img_pts),
        f"[qr-pnp]   object_points_m (TL,TR,BR,BL): "
        + " ".join(f"({p[0]:.4f},{p[1]:.4f},{p[2]:.4f})" for p in obj_local),
        f"[qr-pnp]   K=[[{K[0,0]:.2f},0,{K[0,2]:.2f}],[0,{K[1,1]:.2f},{K[1,2]:.2f}],[0,0,1]]",
        f"[qr-pnp]   world_T_portal t={wtp_t.tolist()} quat_wxyz={wtp_q.tolist()}",
    ]
    if rvec is not None and tvec is not None:
        lines.append(
            f"[qr-pnp]   solvePnP rvec={rvec.reshape(3).tolist()} "
            f"tvec={tvec.reshape(3).tolist()}"
        )
    if cam_from_world is not None:
        cfw_t = np.asarray(cam_from_world.translation, dtype=np.float64).reshape(3)
        cfw_q = np.asarray(cam_from_world.rotation.quat, dtype=np.float64)
        lines.append(
            f"[qr-pnp]   cam_from_world t={cfw_t.tolist()} quat_wxyz={cfw_q.tolist()}"
        )
    if ippe_fit_px is not None:
        lines.append(
            f"[qr-pnp]   ippe_fit_px={ippe_fit_px:.2f} "
            "(portal-local reprojection of solvePnP rvec/tvec; debug only)"
        )
    for line in lines:
        logger.info(line)


def _solve_portal_pnp_one_detection(
    portal: RefinedPortal,
    image_points: np.ndarray,
    K: np.ndarray,
    dist: np.ndarray,
    decoded_text: str,
) -> Optional[Tuple[float, PortalPnPOutcome]]:
    """IPPE_SQUARE in portal frame (OpenCV QR corner order), then compose cam_from_world."""
    object_points = _opencv_qr_object_points(portal.physical_size_m)
    R_wp = portal.world_T_portal.rotation.matrix()
    t_wp = np.asarray(portal.world_T_portal.translation, dtype=np.float64).reshape(3)
    img_pts = np.asarray(image_points, dtype=np.float64).reshape(4, 2)

    pnp_ok, rvec, tvec = cv2.solvePnP(
        object_points,
        img_pts,
        K,
        dist,
        flags=cv2.SOLVEPNP_IPPE_SQUARE,
    )
    if not pnp_ok:
        _log_portal_pnp_debug(
            portal.short_id,
            img_pts=img_pts,
            obj_local=object_points,
            physical_size_m=portal.physical_size_m,
            world_T_portal=portal.world_T_portal,
            K=K,
            pnp_ok=False,
        )
        logger.info(
            "[qr] short_id=%s solvePnP failed (IPPE_SQUARE)",
            portal.short_id,
        )
        return None

    rvec = np.asarray(rvec, dtype=np.float64).reshape(3)
    tvec = np.asarray(tvec, dtype=np.float64).reshape(3)

    R_cv, _ = cv2.Rodrigues(rvec.reshape(3, 1))
    R_cv = R_cv.astype(np.float64)
    R_cw = R_cv @ R_wp.T
    t_cw = tvec - R_cw @ t_wp

    cfw = pycolmap.Rigid3d(pycolmap.Rotation3d(R_cw), t_cw)
    ippe_fit_px = _mean_reproj_portal_local(
        object_points, img_pts, rvec, tvec, K, dist
    )
    _log_portal_pnp_debug(
        portal.short_id,
        img_pts=img_pts,
        obj_local=object_points,
        physical_size_m=portal.physical_size_m,
        world_T_portal=portal.world_T_portal,
        K=K,
        pnp_ok=True,
        rvec=rvec,
        tvec=tvec,
        cam_from_world=cfw,
        ippe_fit_px=ippe_fit_px,
    )
    outcome = PortalPnPOutcome(
        cam_from_world=cfw,
        short_id=portal.short_id,
        decoded_text=decoded_text,
        physical_size_m=portal.physical_size_m,
        world_T_portal=portal.world_T_portal,
        rvec_cv=rvec.copy(),
        tvec_cv=tvec.copy(),
        mean_reproj_px=ippe_fit_px,
    )
    return ippe_fit_px, outcome


def _try_portal_qr_cam_from_world(
    bgr: np.ndarray,
    camera: pycolmap.Camera,
    portals: Dict[str, RefinedPortal],
    log_query: str = "",
) -> Optional[PortalPnPOutcome]:
    """If a known portal QR is visible, return pose and debug intermediates."""
    if not portals:
        return None

    ctx = f"query={log_query!r} " if log_query else ""

    det = cv2.QRCodeDetector()
    ok, decoded_info, points, _ = det.detectAndDecodeMulti(bgr)
    if not ok or points is None:
        logger.debug("[qr] %sdetectAndDecodeMulti not ok or points=None (ok=%s)", ctx, ok)
        return None

    pts_arr = np.asarray(points, dtype=np.float64)
    if pts_arr.ndim != 3 or pts_arr.shape[1] != 4 or pts_arr.shape[2] != 2:
        logger.info(
            "[qr] %sunexpected points array shape=%s (expected Nx4x2)",
            ctx,
            getattr(pts_arr, "shape", None),
        )
        return None
    n_pts = int(pts_arr.shape[0])
    if n_pts == 0:
        logger.debug("[qr] %szero quadrangles from detector", ctx)
        return None

    dec = tuple(decoded_info) if decoded_info is not None else ()

    K = _opencv_K_from_pycolmap_camera(camera)
    dist = np.zeros((5, 1), dtype=np.float64)

    summary: List[str] = []
    best: Optional[Tuple[float, PortalPnPOutcome]] = None

    for i in range(n_pts):
        raw = dec[i] if i < len(dec) else None
        text = str(raw).strip() if raw is not None else ""
        if not text:
            summary.append(f"#{i}:empty_decode")
            continue

        sid = _parse_portal_qr_short_id(text)
        if sid is None:
            summary.append(f"#{i}:non_r8hr payload={_qr_payload_preview(text)}")
            continue
        if sid not in portals:
            summary.append(
                f"#{i}:id={sid} not_in_manifest(n={len(portals)})"
            )
            continue

        # OpenCV QRCodeDetector order: top-left, top-right, bottom-right, bottom-left.
        image_points = pts_arr[i].reshape(4, 2).astype(np.float64)
        portal = portals[sid]
        solved = _solve_portal_pnp_one_detection(
            portal, image_points, K, dist, text
        )
        if solved is None:
            summary.append(f"#{i}:{sid}=manifest_ok pnp_rejected")
            continue
        err, outcome = solved
        summary.append(f"#{i}:{sid}=pnp_ok reproj={err:.2f}px")
        if best is None or err < best[0]:
            best = (err, outcome)

    logger.info(
        "[qr] %sOpenCV QR n=%d %s",
        ctx,
        n_pts,
        " | ".join(summary) if summary else "(no decoded rows)",
    )

    if best is None:
        return None
    _, outcome = best
    return outcome


def _build_feature_conf():
    """Build ALIKED feature config matching triangulation.py process_features_and_matching."""
    conf = deepcopy(extract_features.confs["aliked-n16"])
    conf["model"]["max_num_keypoints"] = 1024
    conf["model"]["detection_threshold"] = 0.3
    conf["model"]["nms_radius"] = 4
    conf["preprocessing"]["resize_max"] = 1024
    return conf


def _resolve_dataset_image_path(image_dir: Path, image_name: str) -> Optional[Path]:
    """Resolve a COLMAP image name under job ``datasets/`` (direct or by leaf name)."""
    direct = image_dir / image_name
    if direct.is_file():
        return direct
    leaf = Path(image_name).name
    for candidate in image_dir.rglob(leaf):
        if candidate.is_file():
            return candidate
    return None


def warmup_lightglue_jit(
    reconstruction: pycolmap.Reconstruction,
    image_dir: Path,
    features_h5: Path,
) -> None:
    """Run one LightGlue pair at startup (``compile_network=True``) to warm JIT / caches.

    Uses the first two reconstruction images (by image id). Mirrors ``localize()``:
    ALIKED on the first image, then LightGlue query+ref against ``features_h5``.
    """
    if len(reconstruction.images) < 2:
        logger.warning("LightGlue JIT warmup skipped: fewer than 2 images in reconstruction")
        return

    image_ids = sorted(reconstruction.images.keys())[:2]
    name_q = reconstruction.images[image_ids[0]].name
    name_db = reconstruction.images[image_ids[1]].name
    t0 = time.perf_counter()

    try:
        with tempfile.TemporaryDirectory(prefix="loc_lg_warmup_") as tmp_dir:
            tmp_path = Path(tmp_dir)
            pairs_path = tmp_path / "pairs.txt"
            matches_path = tmp_path / "matches.h5"

            matcher_conf = deepcopy(match_features.confs["aliked+lightglue"])
            matcher_conf["model"]["compile_network"] = True

            path_q = _resolve_dataset_image_path(image_dir, name_q)
            if path_q is not None:
                query_image_dir = tmp_path / "images"
                query_image_dir.mkdir()
                query_name = path_q.name
                (query_image_dir / query_name).symlink_to(path_q.resolve())

                query_features_path = tmp_path / "query_features.h5"
                feature_conf = _build_feature_conf()
                feature_conf["output"] = "query_features"
                extract_features.main(
                    feature_conf,
                    query_image_dir,
                    feature_path=query_features_path,
                    as_half=True,
                    image_list=[query_name],
                )

                pairs_path.write_text(f"{query_name} {name_db}\n")
                match_features.main(
                    matcher_conf,
                    pairs_path,
                    features=query_features_path,
                    matches=matches_path,
                    features_ref=features_h5,
                )
            else:
                logger.info(
                    "LightGlue warmup: image file missing for %r; "
                    "matching both sides from features.h5 only",
                    name_q,
                )
                pairs_path.write_text(f"{name_q} {name_db}\n")
                match_features.main(
                    matcher_conf,
                    pairs_path,
                    features=features_h5,
                    matches=matches_path,
                )
    except Exception:
        logger.exception("LightGlue JIT warmup failed (server will continue)")
        return

    logger.info(
        "LightGlue JIT warmup finished in %.2fs (pair %r -> %r)",
        time.perf_counter() - t0,
        name_q,
        name_db,
    )


@dataclass
class LocalizationResult:
    """Result of a single-image localization query."""
    success: bool
    refined_cam_from_world: Optional[pycolmap.Rigid3d]
    num_inliers: int = 0
    num_matches: int = 0
    num_2d3d_correspondences: int = 0
    matched_db_image_ids: List[int] = field(default_factory=list)
    covisibility_scores: List[Tuple[int, int]] = field(default_factory=list)
    from_portal_qr: bool = False
    portal_short_id: Optional[str] = None


class SingleImageLocalizer:
    """Localize a single image against a preloaded COLMAP reconstruction.

    Lifecycle:
        1. Create once (loads reconstruction, builds spatial index).
        2. Call localize() for each query frame.
        3. The index and reconstruction stay in memory for repeated queries.
    """

    def __init__(
        self,
        reconstruction: pycolmap.Reconstruction,
        image_dir: Path,
        features_h5: Path,
        voxel_size: float = 0.20,
        num_pairs: int = 10,
        max_depth: float = 15.0,
        ransac_max_error: float = 12.0,
        min_inliers_2d3d_ratio: float = 0.3,
        portals: Optional[Dict[str, RefinedPortal]] = None,
    ):
        """
        Args:
            reconstruction: Loaded COLMAP reconstruction with 3D points and images.
            image_dir: Directory containing the database images (needed by hloc
                       if feature cache doesn't have them, though normally it does).
            features_h5: Path to the precomputed features .h5 file for database images
                         (from the original triangulation run / merged at global stitch).
            voxel_size: Voxel grid cell size in metres.
            num_pairs: Number of database images to match against.
            max_depth: Max frustum ray depth for voxel traversal.
            ransac_max_error: PnP RANSAC inlier threshold in pixels.
            min_inliers_2d3d_ratio: After PnP, reject if num_inliers / max(num_2d3d, 1)
                is below this (holdout QC gate; portal QR skips this check).
            portals: Refined manifest portals (COLMAP world); coupled to this reconstruction.
        """
        self.reconstruction = reconstruction
        self.image_dir = image_dir
        self.features_h5 = features_h5
        self.num_pairs = num_pairs
        self.max_depth = max_depth
        self.ransac_max_error = ransac_max_error
        self.min_inliers_2d3d_ratio = min_inliers_2d3d_ratio
        self.portals = portals

        # Build the spatial index
        self.index = PointcloudCovisibilityIndex(
            reconstruction, voxel_size=voxel_size
        )

        # Pre-build name<->id lookups for the database
        self.db_name_to_id: Dict[str, int] = {
            img.name: img_id for img_id, img in reconstruction.images.items()
        }
        self.db_id_to_name: Dict[int, str] = {
            img_id: img.name for img_id, img in reconstruction.images.items()
        }

        logger.info(
            f"SingleImageLocalizer ready: {len(reconstruction.images)} db images, "
            f"{len(reconstruction.points3D)} 3D points"
        )

    def warmup_lightglue(self) -> None:
        """Pre-compile LightGlue via one matcher pair (see ``warmup_lightglue_jit``)."""
        warmup_lightglue_jit(self.reconstruction, self.image_dir, self.features_h5)

    @classmethod
    def from_reconstruction_dir(
        cls,
        reconstruction_dir: Path,
        image_dir: Path,
        features_h5: Path,
        **kwargs,
    ) -> "SingleImageLocalizer":
        """Create from a COLMAP reconstruction directory on disk."""
        logger.info(f"Loading reconstruction from {reconstruction_dir}")
        reconstruction = pycolmap.Reconstruction(reconstruction_dir)
        return cls(
            reconstruction=reconstruction,
            image_dir=image_dir,
            features_h5=features_h5,
            **kwargs,
        )

    def localize(
        self,
        image_path: Path,
        camera: pycolmap.Camera,
        approximate_cam_from_world: Optional[pycolmap.Rigid3d] = None,
        query_name: Optional[str] = None,
    ) -> LocalizationResult:
        """Localize a single query image.

        Args:
            image_path: Path to the query RGB image.
            camera: Query camera intrinsics (pycolmap.Camera).
            approximate_cam_from_world: Rough pose for feature-based localization only
                (covisibility + matching). Ignored for portal QR — QR pose comes solely
                from manifest geometry + detected corners, not from this prior.
            query_name: Optional name for the query image in h5 files.
                        Defaults to image_path.name.

        Returns:
            LocalizationResult with refined pose (or failure info).
        """
        if query_name is None:
            query_name = image_path.name

        bgr = cv2.imread(str(image_path))
        if bgr is None and self.portals:
            logger.warning("[qr] cv2.imread failed; skip QR scan for %s", image_path)
        # Portal QR: absolute prior from refined manifest only (no approximate_cam_from_world).
        if bgr is not None and self.portals:
            outcome = _try_portal_qr_cam_from_world(
                bgr, camera, self.portals, log_query=query_name
            )
            if outcome is not None:
                wtp = outcome.world_T_portal
                pq = np.asarray(wtp.rotation.quat, dtype=np.float64)
                pt = np.asarray(wtp.translation, dtype=np.float64)
                cfw = outcome.cam_from_world
                cq = np.asarray(cfw.rotation.quat, dtype=np.float64)
                ct = np.asarray(cfw.translation, dtype=np.float64)
                logger.info(
                    "[portal] QR=%r short_id=%s physical_size_m=%.6f "
                    "true_placed_pose_colmap_t=%s quat_wxyz=%s "
                    "solvePnP_rvec=%s solvePnP_tvec=%s mean_reproj_px=%.4f "
                    "cam_from_world_colmap_t=%s quat_wxyz=%s",
                    outcome.decoded_text,
                    outcome.short_id,
                    outcome.physical_size_m,
                    pt.tolist(),
                    pq.tolist(),
                    outcome.rvec_cv.tolist(),
                    outcome.tvec_cv.tolist(),
                    outcome.mean_reproj_px,
                    ct.tolist(),
                    cq.tolist(),
                )
                return LocalizationResult(
                    success=True,
                    refined_cam_from_world=outcome.cam_from_world,
                    num_inliers=4,
                    num_matches=0,
                    num_2d3d_correspondences=4,
                    from_portal_qr=True,
                    portal_short_id=outcome.short_id,
                )

        # TEMPORARY: portal QR only — set True to re-enable feature-based visual localization.
        _enable_visual_loc = False
        if not _enable_visual_loc:
            logger.warning(
                "[portal-only] No portal QR pose; visual localization is temporarily disabled."
            )
            return LocalizationResult(success=False, refined_cam_from_world=None)

        if approximate_cam_from_world is None:
            approximate_cam_from_world = pycolmap.Rigid3d()

        # --- Step 1: Find best database image pairs via frustum covisibility ---
        covisibility_scores = self.index.find_covisible_images(
            approximate_cam_from_world,
            camera,
            num_images=self.num_pairs,
            max_depth=self.max_depth,
        )
        db_image_ids = [img_id for img_id, _score in covisibility_scores]

        if not db_image_ids:
            logger.warning("No database images found for query. Localization failed.")
            return LocalizationResult(success=False, refined_cam_from_world=None)

        db_image_names = [self.db_id_to_name[img_id] for img_id in db_image_ids]
        logger.info(f"Selected {len(db_image_ids)} database images for matching")

        # --- Step 2: Extract features on query image + match ---
        with tempfile.TemporaryDirectory(prefix="loc_query_") as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Symlink the query image so hloc can find it
            query_image_dir = tmp_path / "images"
            query_image_dir.mkdir()
            (query_image_dir / query_name).symlink_to(image_path.resolve())

            query_features_path = tmp_path / "query_features.h5"
            query_matches_path = tmp_path / "query_matches.h5"
            pairs_path = tmp_path / "pairs.txt"

            # Write pairs file: query matched against each DB image
            with open(pairs_path, "w") as f:
                for db_name in db_image_names:
                    f.write(f"{query_name} {db_name}\n")

            # Extract features on the query image
            feature_conf = _build_feature_conf()
            feature_conf["output"] = "query_features"
            extract_features.main(
                feature_conf,
                query_image_dir,
                feature_path=query_features_path,
                as_half=True,
                image_list=[query_name],
            )

            # --- Step 3: Match query against database images ---
            matcher_conf = match_features.confs["aliked+lightglue"]
            matcher_conf["model"]["compile_network"] = True
            ensure_inductor_cudagraph_tls()
            match_features.main(
                matcher_conf,
                pairs_path,
                features=query_features_path,
                matches=query_matches_path,
                features_ref=self.features_h5,
            )
        
            # --- Step 4: Collect 2D<->3D correspondences and run PnP ---
            result = self._estimate_pose(
                query_name=query_name,
                query_camera=camera,
                db_image_ids=db_image_ids,
                query_features_path=query_features_path,
                query_matches_path=query_matches_path,
                covisibility_scores=covisibility_scores,
            )

        return result

    def _estimate_pose(
        self,
        query_name: str,
        query_camera: pycolmap.Camera,
        db_image_ids: List[int],
        query_features_path: Path,
        query_matches_path: Path,
        covisibility_scores: List[Tuple[int, int]],
    ) -> LocalizationResult:
        """Run PnP+RANSAC from feature matches against database 3D points.

        For each matched DB image, look up which of its 2D keypoints have
        associated 3D points, then accumulate 2D(query)<->3D correspondences.
        """
        kpq = get_keypoints(query_features_path, query_name)
        kpq += 0.5  # COLMAP coordinate convention

        # Accumulate 2D query keypoint idx -> 3D point ID
        kp_idx_to_3D: Dict[int, int] = {}
        total_matches = 0

        for db_id in db_image_ids:
            db_image = self.reconstruction.images[db_id]
            if db_image.num_points3D == 0:
                continue

            # Get 3D point IDs for each 2D observation in the DB image
            points3D_ids = np.array([
                p.point3D_id if p.has_point3D() else -1
                for p in db_image.points2D
            ])

            try:
                matched, _ = get_matches(
                    query_matches_path, query_name, db_image.name
                )
            except (KeyError, ValueError):
                continue

            if len(matched) == 0:
                continue

            # Filter to matches where the DB keypoint has a 3D point
            valid = points3D_ids[matched[:, 1]] != -1
            matched = matched[valid]
            total_matches += len(matched)

            for query_idx, db_idx in matched:
                p3d_id = points3D_ids[db_idx]
                # Keep first observation per query keypoint (avoid duplicates)
                if query_idx not in kp_idx_to_3D:
                    kp_idx_to_3D[query_idx] = p3d_id

        if len(kp_idx_to_3D) < 4:
            logger.warning(
                f"Only {len(kp_idx_to_3D)} 2D-3D correspondences found "
                f"(need >=4 for PnP). Localization failed."
            )
            return LocalizationResult(
                success=False,
                refined_cam_from_world=None,
                num_matches=total_matches,
                num_2d3d_correspondences=len(kp_idx_to_3D),
                matched_db_image_ids=db_image_ids,
                covisibility_scores=covisibility_scores,
            )

        # Build arrays for PnP
        query_kp_idxs = list(kp_idx_to_3D.keys())
        points2D = kpq[query_kp_idxs]
        points3D = np.array([
            self.reconstruction.points3D[kp_idx_to_3D[idx]].xyz
            for idx in query_kp_idxs
        ])

        logger.info(
            f"PnP input: {len(points2D)} 2D-3D correspondences "
            f"from {total_matches} raw matches"
        )

        # Run PnP + RANSAC + refinement
        ret = pycolmap.estimate_and_refine_absolute_pose(
            points2D,
            points3D,
            query_camera,
            estimation_options={
                "ransac": {"max_error": self.ransac_max_error}
            },
            refinement_options={},
        )

        if ret is None or "cam_from_world" not in ret:
            logger.warning("PnP+RANSAC failed to find a valid pose.")
            return LocalizationResult(
                success=False,
                refined_cam_from_world=None,
                num_matches=total_matches,
                num_2d3d_correspondences=len(kp_idx_to_3D),
                matched_db_image_ids=db_image_ids,
                covisibility_scores=covisibility_scores,
            )

        num_inliers = int(ret["num_inliers"])
        num_2d3d = len(kp_idx_to_3D)
        ratio = inlier_2d3d_ratio(num_inliers, num_2d3d)

        if not passes_inlier_ratio_gate(
            num_inliers, num_2d3d, self.min_inliers_2d3d_ratio
        ):
            logger.warning(
                "Localization rejected: inlier/2d3d ratio %.4f < %.4f "
                "(%d inliers / %d correspondences)",
                ratio,
                self.min_inliers_2d3d_ratio,
                num_inliers,
                num_2d3d,
            )
            return LocalizationResult(
                success=False,
                refined_cam_from_world=None,
                num_inliers=num_inliers,
                num_matches=total_matches,
                num_2d3d_correspondences=num_2d3d,
                matched_db_image_ids=db_image_ids,
                covisibility_scores=covisibility_scores,
            )

        logger.info(
            f"Localization succeeded: {num_inliers} inliers "
            f"out of {num_2d3d} correspondences (ratio={ratio:.4f})"
        )

        return LocalizationResult(
            success=True,
            refined_cam_from_world=ret["cam_from_world"],
            num_inliers=num_inliers,
            num_matches=total_matches,
            num_2d3d_correspondences=num_2d3d,
            matched_db_image_ids=db_image_ids,
            covisibility_scores=covisibility_scores,
        )
