#!/usr/bin/env python3
"""
HTTP pose-tracking server compatible with Rover-SLAM slam_server REST API.

Uses the same single-image localization stack as ``localize_main.py`` /
``localize_image.py`` (covisibility index + ALIKED + LightGlue + PnP), not a
separate QR-only path.

POST /api/v1/track — queue RGB frame (JSON base64 or raw image bytes).
GET /api/v1/status, /api/v1/trajectory, /api/v1/health — same shape as slam_server_api.md.

Layout (typical global-refinement job workspace):
  --job_root            job directory (contains ``datasets/``)
  --reconstruction_root directory with ``refined_sfm_combined/`` and ``features.h5`` inside it
                        (usually ``<job_root>/refined/global``)
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import queue
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Repo imports: script lives at repo root
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# OpenMP / torch init order (see localize_main.py)
import torch  # noqa: F401

import cv2
import numpy as np
import pycolmap

from localize_main import get_or_create_localizer
from utils.data_utils import convert_pose_opengl_to_colmap

LOG = logging.getLogger("pose_tracking_server")


def _resolve_paths(
    reconstruction_root: Path, job_root: Path, features_override: Optional[Path]
) -> Tuple[Path, Path, Path]:
    """Return (reconstruction_dir, image_dir, features_h5)."""
    rg = reconstruction_root.resolve()
    jr = job_root.resolve()

    cand_recon = [rg / "refined_sfm_combined", rg]
    reconstruction_dir = next((p for p in cand_recon if (p / "cameras.bin").is_file()), None)
    if reconstruction_dir is None:
        raise FileNotFoundError(
            f"No COLMAP model (cameras.bin) under {rg} (tried refined_sfm_combined/ and root)"
        )

    if features_override is not None and features_override.is_file():
        features_h5 = features_override.resolve()
    else:
        features_h5 = reconstruction_dir / "features.h5"
        if not features_h5.is_file():
            raise FileNotFoundError(
                f"Missing features.h5 at {features_h5}. "
                "Run global refinement (merge) or set --features_h5 / LOCALIZE_FEATURES_H5_PATH."
            )

    image_dir = jr / "datasets"
    if not image_dir.is_dir():
        raise FileNotFoundError(
            f"Database image root not found: {image_dir}. "
            "Expected ``datasets/<scan>/Frames/`` under job_root."
        )

    return reconstruction_dir, image_dir, features_h5


def _fallback_approx_cam_from_world(rec: pycolmap.Reconstruction) -> pycolmap.Rigid3d:
    """Rough pose when client sends none and we have no prior (first frame)."""
    if not rec.images:
        return pycolmap.Rigid3d()
    first = next(iter(rec.images.values()))
    return first.cam_from_world()


def _quat_wxyz_from_pycolmap_rot(rot: pycolmap.Rotation3d) -> Tuple[float, float, float, float]:
    q = np.array(rot.quat, dtype=np.float64)
    # pycolmap: Hamilton w,x,y,z
    return float(q[0]), float(q[1]), float(q[2]), float(q[3])


def _gl_pose_to_cam_from_world(
    position_gl: np.ndarray, quat_gl_wxyz: np.ndarray
) -> pycolmap.Rigid3d:
    pos_c, rot_c = convert_pose_opengl_to_colmap(position_gl, quat_gl_wxyz)
    cam_to_world = pycolmap.Rigid3d(pycolmap.Rotation3d(rot_c), pos_c)
    return cam_to_world.inverse()


@dataclass
class TrackRecord:
    success: bool
    timestamp: float
    frame_id: int
    position: Optional[List[float]] = None
    orientation: Optional[List[float]] = None  # wxyz
    processing_time_ms: float = 0.0
    num_inliers: int = 0
    num_matches: int = 0
    num_2d3d: int = 0
    error_message: str = ""


@dataclass
class ServerState:
    max_queue_size: int = 10
    image_queue: "queue.Queue[Dict[str, Any]]" = field(default_factory=queue.Queue)
    trajectory: List[TrackRecord] = field(default_factory=list)
    max_trajectory: int = 1000
    total_requests: int = 0
    successful_tracks: int = 0
    failed_tracks: int = 0
    avg_processing_time_ms: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)
    server_start: float = field(default_factory=time.time)
    # last successful cam_from_world (COLMAP) for chaining guesses
    last_cam_from_world: Optional[pycolmap.Rigid3d] = None
    localizer: Any = None
    voxel_size: float = 0.20

    def process_frame(
        self,
        bgr: np.ndarray,
        timestamp: float,
        frame_id: int,
        intrinsics: Optional[List[float]],
        approx_gl: Optional[List[float]],
    ) -> TrackRecord:
        t0 = time.perf_counter()
        rec = TrackRecord(success=False, timestamp=timestamp, frame_id=frame_id)

        if intrinsics is None or len(intrinsics) < 4:
            rec.error_message = "intrinsics [fx,fy,cx,cy] required (JSON or X-Intrinsics); add w,h for full camera model"
            rec.processing_time_ms = (time.perf_counter() - t0) * 1000.0
            return rec

        fx, fy, cx, cy = intrinsics[:4]
        h, w = bgr.shape[:2]
        if len(intrinsics) >= 6:
            iw, ih = int(intrinsics[4]), int(intrinsics[5])
            if iw > 0 and ih > 0:
                w, h = iw, ih

        if fx == fy:
            camera = pycolmap.Camera(
                model="SIMPLE_PINHOLE",
                width=int(w),
                height=int(h),
                params=[float(fx), float(cx), float(cy)],
            )
        else:
            camera = pycolmap.Camera(
                model="PINHOLE",
                width=int(w),
                height=int(h),
                params=[float(fx), float(fy), float(cx), float(cy)],
            )

        with tempfile.TemporaryDirectory(prefix="pose_track_q_") as tmpd:
            tmp_path = Path(tmpd) / "query.jpg"
            cv2.imwrite(str(tmp_path), bgr)

            with self.lock:
                prior = self.last_cam_from_world

            if approx_gl is not None and len(approx_gl) >= 7:
                pos_gl = np.array(approx_gl[0:3], dtype=np.float64)
                qw, qx, qy, qz = [float(x) for x in approx_gl[3:7]]
                quat_gl = np.array([qw, qx, qy, qz], dtype=np.float64)
                approx_cfw = _gl_pose_to_cam_from_world(pos_gl, quat_gl)
            elif prior is not None:
                approx_cfw = prior
            else:
                approx_cfw = _fallback_approx_cam_from_world(self.localizer.reconstruction)

            result = self.localizer.localize(
                image_path=tmp_path,
                camera=camera,
                approximate_cam_from_world=approx_cfw,
                query_name="query_stream.jpg",
            )

        rec.num_inliers = result.num_inliers
        rec.num_matches = result.num_matches
        rec.num_2d3d = result.num_2d3d_correspondences
        rec.processing_time_ms = (time.perf_counter() - t0) * 1000.0

        if not result.success or result.refined_cam_from_world is None:
            rec.error_message = "localization failed"
            return rec

        cfw = result.refined_cam_from_world
        wfc = cfw.inverse()
        pos = wfc.translation
        qw, qx, qy, qz = _quat_wxyz_from_pycolmap_rot(wfc.rotation)

        rec.success = True
        rec.position = [float(pos[0]), float(pos[1]), float(pos[2])]
        rec.orientation = [qw, qx, qy, qz]

        with self.lock:
            self.last_cam_from_world = cfw

        return rec


STATE: Optional[ServerState] = None


def _worker(state: ServerState) -> None:
    while True:
        item = state.image_queue.get()
        if item is None:
            break
        try:
            bgr = item["image"]
            ts = float(item["timestamp"])
            fid = int(item["frame_id"])
            intr = item.get("intrinsics")
            approx_gl = item.get("approximate_pose_gl")
            rec = state.process_frame(bgr, ts, fid, intr, approx_gl)
            with state.lock:
                if rec.success:
                    state.successful_tracks += 1
                else:
                    state.failed_tracks += 1
                done = state.successful_tracks + state.failed_tracks
                state.avg_processing_time_ms += (
                    rec.processing_time_ms - state.avg_processing_time_ms
                ) / max(done, 1)
                if rec.success:
                    state.trajectory.append(rec)
                    if len(state.trajectory) > state.max_trajectory:
                        state.trajectory.pop(0)
        finally:
            state.image_queue.task_done()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        LOG.info("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, code: int, body: Dict[str, Any]) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Intrinsics, X-Client-ID")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Intrinsics, X-Client-ID")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        assert STATE is not None
        if self.path.startswith("/api/v1/health"):
            uptime = time.time() - STATE.server_start
            self._send_json(
                200,
                {
                    "status": "healthy",
                    "server": "pose_tracking_server (SingleImageLocalizer / hloc)",
                    "version": "0.2.0",
                    "uptime_seconds": int(uptime),
                },
            )
            return
        if self.path.startswith("/api/v1/status"):
            with STATE.lock:
                qsz = STATE.image_queue.qsize()
                tr = STATE.total_requests
                ok = STATE.successful_tracks
                fail = STATE.failed_tracks
                avg = STATE.avg_processing_time_ms
            self._send_json(
                200,
                {
                    "queue_size": qsz,
                    "max_queue_size": STATE.max_queue_size,
                    "total_requests": tr,
                    "successful_tracks": ok,
                    "failed_tracks": fail,
                    "avg_processing_time_ms": avg,
                    "tracking_state": 2 if ok > 0 else 0,
                    "is_lost": fail > ok and ok == 0 and tr > 2,
                },
            )
            return
        if self.path.startswith("/api/v1/trajectory"):
            with STATE.lock:
                traj = list(STATE.trajectory)
            rows = []
            for r in traj:
                if not r.success or r.position is None:
                    continue
                rows.append(
                    {
                        "timestamp": r.timestamp,
                        "frame_id": r.frame_id,
                        "position": r.position,
                        "orientation": r.orientation,
                        "processing_time_ms": r.processing_time_ms,
                    }
                )
            self._send_json(200, {"trajectory": rows, "count": len(rows)})
            return
        if self.path == "/" or self.path.startswith("/?"):
            self._send_json(
                200,
                {
                    "message": "Refined-map pose tracking (SingleImageLocalizer)",
                    "version": "0.2.0",
                    "endpoints": [
                        {"POST /api/v1/track": "Submit image for localization"},
                        {"GET /api/v1/status": "Get server status"},
                        {"GET /api/v1/trajectory": "Get trajectory data"},
                        {"GET /api/v1/health": "Health check"},
                    ],
                },
            )
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        assert STATE is not None
        if not self.path.startswith("/api/v1/track"):
            self._send_json(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length > 0 else b""
        ct = self.headers.get("Content-Type", "")
        intrinsics: Optional[List[float]] = None
        approx_gl: Optional[List[float]] = None
        ts = time.time()
        fid = STATE.total_requests + 1
        bgr: Optional[np.ndarray] = None
        try:
            if "application/json" in ct:
                payload = json.loads(body.decode("utf-8"))
                if "timestamp" in payload:
                    ts = float(payload["timestamp"])
                if "frame_id" in payload:
                    fid = int(payload["frame_id"])
                if "intrinsics" in payload:
                    intrinsics = [float(x) for x in payload["intrinsics"]]
                if "approximate_pose_gl" in payload:
                    approx_gl = [float(x) for x in payload["approximate_pose_gl"]]
                b64 = payload.get("image_base64", "")
                raw = base64.b64decode(b64)
                arr = np.frombuffer(raw, dtype=np.uint8)
                bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            else:
                arr = np.frombuffer(body, dtype=np.uint8)
                bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
                hdr = self.headers.get("X-Intrinsics", "")
                if hdr:
                    parts = hdr.replace(",", " ").split()
                    vals = [float(x) for x in parts if x.strip()]
                    if len(vals) >= 4:
                        intrinsics = vals[:4]
        except Exception as ex:
            self._send_json(400, {"success": False, "error": str(ex)})
            return
        if bgr is None or bgr.size == 0:
            self._send_json(400, {"success": False, "error": "Failed to decode image"})
            return
        with STATE.lock:
            qfull = STATE.image_queue.qsize() >= STATE.max_queue_size
        if qfull:
            self._send_json(
                503,
                {"success": False, "error": "Server queue full, try again later"},
            )
            return
        with STATE.lock:
            STATE.total_requests += 1
        STATE.image_queue.put(
            {
                "image": bgr,
                "timestamp": ts,
                "frame_id": fid,
                "intrinsics": intrinsics,
                "approximate_pose_gl": approx_gl,
            }
        )
        self._send_json(
            200,
            {
                "success": True,
                "message": "Image queued for processing",
                "frame_id": fid,
                "timestamp": ts,
            },
        )


def main() -> None:
    global STATE
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--reconstruction_root",
        type=Path,
        required=True,
        help="e.g. <job>/refined/global (contains refined_sfm_combined/)",
    )
    ap.add_argument(
        "--job_root",
        type=Path,
        required=True,
        help="Job workspace root (contains datasets/)",
    )
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--max-queue", type=int, default=10, dest="max_queue")
    ap.add_argument(
        "--features_h5",
        type=Path,
        default=None,
        help="Override merged features.h5 (default: <reconstruction_dir>/features.h5)",
    )
    ap.add_argument("--voxel_size", type=float, default=0.20)
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    recon_dir, image_dir, features_h5 = _resolve_paths(
        args.reconstruction_root, args.job_root, args.features_h5
    )

    loc = get_or_create_localizer(
        reconstruction_dir=recon_dir,
        image_dir=image_dir,
        features_h5=features_h5,
        voxel_size=args.voxel_size,
    )

    state = ServerState(max_queue_size=max(1, args.max_queue))
    state.localizer = loc
    state.voxel_size = args.voxel_size
    STATE = state

    worker = threading.Thread(target=_worker, args=(state,), daemon=True)
    worker.start()

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    LOG.info(
        "Pose server http://%s:%s (recon=%s, images=%s, features=%s)",
        args.host,
        args.port,
        recon_dir,
        image_dir,
        features_h5,
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Shutting down")
    finally:
        state.image_queue.put(None)
        worker.join(timeout=5.0)
        httpd.server_close()


if __name__ == "__main__":
    main()
