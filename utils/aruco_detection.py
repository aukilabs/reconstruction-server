from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import cv2
import numpy as np
from scipy.spatial.transform import Rotation as scipy_Rotation


ARUCO_ID_PREFIX = "aruco_"
DEFAULT_ARUCO_DICT = "DICT_4X4_250"
DEFAULT_MARKER_SIZE = 0.15


def _opencv_pose_to_opengl(
    rvec: np.ndarray,
    tvec: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """Convert OpenCV camera-space pose to OpenGL convention."""
    rmat_cv, _ = cv2.Rodrigues(rvec.reshape(3, 1))
    flip = np.diag([1.0, -1.0, -1.0])
    rmat_gl = flip @ rmat_cv @ flip
    tvec_gl = flip @ tvec.reshape(3)
    quat_gl_xyzw = scipy_Rotation.from_matrix(rmat_gl).as_quat()
    return tvec_gl, quat_gl_xyzw


def _load_frame_timestamps(
    frames_csv_path: Path,
    use_frames_from_video: bool,
    scan_folder_name: str,
) -> Dict[str, float]:
    """Load filename-to-timestamp mapping from Frames.csv."""
    timestamps_per_filename: Dict[str, float] = {}
    with frames_csv_path.open(newline="") as csvfile:
        frame_index = 0
        for row in csv.reader(csvfile):
            if not row:
                continue
            timestamp_s = float(row[0])
            filename = (
                f"{scan_folder_name}_{frame_index:06d}.jpg"
                if use_frames_from_video
                else row[1]
            )
            frame_index += 1
            timestamps_per_filename[filename] = timestamp_s
    return timestamps_per_filename


def _load_intrinsics(intrinsics_csv_path: Path) -> Dict[int, np.ndarray]:
    """Load timestamp-to-camera intrinsics mapping from CameraIntrinsics.csv."""
    intrinsics_per_timestamp_ns: Dict[int, np.ndarray] = {}
    with intrinsics_csv_path.open(newline="") as csvfile:
        for row in csv.reader(csvfile):
            if not row:
                continue
            timestamp_ns = round(float(row[0]) * 1e9)
            fx, fy = float(row[1]), float(row[2])
            cx, cy = float(row[3]), float(row[4])
            camera_matrix = np.array(
                [[fx, 0.0, cx], [0.0, fy, cy], [0.0, 0.0, 1.0]],
                dtype=np.float64,
            )
            intrinsics_per_timestamp_ns[timestamp_ns] = camera_matrix
    return intrinsics_per_timestamp_ns


def _iter_frame_images(images_dir: Path) -> Iterable[Path]:
    for path in sorted(images_dir.iterdir()):
        if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png"}:
            yield path


def detect_aruco_in_frames(
    images_dir: Path,
    frames_csv_path: Path,
    intrinsics_csv_path: Path,
    output_csv_path: Path,
    use_frames_from_video: bool,
    scan_folder_name: str,
    logger: Optional[logging.Logger] = None,
    marker_size_meters: float = DEFAULT_MARKER_SIZE,
    aruco_dict_name: str = DEFAULT_ARUCO_DICT,
) -> None:
    """
    Detect ArUco markers in extracted frames and write detections CSV.

    Output row format:
    timestamp_s, short_id, px, py, pz, rx, ry, rz, rw, c1x, c1y, c2x, c2y, c3x, c3y, c4x, c4y
    """
    active_logger = logger or logging.getLogger(__name__)

    if not images_dir.exists():
        active_logger.warning("Images directory does not exist: %s", images_dir)
        output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        output_csv_path.write_text("")
        return

    if not frames_csv_path.exists():
        active_logger.warning("Frames CSV not found: %s", frames_csv_path)
        output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        output_csv_path.write_text("")
        return

    if not intrinsics_csv_path.exists():
        active_logger.warning("CameraIntrinsics CSV not found: %s", intrinsics_csv_path)
        output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        output_csv_path.write_text("")
        return

    timestamps_per_filename = _load_frame_timestamps(
        frames_csv_path=frames_csv_path,
        use_frames_from_video=use_frames_from_video,
        scan_folder_name=scan_folder_name,
    )
    intrinsics_per_timestamp_ns = _load_intrinsics(intrinsics_csv_path)

    if not hasattr(cv2, "aruco"):
        active_logger.warning("cv2.aruco is unavailable; skipping ArUco detection.")
        output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        output_csv_path.write_text("")
        return

    aruco_dict_id = getattr(cv2.aruco, aruco_dict_name, None)
    if aruco_dict_id is None:
        active_logger.warning(
            "Unknown ArUco dictionary %s; defaulting to %s",
            aruco_dict_name,
            DEFAULT_ARUCO_DICT,
        )
        aruco_dict_id = getattr(cv2.aruco, DEFAULT_ARUCO_DICT)

    aruco_dict = cv2.aruco.getPredefinedDictionary(aruco_dict_id)
    detector = cv2.aruco.ArucoDetector(aruco_dict, cv2.aruco.DetectorParameters())

    half_size = marker_size_meters * 0.5
    object_points = np.array(
        [
            [-half_size, half_size, 0.0],
            [half_size, half_size, 0.0],
            [half_size, -half_size, 0.0],
            [-half_size, -half_size, 0.0],
        ],
        dtype=np.float64,
    )
    dist_coeffs = np.zeros((4, 1), dtype=np.float64)

    rows = []
    frames_processed = 0
    detections_count = 0

    for image_path in _iter_frame_images(images_dir):
        timestamp_s = timestamps_per_filename.get(image_path.name)
        if timestamp_s is None:
            continue

        timestamp_ns = round(timestamp_s * 1e9)
        camera_matrix = intrinsics_per_timestamp_ns.get(timestamp_ns)
        if camera_matrix is None:
            continue

        frame = cv2.imread(str(image_path))
        if frame is None:
            continue

        frames_processed += 1
        corners, ids, _ = detector.detectMarkers(frame)
        if ids is None or len(ids) == 0:
            continue

        for detection_index, marker_id in enumerate(ids.flatten().tolist()):
            image_points = corners[detection_index].reshape(4, 2).astype(np.float64)
            success, rvec, tvec = cv2.solvePnP(
                object_points,
                image_points,
                camera_matrix,
                dist_coeffs,
                flags=cv2.SOLVEPNP_IPPE_SQUARE,
            )
            if not success:
                continue

            pos_gl, quat_gl = _opencv_pose_to_opengl(rvec, tvec)

            # Timestamp de-dup for multi-marker-per-frame.
            detection_timestamp_s = timestamp_s + (detection_index * 1e-9)
            row = [
                f"{detection_timestamp_s:.9f}",
                f"{ARUCO_ID_PREFIX}{marker_id}",
                float(pos_gl[0]),
                float(pos_gl[1]),
                float(pos_gl[2]),
                float(quat_gl[0]),
                float(quat_gl[1]),
                float(quat_gl[2]),
                float(quat_gl[3]),
                float(image_points[0, 0]),
                float(image_points[0, 1]),
                float(image_points[1, 0]),
                float(image_points[1, 1]),
                float(image_points[2, 0]),
                float(image_points[2, 1]),
                float(image_points[3, 0]),
                float(image_points[3, 1]),
            ]
            rows.append(row)
            detections_count += 1

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with output_csv_path.open("w", newline="") as csvfile:
        csv.writer(csvfile).writerows(rows)

    active_logger.info(
        "ArUco detection completed: frames=%d detections=%d output=%s",
        frames_processed,
        detections_count,
        output_csv_path,
    )
