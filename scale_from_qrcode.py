#!/usr/bin/env python3
"""Scale estimation from QR code detections using COLMAP model data."""

import argparse
import os
from pathlib import Path

import cv2
import numpy as np
import pycolmap

from utils.marker_utils import (
    QRCodeDetection3DResult,
    QRCodeDetectionResult,
    detect_qr_code_3d_corners,
    detect_and_estimate_qr_code_pose,
)


def load_colmap_reconstruction(model_dir: Path) -> pycolmap.Reconstruction:
    """Load COLMAP reconstruction from model folder."""
    reconstruction = pycolmap.Reconstruction()
    reconstruction.read(model_dir)
    return reconstruction


def resolve_image_path(model_dir: str, image_name: str) -> str:
    """Try common locations for image path inside a COLMAP model workspace."""
    candidates = [
        os.path.join(model_dir, image_name),
        os.path.join(model_dir, "images", image_name),
        os.path.join(model_dir, "../images", image_name),
        os.path.join(model_dir, image_name.lstrip("/")),
    ]
    for p in candidates:
        p = os.path.expanduser(p)
        if os.path.exists(p):
            return p
    return None


def compute_scale_from_3d_corners(
    corners_3d: np.ndarray,
    qr_code_size_m: float,
) -> float:
    """Compute rough scale factor from normalized 3D corner positions."""
    if corners_3d.shape != (4, 3):
        raise ValueError("Expected 4 corner 3D points in shape (4,3)")

    # The detection output is on Z=1 plane; distance between points is relative to this scale.
    print(f"corners at camera frame:\n{corners_3d}")
    edge_lengths = [
        np.linalg.norm(corners_3d[i] - corners_3d[(i + 1) % 4])
        for i in range(4)
    ]
    mean_edge_length = float(np.mean(edge_lengths))
    if mean_edge_length <= 0:
        raise ValueError("Invalid normalized QR corner geometry.")

    print(f"edge lengths:\n{edge_lengths}")
    print(f"qr_code_size_m: {qr_code_size_m}")

    scale = qr_code_size_m / mean_edge_length
    return scale


def process_colmap_model(
    model_dir: Path,
    image_dir: Path,
    qr_code_size_m: float,
):
    """Process images in the COLMAP model and estimate QR scale.

    Returns:
        Tuple of (detected_scales, first_qr_info) where first_qr_info is either None
        or a dict with keys: image_id, image_name, qr_center_cam (3D in camera frame),
        image_object, reconstruction.
    """
    reconstruction = load_colmap_reconstruction(model_dir)

    if reconstruction.num_images == 0:
        raise RuntimeError(f"No images found in COLMAP model at {model_dir}")

    print(f"Loaded COLMAP model with {reconstruction.num_images} images")

    detected_scales = []
    first_qr_info = None

    for image in reconstruction.images.values():
        image_name = image.name
        # image_path = resolve_image_path(model_dir, image_name)
        image_path = os.path.join(image_dir, image_name)
        if image_path is None:
            print(f"Warning: file {image_name} not found under {model_dir}; skipping")
            continue

        image_bgr = cv2.imread(image_path)
        if image_bgr is None:
            print(f"Warning: could not read {image_path}; skipping")
            continue

        camera = reconstruction.cameras[image.camera_id]
        camera_matrix = np.array(camera.calibration_matrix(), dtype=np.float64)

        # pycolmap camera.params may include distortion coefficients after first 4
        dist_coeffs = np.zeros((5, 1), dtype=np.float64)
        if hasattr(camera, "params") and len(camera.params) >= 5:
            # typically [fx, fy, cx, cy, k1, k2, ...]
            for i in range(min(5, len(camera.params))):
                dist_coeffs[i, 0] = camera.params[i] if i >= 4 else 0.0

        # Detect QR using rough 3D projection (unit depth, no size known)
        qr3d: QRCodeDetection3DResult = detect_qr_code_3d_corners(
            image_bgr, camera_matrix, dist_coeffs
        )

        if not qr3d.is_detected:
            # print(f"{image_name}: QR not detected.")
            continue
        if qr3d.marker_text == '':
            continue
        # print(f"{image_name}: QR detected with marker text '{qr3d.marker_text}'")
        try:
            scale = compute_scale_from_3d_corners(np.array(qr3d.corner_points_3d), qr_code_size_m)
        except Exception as exc:
            print(f"{image_name}: failed scale cacl: {exc}")
            continue

        detected_scales.append(scale)
        print(f"{image_name}: QR '{qr3d.marker_text}' detected, scale={scale:.6f} (m/unit)")
        
        # Save first QR detection info if not already saved
        if first_qr_info is None:
            first_qr_info = {
                'image_id': image.image_id,
                'image_name': image_name,
                'image': image,
                'image_path': image_path,
                'camera_matrix': camera_matrix,
                'dist_coeffs': dist_coeffs,
                'qr_code_size_m': qr_code_size_m,
            }

        qrr: QRCodeDetectionResult = detect_and_estimate_qr_code_pose(image_bgr, camera_matrix, dist_coeffs, 0.1)

        print(f"estiamted translation:\n{qrr.translation_vector}")
        print(f"estiamted rotation:\n{qrr.rotation_matrix}")

    return detected_scales, first_qr_info


def apply_scale_to_reconstruction(
    reconstruction: pycolmap.Reconstruction,
    scale: float,
) -> None:
    """Apply scale to all camera positions and 3D points in reconstruction.
    
    Uses the Reconstruction.transform() method with a Sim3d similarity transform.
    
    Args:
        reconstruction: The COLMAP reconstruction to modify in-place.
        scale: Scale factor to apply to distances.
    """
    print(f"\nApplying scale factor {scale:.6f} to reconstruction...")
    
    # Create a Sim3d similarity transform with identity rotation, zero translation, and scale
    rotation_identity = pycolmap.Rotation3d()
    translation_zero = np.array([0.0, 0.0, 0.0])
    sim3d_transform = pycolmap.Sim3d(scale, rotation_identity, translation_zero)
    
    # Apply the transformation to the entire reconstruction
    reconstruction.transform(sim3d_transform)
    
    num_images = len(reconstruction.images)
    num_points3d = len(reconstruction.points3D)
    print(f"Scaled {num_images} images and {num_points3d} 3D points.")


def set_first_qr_as_origin(
    reconstruction: pycolmap.Reconstruction,
    qr_info: dict,
) -> None:
    """Set the first detected QR code center as world origin (0, 0, 0).

    Redetect the QR code in the first image and estimate its full 3D pose.
    The resulting world coordinate of the QR center is then moved to origin.

    Args:
        reconstruction: The COLMAP reconstruction to modify in-place.
        qr_info: Dictionary with keys from process_colmap_model including:
            - image: pycolmap.Image
            - image_path: str
            - camera_matrix: np.ndarray
            - dist_coeffs: np.ndarray
            - qr_code_size_m: float
    """
    image = qr_info["image"]
    image_path = qr_info["image_path"]
    camera_matrix = qr_info["camera_matrix"]
    dist_coeffs = qr_info["dist_coeffs"]
    qr_code_size_m = qr_info["qr_code_size_m"]

    print(f"\nRedetecting QR code in first detected image for origin alignment...")
    print(f"  Image: {image.name}")

    image_bgr = cv2.imread(image_path)
    if image_bgr is None:
        raise FileNotFoundError(f"Cannot read first QR image: {image_path}")

    # Pose estimation from QR corners via PnP (size required)
    qr_pose: QRCodeDetectionResult = detect_and_estimate_qr_code_pose(
        image_bgr,
        camera_matrix,
        dist_coeffs,
        qr_code_size_m * 1000.0,  # function expects mm
    )

    if not qr_pose.is_detected or not qr_pose.pose_estimated:
        raise RuntimeError("QR pose could not be estimated on first QR image")

    # QR center in camera coordinates: use tvec directly
    qr_center_cam = np.squeeze(qr_pose.translation_vector)  # shape (3,)
    print(f"  QR center in camera frame: {qr_center_cam}")

    # Transform this point to world coordinates using matrix multiplication
    if not image.has_pose:
        raise ValueError(f"Image {image.name} has no pose information")

    cam_from_world = image.cam_from_world()
    world_from_cam = cam_from_world.inverse()

    # Rigid3d.matrix() returns 3x4; use homogeneous coordinate conversion
    world_from_cam_mat = np.array(world_from_cam.matrix())  # (3, 4)
    qr_center_cam_h = np.append(qr_center_cam, 1.0)  # (4,)
    qr_center_world = world_from_cam_mat.dot(qr_center_cam_h)  # (3,)

    print(f"  QR center in world frame: {qr_center_world}")

    # Translate reconstruction so QR center lands at world origin
    rotation_identity = pycolmap.Rotation3d()
    translation = -qr_center_world
    sim3d_translation = pycolmap.Sim3d(1.0, rotation_identity, translation)

    reconstruction.transform(sim3d_translation)

    print("  Applied transformation to set QR as origin.")


def save_scaled_model(reconstruction: pycolmap.Reconstruction, output_dir: Path) -> None:
    """Save scaled reconstruction to output directory.
    
    Args:
        reconstruction: The COLMAP reconstruction to save.
        output_dir: Path to output directory.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\nSaving scaled model to {output_dir}...")
    reconstruction.write(str(output_dir))
    print("Model saved successfully.")



def main():
    parser = argparse.ArgumentParser(description="Estimate scene scale from QR code and COLMAP model")
    parser.add_argument(
        "--model_dir",
        type=Path,
        required=True,
        help="Path to COLMAP model folder containing cameras.bin, images.bin, points3D.bin",
    )
    parser.add_argument(
        "--image_dir",
        type=Path,
        required=True,
        help="Path to directory containing image files",
    )
    parser.add_argument(
        "--qr_code_size_m",
        required=True,
        type=float,
        help="Real-world QR code side length in meters",
    )
    parser.add_argument(
        "--output_dir",
        type=Path,
        required=True,
        help="Output directory for scaled COLMAP model",
    )
    parser.add_argument(
        "--set_qr_origin",
        action="store_true",
        help="Set the first detected QR code as world origin (0, 0, 0)",
    )
    args = parser.parse_args()

    model_dir = args.model_dir
    if not os.path.isdir(model_dir):
        raise FileNotFoundError(f"Model directory not found: {model_dir}")

    image_dir = args.image_dir
    if not os.path.isdir(image_dir):
        raise FileNotFoundError(f"Image directory not found: {image_dir}")

    # Detect QR codes and collect scales
    detected_scales, first_qr_info = process_colmap_model(model_dir, image_dir, args.qr_code_size_m)
    
    if not detected_scales:
        print("No QR codes detected in any image. Cannot estimate scale.")
        return
    
    # Compute average scale
    avg_scale = float(np.mean(detected_scales))
    print(f"\n=== Scale Summary ===")
    print(f"Detected {len(detected_scales)} scale(s)")
    print(f"Min scale: {min(detected_scales):.6f} m/unit")
    print(f"Max scale: {max(detected_scales):.6f} m/unit")
    print(f"Average scale: {avg_scale:.6f} m/unit")
    
    # Load reconstruction and apply average scale
    reconstruction = load_colmap_reconstruction(model_dir)
    apply_scale_to_reconstruction(reconstruction, avg_scale)
    
    # # Optionally set QR code as origin
    # if args.set_qr_origin and first_qr_info is not None:
    #     set_first_qr_as_origin(reconstruction, first_qr_info)

    # Save scaled (and optionally origin-aligned) model into output directory
    save_scaled_model(reconstruction, args.output_dir)
    
if __name__ == "__main__":
    main()