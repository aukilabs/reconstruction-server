"""QR Code detector with RANSAC-PnP pose estimation."""

import numpy as np
import cv2
from dataclasses import dataclass
from typing import Optional, List, Tuple


@dataclass
class QRCodeDetectionResult:
    """Structure to hold QR code detection and pose estimation results."""
    
    # Whether QR code was successfully detected
    is_detected: bool = False
    
    # The 4 corner points of the QR code in camera coordinates (pixel positions)
    # List of [x, y] coordinates
    corner_points_2d: List[np.ndarray] = None
    
    # Estimated pose from camera frame
    # Rotation matrix (3x3) from QR code frame to camera frame
    rotation_matrix: Optional[np.ndarray] = None
    
    # Translation vector (3x1) - position of QR code center in camera frame
    translation_vector: Optional[np.ndarray] = None
    
    # The 3D corner positions in the QR code's local frame
    # Assumes QR code lies on Z=0 plane in its own coordinate system
    corner_points_3d: List[np.ndarray] = None
    
    # Reprojection error from RANSAC-PnP (for quality assessment)
    reprojection_error: float = 0.0
    
    # Number of inliers used in RANSAC-PnP
    num_inliers: int = 0
    
    # QR code marker text content (if decodable)
    marker_text: str = ""
    
    # Success indicator for pose estimation
    pose_estimated: bool = False
    
    def __post_init__(self):
        """Initialize list fields if None."""
        if self.corner_points_2d is None:
            self.corner_points_2d = []
        if self.corner_points_3d is None:
            self.corner_points_3d = []


class QRCodeDetector:
    """QR Code detector with pose estimation using RANSAC-PnP."""
    
    def __init__(self):
        """Initialize the QR code detector."""
        self.qr_detector = cv2.QRCodeDetector()
    
    def detect_and_estimate_pose(
        self,
        image: np.ndarray,
        camera_matrix: np.ndarray,
        dist_coeffs: np.ndarray,
        qr_code_size_m: float = 0.1,
    ) -> QRCodeDetectionResult:
        """
        Detect QR code in the image and estimate its pose using RANSAC-PnP.
        
        Args:
            image: Input image (grayscale or BGR)
            camera_matrix: 3x3 camera intrinsic matrix [fx 0 cx; 0 fy cy; 0 0 1]
            dist_coeffs: Distortion coefficients (4x1 or 5x1)
            qr_code_size_mm: Size of the QR code in millimeters (assumed square)
        
        Returns:
            QRCodeDetectionResult with detected positions and pose
        """
        result = QRCodeDetectionResult()
        
        # Convert to grayscale if needed
        if len(image.shape) == 3:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        else:
            gray = image
        
        # Detect QR code
        ret_val, decoded_info, points, straight_qr = self.qr_detector.detectAndDecodeMulti(gray)
        
        if not ret_val or len(points) == 0:
            result.is_detected = False
            return result
        
        # Use the first detected QR code
        corners = points[0].astype(np.float32)  # Shape: (4, 2)
        
        # Store marker text if available
        if decoded_info and len(decoded_info) > 0:
            result.marker_text = decoded_info[0]
        
        # Convert 2D corners to list of numpy arrays
        result.corner_points_2d = [corners[i].astype(np.float64) for i in range(4)]
        result.is_detected = True
        
        # Create 3D corners of the QR code (assuming it's planar at Z=0 in its frame)
        # The QR code is centered at origin, extending from -size/2 to +size/2
        result.corner_points_3d = self._create_qr_code_3d_corners(qr_code_size_m)
        
        # Estimate pose using RANSAC-PnP
        object_points = np.array(result.corner_points_3d, dtype=np.float32)
        image_points = corners
        
        # Use solvePnPRansac for robust pose estimation
        success, rvec, tvec, inliers = cv2.solvePnPRansac(
            objectPoints=object_points,
            imagePoints=image_points,
            cameraMatrix=camera_matrix,
            distCoeffs=dist_coeffs,
            useExtrinsicGuess=False,
            iterationsCount=100,
            reprojectionError=8.0,
            confidence=0.99,
        )
        
        if success:
            result.pose_estimated = True
            
            # Convert rotation vector to rotation matrix
            rotation_matrix, _ = cv2.Rodrigues(rvec)
            result.rotation_matrix = rotation_matrix.astype(np.float64)
            result.translation_vector = tvec.astype(np.float64)
            
            # Count inliers
            if inliers is not None:
                result.num_inliers = len(inliers)
            else:
                result.num_inliers = len(object_points)
            
            # Compute reprojection error
            result.reprojection_error = self._compute_reprojection_error(
                object_points,
                image_points,
                camera_matrix,
                result.rotation_matrix,
                result.translation_vector,
                dist_coeffs,
            )
        else:
            result.pose_estimated = False
        
        return result
    
    def _create_qr_code_3d_corners(self, qr_code_size_m: float) -> List[np.ndarray]:
        """
        Create 3D corner positions of the QR code in its local frame.
        
        Assumes the QR code is planar, centered at origin, lying on Z=0 plane.
        Corners are at the corners of a square of size qr_code_size_m x qr_code_size_m.
        
        Args:
            qr_code_size_mm: Size of the QR code in millimeters
        
        Returns:
            List of 4 corner points as numpy arrays in 3D space
        """
        half_size = qr_code_size_m / 2.0
        
        # Define corners: top-left, top-right, bottom-right, bottom-left
        # In the QR code frame with Z pointing outward
        corners = [
            np.array([-half_size, -half_size, 0.0], dtype=np.float32),  # top-left
            np.array([half_size, -half_size, 0.0], dtype=np.float32),   # top-right
            np.array([half_size, half_size, 0.0], dtype=np.float32),    # bottom-right
            np.array([-half_size, half_size, 0.0], dtype=np.float32),   # bottom-left
        ]
        
        return corners
    
    def _compute_reprojection_error(
        self,
        object_points: np.ndarray,
        image_points: np.ndarray,
        camera_matrix: np.ndarray,
        rotation_matrix: np.ndarray,
        translation_vector: np.ndarray,
        dist_coeffs: np.ndarray,
    ) -> float:
        """
        Compute the mean reprojection error.
        
        Args:
            object_points: 3D points in object frame (Nx3)
            image_points: 2D points in image frame (Nx2)
            camera_matrix: Camera intrinsic matrix (3x3)
            rotation_matrix: Rotation matrix from object to camera (3x3)
            translation_vector: Translation vector from object to camera (3x1)
            dist_coeffs: Distortion coefficients
        
        Returns:
            Mean reprojection error
        """
        # Convert rotation matrix to rotation vector for projectPoints
        rvec, _ = cv2.Rodrigues(rotation_matrix)
        
        # Project 3D points to image plane
        projected_points, _ = cv2.projectPoints(
            object_points,
            rvec,
            translation_vector,
            camera_matrix,
            dist_coeffs,
        )
        
        # Reshape projected points to (N, 2)
        projected_points = projected_points.reshape(-1, 2)
        
        # Compute Euclidean distance between projected and observed points
        errors = np.linalg.norm(projected_points - image_points, axis=1)
        mean_error = np.mean(errors)
        
        return float(mean_error)


def detect_and_estimate_qr_code_pose(
    image: np.ndarray,
    camera_matrix: np.ndarray,
    dist_coeffs: np.ndarray,
    qr_code_size_mm: float = 100.0,
) -> QRCodeDetectionResult:
    """
    Convenience function to detect QR code and estimate its pose.
    
    Args:
        image: Input image (grayscale or BGR)
        camera_matrix: 3x3 camera intrinsic matrix
        dist_coeffs: Distortion coefficients
        qr_code_size_mm: Size of the QR code in millimeters
    
    Returns:
        QRCodeDetectionResult with detection and pose estimation results
    """
    detector = QRCodeDetector()
    return detector.detect_and_estimate_pose(
        image, camera_matrix, dist_coeffs, qr_code_size_mm
    )


@dataclass
class QRCodeDetection3DResult:
    """Structure to hold QR code detection with rough 3D corner projections."""
    
    # Whether QR code was successfully detected
    is_detected: bool = False
    
    # The 4 corner points of the QR code in camera coordinates (pixel positions)
    corner_points_2d: List[np.ndarray] = None
    
    # Rough 3D corner positions in camera frame
    # Assumes QR code is at unit distance (Z=1) for scaling
    corner_points_3d: List[np.ndarray] = None
    
    # QR code marker text content (if decodable)
    marker_text: str = ""
    
    def __post_init__(self):
        """Initialize list fields if None."""
        if self.corner_points_2d is None:
            self.corner_points_2d = []
        if self.corner_points_3d is None:
            self.corner_points_3d = []


def detect_qr_code_3d_corners(
    image: np.ndarray,
    camera_matrix: np.ndarray,
    dist_coeffs: np.ndarray,
) -> QRCodeDetection3DResult:
    """
    Detect QR code and project rough 3D corner positions without knowing QR code size.
    
    This function detects QR code corners in 2D and projects them to rough 3D positions
    by assuming the QR code lies on a plane at unit distance (Z=1) in camera coordinates.
    The resulting 3D points maintain correct relative positions and directions but are
    scaled (actual distance unknown).
    
    Args:
        image: Input image (grayscale or BGR)
        camera_matrix: 3x3 camera intrinsic matrix [fx 0 cx; 0 fy cy; 0 0 1]
        dist_coeffs: Distortion coefficients (4x1 or 5x1)
    
    Returns:
        QRCodeDetection3DResult with detected 2D corners and rough 3D projections
    """
    result = QRCodeDetection3DResult()
    
    # Convert to grayscale if needed
    if len(image.shape) == 3:
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    else:
        gray = image
    
    # Detect QR code
    qr_detector = cv2.QRCodeDetector()
    ret_val, decoded_info, points, straight_qr = qr_detector.detectAndDecodeMulti(gray)
    
    if not ret_val or len(points) == 0:
        result.is_detected = False
        return result
    
    # Use the first detected QR code
    corners_2d = points[0].astype(np.float32)  # Shape: (4, 2)
    
    # Store marker text if available
    if decoded_info and len(decoded_info) > 0:
        result.marker_text = decoded_info[0]
    
    # Store 2D corners
    result.corner_points_2d = [corners_2d[i].astype(np.float64) for i in range(4)]
    result.is_detected = True
    
    # Undistort the 2D points
    undistorted_corners = cv2.undistortPoints(
        corners_2d.reshape(-1, 1, 2),  # Shape: (4, 1, 2)
        camera_matrix,
        dist_coeffs,
        None,
        camera_matrix
    ).reshape(-1, 2)  # Shape: (4, 2)
    
    # Extract camera intrinsics
    fx = camera_matrix[0, 0]
    fy = camera_matrix[1, 1]
    cx = camera_matrix[0, 2]
    cy = camera_matrix[1, 2]
    
    # Convert to normalized camera coordinates and project to 3D at Z=1
    result.corner_points_3d = []
    for corner in undistorted_corners:
        # Convert from pixel coordinates to normalized camera coordinates
        x_norm = (corner[0] - cx) / fx
        y_norm = (corner[1] - cy) / fy
        
        # Project to 3D assuming Z=1 (unit distance)
        # This gives rough 3D positions that maintain relative geometry
        point_3d = np.array([x_norm, y_norm, 1.0], dtype=np.float64)
        result.corner_points_3d.append(point_3d)
    
    return result
