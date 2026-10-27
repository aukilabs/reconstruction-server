"""Pose and rotation math: portal orientation checks, pose averaging.

Frame convention conversions are in reconstruction.geometry.frames."""
import pycolmap
import numpy as np
from scipy.spatial.transform import Rotation as scipy_Rotation
from numpy.linalg import norm
from numpy import arccos, rad2deg


def is_portal_almost_flat(rotation_matrix, angle_threshold=20):
    current_z = rotation_matrix[:, 2]
    downwards = np.array([-1, 0, 0])
    angle = np.arccos(np.clip(np.dot(current_z, downwards), -1.0, 1.0))
    return np.rad2deg(angle) < angle_threshold


def is_portal_near_floor_height(position, height_threshold=0.5):
    return np.abs(position[0]) < height_threshold


def is_floor_portal(pose, height_threshold=0.5, angle_threshold=20):
    return is_portal_near_floor_height(pose.translation, height_threshold) and \
           is_portal_almost_flat(pose.rotation.matrix(), angle_threshold)


def flatten_portal_rotation(rotation_matrix, angle_threshold=20):
    if rotation_matrix.shape != (3, 3):
        raise ValueError("Input must be a 3x3 matrix")

    # Extract the current Z-axis from the rotation matrix
    current_z = rotation_matrix[:, 2]
    downwards = np.array([-1, 0, 0])

    # If clearly not flat don't change
    if not is_portal_almost_flat(rotation_matrix, angle_threshold):
        return rotation_matrix

    # Compute the rotation axis to align current Z with desired Z
    rotation_axis = np.cross(current_z, downwards)
    rotation_axis_norm = np.linalg.norm(rotation_axis)

    if rotation_axis_norm > 1e-6:  # Avoid division by zero
        rotation_axis /= rotation_axis_norm

        # Compute the angle to rotate current Z to desired Z
        angle = np.arccos(np.clip(np.dot(current_z, downwards), -1.0, 1.0))

        # Create the rotation matrix to align Z
        K = np.array([
            [0, -rotation_axis[2], rotation_axis[1]],
            [rotation_axis[2], 0, -rotation_axis[0]],
            [-rotation_axis[1], rotation_axis[0], 0]
        ])
        R_align_z = np.eye(3) + np.sin(angle) * K + (1 - np.cos(angle)) * np.dot(K, K)
        # Apply the alignment rotation
        rotation_matrix = np.dot(R_align_z, rotation_matrix)

    # Adjust X and Y to ensure orthonormality
    x_axis = rotation_matrix[:, 0]
    y_axis = np.cross(downwards, x_axis)
    y_axis /= np.linalg.norm(y_axis)
    x_axis = np.cross(y_axis, downwards)
    x_axis /= np.linalg.norm(x_axis)

    # Reconstruct the rotation matrix
    flattened_rotation = np.column_stack((x_axis, y_axis, downwards))
    return flattened_rotation


def rectify_portal_pose(qr_pose, angle_threshold=20, height_threshold=0.5):
    pos = qr_pose.translation
    rot3d = qr_pose.rotation

    if is_portal_almost_flat(rot3d.matrix(), angle_threshold):
        rot3d = pycolmap.Rotation3d(flatten_portal_rotation(rot3d.matrix(), angle_threshold))

        # If flat and also near floor, snap height too. But NOT snapping desk portals to floor!
        if is_portal_near_floor_height(pos, height_threshold):
            pos = pos.copy() # avoid modifying input pose
            pos[0] = 0.0

    return pycolmap.Rigid3d(rot3d, pos)


def quaternion_to_rotation_matrix(q):
    x, y, z, w = q
    return np.array([
        [1 - 2*(y**2 + z**2), 2*(x*y - z*w), 2*(x*z + y*w)],
        [2*(x*y + z*w), 1 - 2*(x**2 + z**2), 2*(y*z - x*w)],
        [2*(x*z - y*w), 2*(y*z + x*w), 1 - 2*(x**2 + y**2)]
    ])


def average_rotation_matrices(rotation_matrices):
    R_avg = np.mean(rotation_matrices, axis=0)
    U, _, Vt = np.linalg.svd(R_avg)
    R_best_fit = np.dot(U, Vt)
    return R_best_fit


def rotation_matrix_to_quaternion(R):
    r = scipy_Rotation.from_matrix(R)
    return r.as_quat()


def average_quaternions_svd(quaternions):
    # Convert quaternions to rotation matrices
    rotation_matrices = np.array([quaternion_to_rotation_matrix(q) for q in quaternions])

    # Compute the average rotation matrix
    R_avg = average_rotation_matrices(rotation_matrices)

    # Convert the average rotation matrix back to a quaternion
    avg_quaternion = rotation_matrix_to_quaternion(R_avg)

    return avg_quaternion


def mean_pose(poses):
    if len(poses) == 1:
        return poses[0]

    pos = np.mean(np.array([pose.translation for pose in poses]), axis=0)
    #quat = np.array(poses[0].rotation.quat) # TODO select best fit for rotation also
    #quat_sum = np.sum(np.array([pose.rotation.quat for pose in poses]), axis=0)
    #quat = quat_sum / norm(quat_sum)
    quat = average_quaternions_svd(np.array([pose.rotation.quat for pose in poses]))
    return pycolmap.Rigid3d(pycolmap.Rotation3d(quat), pos)


def vec3_angle(v, w):
    value = v.dot(w)/(norm(v)*norm(w))

    # Rounding errors can be slightly outside range of arccos. Clamp to range.
    if value > 1.0 or value < -1.0:
        if np.abs(value) < 1.000001:
            value = np.clip(value, -1, 1)
        else:
            raise Exception(f"BAD VALUE in arccos: {value}. Must be within -1 .. 1 range")

    return rad2deg(arccos(value))


def cam_center_from_extrinsics(R: np.ndarray, t: np.ndarray) -> np.ndarray:
    # world->cam: x_c = R X + t; center C satisfies R C + t = 0 => C = -R^T t
    return -R.T @ t


def transform_with_scale(alignment_transform: pycolmap.Sim3d, pose: pycolmap.Rigid3d) -> pycolmap.Rigid3d:
    pose = pycolmap.Sim3d(1.0, pose.rotation, pose.translation)
    pose = alignment_transform * pose
    return pycolmap.Rigid3d(pose.rotation, pose.translation)
