"""Frame conversions specific to Auki SDK capture sessions.

Auki pose logs use natural parent-child semantics: base_link -> camera data satisfies
p_base_link = R * p_camera + t, which is pycolmap's own A_from_B. So pose log samples load
straight into pycolmap.Rigid3d and chain with plain `*`; only relabelling an axis convention
goes through auki_geometry (reconstruction.geometry.frames). The frames involved are listed
in docs/coordinate-conventions.md.
"""
import numpy as np
import pycolmap

from reconstruction.geometry.frames import (
    AUKI_ANCHOR,
    AUKI_ANCHOR_MARKER,
    COLMAP_WORLD,
    convert_transform_target_convention,
)


def _convert_target(pose: pycolmap.Rigid3d, from_frame: dict, to_frame: dict) -> pycolmap.Rigid3d:
    position, quaternion = convert_transform_target_convention(
        pose.translation, pose.rotation.quat, from_frame, to_frame
    )
    return pycolmap.Rigid3d(pycolmap.Rotation3d(quaternion), position)


def convert_to_colmap_world(world_from_x: pycolmap.Rigid3d, world_frame: dict) -> pycolmap.Rigid3d:
    """Re-express a session's world_from_X pose in COLMAP_WORLD, leaving X's own axes alone.

    world_frame: the "world" frame as this session's registry declares it (load_auki_session
    reads it rather than assuming it, since another robot or SDK build may declare other
    axes). World is the target side of world_from_X, so only the target is relabelled:
    relabelling both sides (convert_pose_convention) would also change X's axes, and a camera's
    are already COLMAP's.
    """
    return _convert_target(world_from_x, world_frame, COLMAP_WORLD)


def convert_qr0_to_auki(qr0_from_x: pycolmap.Rigid3d) -> pycolmap.Rigid3d:
    """Re-express a pose given relative to a QR marker's own frame (the output of
    convert_colmap_pose_to_qr_origin, not a COLMAP-world pose) in the Auki anchor convention
    (forward, up, right), leaving X's own axes alone. AUKI_ANCHOR_MARKER says how the marker
    frame is declared for this, and why that matters for wall markers."""
    return _convert_target(qr0_from_x, AUKI_ANCHOR_MARKER, AUKI_ANCHOR)


# The target-side relabelling is one fixed rotation whatever the pose; derived from the
# identity pose so it follows the frame declarations.
_QR0_TO_AUKI_ROTATION = np.array(
    convert_qr0_to_auki(pycolmap.Rigid3d(pycolmap.Rotation3d(), np.zeros(3))).rotation.matrix()
)


def convert_colmap_pose_to_qr_origin(
    world_from_x: pycolmap.Rigid3d, qr0_from_world: pycolmap.Rigid3d
) -> pycolmap.Rigid3d:
    """Re-origin a COLMAP-world pose at a QR marker (qr0_from_world: that marker's mean world
    pose, inverted). The result is in the marker's own axes, which are pnp-lab's (right, up,
    out of the face; OpenGL's axes), so no relabelling is needed for that convention."""
    return qr0_from_world * world_from_x


def convert_colmap_pose_to_qr_origin_auki(
    world_from_x: pycolmap.Rigid3d, qr0_from_world: pycolmap.Rigid3d
) -> pycolmap.Rigid3d:
    """Re-origin a COLMAP-world pose at a QR marker, in the Auki anchor convention."""
    return convert_qr0_to_auki(convert_colmap_pose_to_qr_origin(world_from_x, qr0_from_world))


def convert_colmap_points_to_qr_origin(points_xyz: np.ndarray, qr0_from_world: pycolmap.Rigid3d) -> np.ndarray:
    """Vectorised, position-only convert_colmap_pose_to_qr_origin for a point cloud."""
    points_xyz = np.asarray(points_xyz, dtype=float)
    return (qr0_from_world.rotation.matrix() @ points_xyz.T).T + qr0_from_world.translation


def convert_colmap_points_to_qr_origin_auki(points_xyz: np.ndarray, qr0_from_world: pycolmap.Rigid3d) -> np.ndarray:
    """Vectorised, position-only convert_colmap_pose_to_qr_origin_auki for a point cloud."""
    return (_QR0_TO_AUKI_ROTATION @ convert_colmap_points_to_qr_origin(points_xyz, qr0_from_world).T).T
