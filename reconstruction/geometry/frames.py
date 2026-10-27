"""Coordinate frame conventions, declared once, and conversions between them.

Each frame is an Auki SDK frame registry entry: what its x, y and z axes point toward,
its handedness and its units. auki_geometry does the conversions, so a conversion
names the two frames instead of spelling out an axis swap.
docs/coordinate-conventions.md lists which frame every pipeline step and output uses.
"""
import auki_geometry
import numpy as np


def _frame(frame_id, x, y, z):
    return {
        "peer_id": "reconstruction",
        "frame_id": frame_id,
        "handedness": "right",
        "axes": {"x": x, "y": y, "z": z},
        "units": "meters",
    }


# OpenGL: x right, y up, z backward (the SDK's opengl preset). ARKit poses
# (ARposes.csv, PortalDetections.csv), Domain portal poses (portals.json) and
# refined_manifest.json are in this convention.
OPENGL = _frame("opengl", "right", "up", "backward")

# The world frame of every COLMAP model this pipeline writes: x up (gravity is -x),
# y right, z forward. OPENGL with x and y swapped and z negated, so ARKit poses
# convert into it directly.
COLMAP_WORLD = _frame("colmap_world", "up", "right", "forward")


def convert_pose_convention(position, quaternion, from_frame, to_frame):
    """Re-express a pose in another frame convention: both the frame it is expressed in
    and its own axes, so a pose in OPENGL comes out in COLMAP_WORLD on both sides.

    Quaternions are [x, y, z, w]. The result may be the negated quaternion, the same
    rotation.
    """
    flat = np.concatenate([np.asarray(position, dtype=float), np.asarray(quaternion, dtype=float)])
    converted = auki_geometry.convert_pose_convention(flat.tolist(), from_frame, to_frame)
    return np.array(converted[:3]), np.array(converted[3:])


def convention_matrix(from_frame, to_frame):
    """4x4 matrix that re-expresses points from one frame convention in another."""
    return np.array(auki_geometry.convention_matrix(from_frame, to_frame))
