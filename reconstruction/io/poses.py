"""Reads domain poses.json, the placed lighthouse list, as portal poses.

The file is the Domain Server response from GET /api/v1/domains/{id}/lighthouses,
published by DomainManagementTool as domain data named poses.json. Positions and
quaternions are OpenGL, the same convention as refined_manifest.json portals.
"""
import json
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np

from reconstruction.geometry.pose import (
    convert_pose_opengl_to_colmap,
    quaternion_to_rotation_matrix,
)


def parse_portals_from_poses(
    poses_path: Path,
) -> Dict[str, Tuple[np.ndarray, np.ndarray, Optional[float]]]:
    """short_id -> (R_world_portal, t_world_portal, reported_size) in OpenGL.

    Entries with no short id or no position and quaternion are skipped.
    reported_size is left in centimeters.
    """
    out = {}
    for short_id, translation, quaternion, size in _iter_pose_entries(poses_path):
        rotation = quaternion_to_rotation_matrix(quaternion)
        out[short_id] = (rotation, translation, size)
    return out


def parse_portals_from_poses_colmap(
    poses_path: Path,
) -> Dict[str, Tuple[np.ndarray, np.ndarray, Optional[float]]]:
    """short_id -> (R_world_portal, t_world_portal, size_m) in COLMAP.

    poses.json stores OpenGL positions and quaternions. reported_size is
    centimeters. The returned size is meters, matching portals.csv and the
    physicalSize written to refined_manifest.json.
    """
    out = {}
    for short_id, translation, quaternion, size in _iter_pose_entries(poses_path):
        colmap_t, colmap_q = convert_pose_opengl_to_colmap(translation, quaternion)
        rotation = quaternion_to_rotation_matrix(colmap_q)
        size_m = None if size is None else float(size) / 100.0
        out[short_id] = (rotation, np.asarray(colmap_t, dtype=np.float64), size_m)
    return out


def _iter_pose_entries(poses_path: Path):
    data = json.loads(Path(poses_path).read_text())
    if isinstance(data, list):
        poses = data
    elif isinstance(data, dict):
        poses = data.get("poses") or []
    else:
        poses = []

    for pose in poses:
        if not isinstance(pose, dict):
            continue
        short_id = pose.get("short_id") or pose.get("shortId")
        if short_id is None or str(short_id) == "":
            continue
        if not _has_pose(pose):
            continue
        translation = np.array(
            [float(pose["px"]), float(pose["py"]), float(pose["pz"])],
            dtype=np.float64,
        )
        quaternion = (
            float(pose["rx"]),
            float(pose["ry"]),
            float(pose["rz"]),
            float(pose["rw"]),
        )
        size = pose.get("reported_size", pose.get("reportedSize"))
        if size is not None:
            size = float(size)
        yield str(short_id), translation, quaternion, size


def _has_pose(pose: dict) -> bool:
    keys = ("px", "py", "pz", "rx", "ry", "rz", "rw")
    return all(key in pose and pose[key] is not None for key in keys)
