"""Reads domain poses.json, the placed lighthouse list, as portal poses.

The file is the Domain Server response from GET /api/v1/domains/{id}/lighthouses,
published by DomainManagementTool as domain data named poses.json. Positions and
quaternions are OpenGL, the same convention as refined_manifest.json portals.
"""
import json
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np

from reconstruction.geometry.pose import quaternion_to_rotation_matrix


def parse_portals_from_poses(
    poses_path: Path,
) -> Dict[str, Tuple[np.ndarray, np.ndarray, Optional[float]]]:
    """short_id -> (R_world_portal, t_world_portal, reported_size) in OpenGL.

    Entries with no short id or no position and quaternion are skipped.
    """
    data = json.loads(Path(poses_path).read_text())
    if isinstance(data, list):
        poses = data
    elif isinstance(data, dict):
        poses = data.get("poses") or []
    else:
        poses = []

    out = {}
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
        rotation = quaternion_to_rotation_matrix(
            (float(pose["rx"]), float(pose["ry"]), float(pose["rz"]), float(pose["rw"]))
        )
        size = pose.get("reported_size", pose.get("reportedSize"))
        if size is not None:
            size = float(size)
        out[str(short_id)] = (rotation, translation, size)
    return out


def _has_pose(pose: dict) -> bool:
    keys = ("px", "py", "pz", "rx", "ry", "rz", "rw")
    return all(key in pose and pose[key] is not None for key in keys)
