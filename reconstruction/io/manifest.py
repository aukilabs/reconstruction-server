"""Reads a previous refined_manifest.json."""
import json
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple
from reconstruction.geometry.pose import quaternion_to_rotation_matrix


def parse_info_from_manifest(manifest_path: Path) -> Tuple[Dict[str, Tuple[np.ndarray, np.ndarray]], List[str]]:
    """
    Returns dict: shortId -> (R_world_portal, t_world_portal)
    Assumes 'pose' is the transform from portal frame to world frame (world_T_portal).
    """
    data = json.loads(Path(manifest_path).read_text())
    portals = data.get('portals', [])
    out = {}
    for p in portals:
        sid = p.get('shortId') or p.get('shortID') or p.get('id')
        pose = p.get('pose') or p.get('averagePose')  # use pose; fallback to averagePose
        if sid is None or pose is None: 
            continue
        pos = pose.get('position', {})
        rot = pose.get('rotation', {})
        if not all(k in pos for k in ('x','y','z')): 
            continue
        if not all(k in rot for k in ('x','y','z','w')): 
            continue
        t = np.array([pos['x'], pos['y'], pos['z']], dtype=np.float64)
        R = quaternion_to_rotation_matrix((rot['x'], rot['y'], rot['z'], rot['w']))
        size = p.get('physicalSize', None)
        out[sid] = (R, t, size)

    data_id = data.get('dataIDs', [])

    data_id = [str(d) for d in data_id if "refined_scan" in str(d)]

    return out, data_id
