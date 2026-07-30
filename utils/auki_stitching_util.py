"""Stitch several independently-reconstructed session segments (each produced by
refine_auki_session, anchored at its own QR marker) back into one common frame, using
the QR marker(s) shared between consecutive segments as the alignment anchor -- no new
pose estimation, purely re-using each segment's own already-computed QR-anchored poses.

Chain-stitching only works where consecutive segments actually share a marker (verified
per-pair, not assumed): the reference segment (first in the chain) defines the global
frame, and every later segment's transform into that frame is
`T_prev_from_marker @ inverse(T_this_from_marker)` composed along the chain. Segments
where the chain breaks (a consecutive pair shares no marker) start a new, independent
group instead of being silently misaligned -- see chain_stitch_segments' `groups` return
value.
"""
from pathlib import Path
from typing import List, NamedTuple, Optional
import csv

import numpy as np
import open3d as o3d
import pycolmap


class SegmentAnchorData(NamedTuple):
    segment_id: str
    anchor_rows: list  # [{"short_id", "is_origin", "position", "quat"}, ...]
    point_cloud: o3d.geometry.PointCloud


class JointDiagnostics(NamedTuple):
    segment_a: str
    segment_b: str
    shared_markers: List[str]
    anchor_marker: str
    n_nearby_points: int
    frac_within_5cm: float
    frac_within_10cm: float
    median_error_m: float


def load_segment_qr_anchored(local_output_root, segment_id) -> SegmentAnchorData:
    seg_dir = Path(local_output_root) / segment_id
    anchor_rows = []
    with open(seg_dir / "sfm" / "qr_anchor_poses.csv", newline="") as f:
        for row in csv.reader(f):
            anchor_rows.append({
                "short_id": row[0], "is_origin": row[1] == "True",
                "position": np.array(row[2:5], dtype=float),
                "quat": np.array(row[5:9], dtype=float),
            })
    pcd = o3d.io.read_point_cloud(str(seg_dir / "point_cloud_qr_anchored.ply"))
    return SegmentAnchorData(segment_id, anchor_rows, pcd)


def rigid_4x4_from_position_quat(position, quat_xyzw) -> np.ndarray:
    T = np.eye(4)
    T[:3, :3] = pycolmap.Rotation3d(quat_xyzw).matrix()
    T[:3, 3] = position
    return T


def _marker_pose(anchor_rows, marker_id) -> np.ndarray:
    row = next(r for r in anchor_rows if r["short_id"] == marker_id)
    return rigid_4x4_from_position_quat(row["position"], row["quat"])


def _shared_markers(a: SegmentAnchorData, b: SegmentAnchorData) -> List[str]:
    ids_a = {r["short_id"] for r in a.anchor_rows}
    ids_b = {r["short_id"] for r in b.anchor_rows}
    return sorted(ids_a & ids_b)


def _stitch_transform(a: SegmentAnchorData, b: SegmentAnchorData, marker_id) -> np.ndarray:
    """4x4 transform mapping points in b's anchored frame into a's."""
    return _marker_pose(a.anchor_rows, marker_id) @ np.linalg.inv(_marker_pose(b.anchor_rows, marker_id))


def _joint_alignment(a: SegmentAnchorData, b: SegmentAnchorData, marker_id,
                      T_a_to_ref: np.ndarray, T_b_to_ref: np.ndarray, radius_m: float) -> JointDiagnostics:
    """Nearest-neighbor distance from segment A's points *near the shared marker* to
    segment B's (transformed) point cloud -- a direct read on whether the physical
    structure actually lines up, not just the marker itself (which lines up by
    construction). Both point clouds are given already in the common reference frame."""
    pcd_a = o3d.geometry.PointCloud(a.point_cloud)
    pcd_a.transform(T_a_to_ref)
    pcd_b = o3d.geometry.PointCloud(b.point_cloud)
    pcd_b.transform(T_b_to_ref)

    marker_pos = (T_a_to_ref @ _marker_pose(a.anchor_rows, marker_id))[:3, 3]
    pts_a = np.asarray(pcd_a.points)
    nearby = np.linalg.norm(pts_a - marker_pos, axis=1) < radius_m
    sub = o3d.geometry.PointCloud()
    sub.points = o3d.utility.Vector3dVector(pts_a[nearby])
    d = np.asarray(sub.compute_point_cloud_distance(pcd_b)) if nearby.sum() else np.array([np.nan])

    return JointDiagnostics(
        segment_a=a.segment_id, segment_b=b.segment_id, shared_markers=_shared_markers(a, b),
        anchor_marker=marker_id, n_nearby_points=int(nearby.sum()),
        frac_within_5cm=float((d < 0.05).mean()), frac_within_10cm=float((d < 0.10).mean()),
        median_error_m=float(np.median(d)),
    )


class StitchGroup(NamedTuple):
    segment_ids: List[str]
    transforms: dict  # segment_id -> 4x4, relative to segment_ids[0]
    joints: List[JointDiagnostics]


def chain_stitch_segments(local_output_root, segment_ids, logger, radius_m=1.5) -> List[StitchGroup]:
    """Chain-stitch every consecutive pair of segments in `segment_ids` (assumed already
    in time order) via their shared QR marker(s). Returns one or more StitchGroup --
    more than one only if a consecutive pair shares no marker at all, in which case that
    pair starts a fresh group rather than being forced into a misaligned chain.
    """
    data = {sid: load_segment_qr_anchored(local_output_root, sid) for sid in segment_ids}

    groups: List[StitchGroup] = []
    group_ids: List[str] = [segment_ids[0]]
    group_transforms = {segment_ids[0]: np.eye(4)}
    group_joints: List[JointDiagnostics] = []

    for i in range(len(segment_ids) - 1):
        a_id, b_id = segment_ids[i], segment_ids[i + 1]
        a, b = data[a_id], data[b_id]
        shared = _shared_markers(a, b)

        if not shared:
            logger.warning(f"{a_id} <-> {b_id}: no shared marker -- chain breaks here, "
                            f"starting a new stitch group at {b_id}")
            groups.append(StitchGroup(group_ids, group_transforms, group_joints))
            group_ids = [b_id]
            group_transforms = {b_id: np.eye(4)}
            group_joints = []
            continue

        marker = shared[0]
        T_b_to_a = _stitch_transform(a, b, marker)
        T_b_to_ref = group_transforms[a_id] @ T_b_to_a
        group_transforms[b_id] = T_b_to_ref
        group_ids.append(b_id)

        joint = _joint_alignment(a, b, marker, group_transforms[a_id], T_b_to_ref, radius_m)
        group_joints.append(joint)
        logger.info(f"{a_id} <-> {b_id} (via {marker}, shared={shared}): "
                    f"{joint.frac_within_5cm:.1%} within 5cm, {joint.frac_within_10cm:.1%} within 10cm, "
                    f"median {joint.median_error_m:.3f}m")

    groups.append(StitchGroup(group_ids, group_transforms, group_joints))
    if len(groups) > 1:
        logger.warning(f"Chain broke into {len(groups)} disjoint stitch group(s): "
                        f"{[g.segment_ids for g in groups]}")
    return groups


def write_global_outputs(local_output_root, group: StitchGroup, global_output_root, logger):
    """Merge one StitchGroup's segments into a single global point cloud + a single
    deduplicated portal (QR marker) table, all expressed in group.segment_ids[0]'s frame.
    Markers seen in more than one segment are averaged (position: mean; orientation:
    picked from whichever segment saw it with the lowest single-segment deviation isn't
    tracked post-hoc here, so this simply averages quaternions via the sign-corrected
    mean, which is fine for the small angular disagreements expected between adjacent
    segments' independent estimates of the same physical marker)."""
    global_output_root = Path(global_output_root)
    global_output_root.mkdir(parents=True, exist_ok=True)
    data = {sid: load_segment_qr_anchored(local_output_root, sid) for sid in group.segment_ids}

    all_points, all_colors = [], []
    marker_observations = {}  # short_id -> list of (position, quat) in global frame
    detection_rows = []

    for sid in group.segment_ids:
        seg = data[sid]
        T = group.transforms[sid]
        pts = np.asarray(seg.point_cloud.points)
        if len(pts):
            pts_global = (T @ np.c_[pts, np.ones(len(pts))].T).T[:, :3]
            all_points.append(pts_global)
            all_colors.append(np.asarray(seg.point_cloud.colors))
        for r in seg.anchor_rows:
            pose_global = T @ rigid_4x4_from_position_quat(r["position"], r["quat"])
            quat = pycolmap.Rotation3d(pose_global[:3, :3]).quat
            marker_observations.setdefault(r["short_id"], []).append((pose_global[:3, 3], quat, sid))

        detections_path = Path(local_output_root) / sid / "sfm" / "qr_detections.csv"
        if detections_path.exists():
            with open(detections_path, newline="") as f:
                reader = csv.reader(f)
                header = next(reader)
                for row in reader:
                    pos = np.array(row[4:7], dtype=float)
                    quat = np.array(row[7:11], dtype=float)
                    pose_global = T @ rigid_4x4_from_position_quat(pos, quat)
                    pos_g, quat_g = pose_global[:3, 3], pycolmap.Rotation3d(pose_global[:3, :3]).quat
                    detection_rows.append([sid, *row[:4], *pos_g, *quat_g])

    merged = o3d.geometry.PointCloud()
    if all_points:
        merged.points = o3d.utility.Vector3dVector(np.concatenate(all_points))
        merged.colors = o3d.utility.Vector3dVector(np.concatenate(all_colors))
    merged_ply_path = global_output_root / "point_cloud.ply"
    o3d.io.write_point_cloud(str(merged_ply_path), merged)
    logger.info(f"Wrote {merged_ply_path} ({len(merged.points)} points from {len(group.segment_ids)} segment(s))")

    portals_path = global_output_root / "qr_portals.csv"
    with open(portals_path, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["short_id", "n_segments_observed", "segment_ids", "px", "py", "pz", "qx", "qy", "qz", "qw"])
        for short_id, obs in sorted(marker_observations.items()):
            mean_pos = np.mean([o[0] for o in obs], axis=0)
            quats = np.array([o[1] for o in obs])
            quats[np.dot(quats, quats[0]) < 0] *= -1  # sign-align before averaging
            mean_quat = quats.mean(axis=0)
            mean_quat /= np.linalg.norm(mean_quat)
            writer.writerow([short_id, len(obs), ";".join(o[2] for o in obs), *mean_pos, *mean_quat])
    logger.info(f"Wrote {portals_path} ({len(marker_observations)} unique marker(s))")

    detections_path = global_output_root / "qr_detections.csv"
    with open(detections_path, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["segment_id", "short_id", "image_id", "sensor_id", "camera_id",
                          "px", "py", "pz", "qx", "qy", "qz", "qw"])
        writer.writerows(detection_rows)
    logger.info(f"Wrote {detections_path} ({len(detection_rows)} detection(s))")

    joints_path = global_output_root / "stitching_report.csv"
    with open(joints_path, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["segment_a", "segment_b", "anchor_marker", "shared_markers",
                          "n_nearby_points", "frac_within_5cm", "frac_within_10cm", "median_error_m"])
        for j in group.joints:
            writer.writerow([j.segment_a, j.segment_b, j.anchor_marker, ";".join(j.shared_markers),
                              j.n_nearby_points, j.frac_within_5cm, j.frac_within_10cm, j.median_error_m])
    logger.info(f"Wrote {joints_path} ({len(group.joints)} joint(s))")

    return {
        "point_cloud_path": merged_ply_path, "portals_path": portals_path,
        "detections_path": detections_path, "n_points": len(merged.points),
        "n_unique_markers": len(marker_observations),
    }
