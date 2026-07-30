"""Stitch session segments back together using the original, continuous world->base_link
trajectory each segment's rig frames were seeded from, instead of QR markers.

refine_auki_session saves two reconstructions per segment: `colmap_rec/` (the
registry-seeded poses, written *before* bundle adjustment -- literally a windowed slice
of the one continuous trajectory spanning the whole original session, so every segment's
colmap_rec is already in the same shared world frame by construction) and `sfm/` (the
refined poses *after* BA -- accurate relative to other cameras/points within that
segment, but BA has no absolute-pose prior tying it to that world frame, so the whole
segment's refined result is free to drift/rotate away from it as a rigid unit).

This module computes that per-segment drift (a single rigid transform, via Kabsch
alignment on common camera centers between colmap_rec and sfm) and undoes it -- putting
every segment's refined point cloud and QR marker poses directly into the original
session's world frame, with no dependency on any other segment or any QR marker overlap
between them. Unlike `auki_stitching_util`'s QR-marker chain-stitch, this can't suffer a
"chain break" (every segment aligns independently to the same trajectory) and doesn't
amplify a single noisy marker pose into a whole-segment misalignment -- so what
inconsistency *remains* between segments' independent observations of the same marker,
after this correction, isolates genuine per-detection QR/PnP error rather than
segment-to-segment registration error.
"""
from pathlib import Path
from typing import NamedTuple, List
import csv

import numpy as np
import open3d as o3d
import pycolmap


class TrajectoryAlignment(NamedTuple):
    segment_id: str
    transform: np.ndarray  # 4x4, maps sfm (refined) frame -> colmap_rec (world/trajectory) frame
    n_common_images: int
    rmse_before_m: float
    rmse_after_m: float
    rotation_correction_deg: float
    translation_correction_m: float


def kabsch_rigid_transform(P, Q):
    """Rotation R and translation t minimizing sum ||R@P_i + t - Q_i||^2 (no scaling --
    the rig's fixed sensor_from_rig extrinsics already fix metric scale, so a similarity
    transform would just be overfitting noise)."""
    centroid_P, centroid_Q = P.mean(axis=0), Q.mean(axis=0)
    Pc, Qc = P - centroid_P, Q - centroid_Q
    H = Pc.T @ Qc
    U, S, Vt = np.linalg.svd(H)
    d = np.sign(np.linalg.det(Vt.T @ U.T))
    R = Vt.T @ np.diag([1, 1, d]) @ U.T
    t = centroid_Q - R @ centroid_P
    return R, t


def compute_trajectory_alignment(local_output_root, segment_id) -> TrajectoryAlignment:
    seg_dir = Path(local_output_root) / segment_id
    world_rec = pycolmap.Reconstruction(str(seg_dir / "colmap_rec"))
    refined_rec = pycolmap.Reconstruction(str(seg_dir / "sfm"))

    common_ids = sorted(set(world_rec.images.keys()) & set(refined_rec.images.keys()))
    world_centers = np.array([world_rec.images[i].cam_from_world().inverse().translation for i in common_ids])
    refined_centers = np.array([refined_rec.images[i].cam_from_world().inverse().translation for i in common_ids])

    rmse_before = float(np.sqrt(((refined_centers - world_centers) ** 2).sum(axis=1)).mean())
    R, t = kabsch_rigid_transform(refined_centers, world_centers)
    aligned = (R @ refined_centers.T).T + t
    rmse_after = float(np.sqrt(((aligned - world_centers) ** 2).sum(axis=1)).mean())

    transform = np.eye(4)
    transform[:3, :3], transform[:3, 3] = R, t
    rotation_deg = float(np.degrees(np.arccos(np.clip((np.trace(R) - 1) / 2, -1, 1))))

    return TrajectoryAlignment(
        segment_id=segment_id, transform=transform, n_common_images=len(common_ids),
        rmse_before_m=rmse_before, rmse_after_m=rmse_after,
        rotation_correction_deg=rotation_deg, translation_correction_m=float(np.linalg.norm(t)),
    )


def load_portal_detections(local_output_root, segment_id):
    """Raw per-detection rows from sfm/portals.csv -- plain COLMAP-world poses, not
    QR-anchored/Auki-converted (see utils/data_utils.py's save_portal_csv /
    get_world_space_qr_codes). One row per (image_id, marker) detection, not per marker."""
    path = Path(local_output_root) / segment_id / "sfm" / "portals.csv"
    rows = []
    if not path.exists():
        return rows
    with open(path, newline="") as f:
        for row in csv.reader(f):
            rows.append({
                "image_id": int(row[0]), "short_id": row[1],
                "position": np.array(row[3:6], dtype=float), "quat": np.array(row[6:10], dtype=float),
            })
    return rows


def rigid_4x4_from_position_quat(position, quat_xyzw) -> np.ndarray:
    T = np.eye(4)
    T[:3, :3] = pycolmap.Rotation3d(quat_xyzw).matrix()
    T[:3, 3] = position
    return T


def transform_detections(detections, transform):
    out = []
    for d in detections:
        pose = transform @ rigid_4x4_from_position_quat(d["position"], d["quat"])
        out.append({**d, "position": pose[:3, 3], "quat": pycolmap.Rotation3d(pose[:3, :3]).quat})
    return out


def cluster_position_detections(detections, eps=0.15):
    """Groups detections of one marker (already in a common frame) whose positions
    agree within `eps` meters -- DBSCAN with min_samples=1 so every detection lands in
    some cluster (a real, single physical marker with only PnP noise should form one
    big cluster; multiple clusters -- especially ones that are internally tight but far
    from each other -- point at something more specific than noise: a duplicate
    physical marker sharing the same decoded id, or a tracking glitch during that
    contiguous block of frames)."""
    from sklearn.cluster import DBSCAN
    positions = np.array([d["position"] for d in detections])
    return DBSCAN(eps=eps, min_samples=1).fit_predict(positions)
