"""QR markers in an Auki session: detection (qr-lab), pose estimation (pnp-lab) and outputs."""
from typing import Dict, List, Optional
import csv

import cv2
import numpy as np
import pycolmap

import auki_pnplab
import qr_lab

from reconstruction.geometry.auki_conventions import (
    convert_colmap_points_to_qr_origin_auki,
    convert_colmap_pose_to_qr_origin_auki,
)
from reconstruction.geometry.frames import OPENCV_CAMERA, OPENGL, convert_transform_target_convention
from reconstruction.geometry.pose import mean_pose, rectify_portal_pose
from reconstruction.io.auki_session import AukiSessionData
from reconstruction.io.portals import save_portal_csv
from reconstruction.pipeline.auki_session.rig import ReconstructionBuild
from reconstruction.pipeline.local.qr import get_world_space_qr_codes

# The direction a flat marker's z points in COLMAP world: pnp-lab's marker z points out of
# the face, so up (+x) for a floor marker; the DMT default is down (see geometry/pose.py).
_QR_UP_AXIS = np.array([1.0, 0.0, 0.0])

# portals.csv is read by global and update refinement next to DMT scans, whose marker axes
# were relabelled together with the world on the way in (convert_pose_convention). pnp-lab's
# marker axes are not, so the two differ by that relabelling: a 180 degree turn about the
# in-plane axis at 45 degrees, which flips the marker's normal and swaps its in-plane axes.
# Right-multiplying by it changes only the marker's own axis labels, not where it is.
# portals.csv uses the DMT convention because every domain map and app-placed portal already
# does; the cost is that a floor marker's z in portals.csv points down. qr_anchor_poses.csv
# and point_cloud_qr_anchored.ply keep pnp-lab's convention; they serve AR clients.
_DOMAIN_PORTAL_FRAME_RELABEL = pycolmap.Rigid3d(
    pycolmap.Rotation3d(np.array([0.70710678, 0.70710678, 0.0, 0.0])), np.zeros(3))

# BGR, for corners 0-3 in the order qr-lab returns them: red, green, blue, yellow.
_CORNER_COLORS_BGR = [(0, 0, 255), (0, 200, 0), (255, 0, 0), (0, 220, 255)]


def _camera_model_dict(camera: pycolmap.Camera) -> dict:
    return {
        "fx": camera.params[0], "fy": camera.params[1],
        "cx": camera.params[2], "cy": camera.params[3],
        "dist": list(camera.params[4:]),
    }


def _corners_to_camera_space_pose(corners, qr_size_m: float, camera_model: dict) -> Optional[pycolmap.Rigid3d]:
    """pnp-lab's pose for one detection's corners, as camera_from_marker with the camera in
    COLMAP's camera frame. None if the solver does not converge.

    pnp-lab returns camera_from_marker with the camera in OpenGL axes (right, up, backward).
    Only the camera side (the target) is relabelled; the marker keeps pnp-lab's own axes
    (right, up, out of the face). Relabelling both sides would leave a marker facing the
    camera as the identity, but in COLMAP's camera frame its normal points along -z, so it
    must not be.
    """
    try:
        est = auki_pnplab.estimate_square_pose_from_pixels(
            list(map(list, corners)), qr_size_m, camera_model
        )
    except RuntimeError:
        return None

    p, r = est["pose"]["position"], est["pose"]["rotation"]
    position, quaternion = convert_transform_target_convention(
        [p["x"], p["y"], p["z"]], [r["x"], r["y"], r["z"], r["w"]], OPENGL, OPENCV_CAMERA
    )
    return pycolmap.Rigid3d(pycolmap.Rotation3d(quaternion), position)


def detect_qr_codes(data: AukiSessionData, build: ReconstructionBuild, qr_size_m: float, preset: str = "robust_fast"):
    """Run qr-lab on every extracted frame and estimate each detection's camera-space pose with
    the seeded intrinsics.

    Returns detections_per_qr, image_ids_per_qr, corners_per_qr (aligned per marker short id)
    and portal_sizes. No marker size is recorded anywhere in the SDK (registry, detection
    payload, resource catalog), so every marker gets qr_size_m.
    """
    rec = build.reconstruction
    detections_per_qr: Dict[str, List[pycolmap.Rigid3d]] = {}
    image_ids_per_qr: Dict[str, List[int]] = {}
    corners_per_qr: Dict[str, List[list]] = {}

    for sensor_id, records in data.images_per_sensor.items():
        camera_model = _camera_model_dict(rec.cameras[build.camera_id_per_sensor[sensor_id]])
        for record in records:
            image_id = build.image_id_per_record.get((sensor_id, record.timestamp_ns))
            if image_id is None:
                continue  # dropped for lack of a nearby pose sample
            gray = cv2.imread(str(data.images_dir / record.relpath), cv2.IMREAD_GRAYSCALE)
            if gray is None:
                continue
            result = qr_lab.scan(gray, preset=preset, refine=True)
            for code in result["codes"]:
                short_id = code["code"]["payload"].rsplit("/", 1)[-1]
                corners = list(map(list, code.get("refined_corners_source", code["corners_source"])))
                cam_space_pose = _corners_to_camera_space_pose(corners, qr_size_m, camera_model)
                if cam_space_pose is None:
                    continue

                detections_per_qr.setdefault(short_id, []).append(cam_space_pose)
                image_ids_per_qr.setdefault(short_id, []).append(image_id)
                corners_per_qr.setdefault(short_id, []).append(corners)

    portal_sizes = {short_id: qr_size_m for short_id in detections_per_qr}
    return detections_per_qr, image_ids_per_qr, corners_per_qr, portal_sizes


def reestimate_qr_camera_poses(
    reconstruction: pycolmap.Reconstruction,
    image_ids_per_qr: Dict[str, List[int]],
    corners_per_qr: Dict[str, List[list]],
    portal_sizes: Dict[str, float],
    ignore_distortion: bool = False,
):
    """Re-run pnp-lab for every detection with each image's refined intrinsics. Corner pixels
    do not depend on intrinsics, so the corners are reused rather than scanning again.

    ignore_distortion: drop each camera's distortion (keep fx, fy, cx, cy) before solving.
    pnp-lab only models Brown-Conrady distortion, so COLMAP's OPENCV_FISHEYE k1-k4 get
    misapplied as k1, k2, p1, p2; and self-calibrated fisheye coefficients are poorly
    constrained at the image periphery anyway. Dropping them raised convergence from about
    25% to 96% on a self-calibrated fisheye session. It fixes convergence, not accuracy at the
    periphery.

    Returns (detections_per_qr, image_ids_per_qr, corners_per_qr) aligned like
    detect_qr_codes's: a detection that does not converge is dropped from all three.
    """
    detections_per_qr: Dict[str, List[pycolmap.Rigid3d]] = {}
    kept_image_ids_per_qr: Dict[str, List[int]] = {}
    kept_corners_per_qr: Dict[str, List[list]] = {}

    for short_id, image_ids in image_ids_per_qr.items():
        qr_size_m = portal_sizes[short_id]
        for image_id, corners in zip(image_ids, corners_per_qr[short_id]):
            image = reconstruction.images[image_id]
            camera_model = _camera_model_dict(reconstruction.cameras[image.camera_id])
            if ignore_distortion:
                camera_model["dist"] = []
            cam_space_pose = _corners_to_camera_space_pose(corners, qr_size_m, camera_model)
            if cam_space_pose is None:
                continue
            detections_per_qr.setdefault(short_id, []).append(cam_space_pose)
            kept_image_ids_per_qr.setdefault(short_id, []).append(image_id)
            kept_corners_per_qr.setdefault(short_id, []).append(corners)

    return detections_per_qr, kept_image_ids_per_qr, kept_corners_per_qr


def export_qr_anchored_outputs(triangulated, qr_mean_poses, deviations, paths, logger, qr_origin_id=None):
    """Write the point cloud and every marker's pose re-origined at one marker, in the Auki
    anchor convention (forward, up, right): what an AR client anchored to that physical
    marker needs. portals.csv stays in COLMAP world.

    qr_origin_id: the marker to re-origin at. Defaults to the one with the lowest
    cross-detection deviation, the most consistent pose; a caller that knows which marker its
    AR client scans first should pass that one.
    """
    if not qr_mean_poses:
        logger.warning("No QR markers survived re-estimation -- skipping QR-anchored export")
        return

    if qr_origin_id is None:
        qr_origin_id = min(deviations, key=deviations.get)
        logger.info(f"Re-origining at QR {qr_origin_id} (lowest cross-detection deviation: "
                    f"{deviations[qr_origin_id]:.5f}m)")
    else:
        logger.info(f"Re-origining at QR {qr_origin_id} (explicitly requested)")

    qr0_from_world = qr_mean_poses[qr_origin_id].inverse()

    with open(paths.qr_anchor_poses, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for qr_id, pose in qr_mean_poses.items():
            anchored_pose = convert_colmap_pose_to_qr_origin_auki(pose, qr0_from_world)
            writer.writerow([qr_id, qr_id == qr_origin_id, *anchored_pose.translation, *anchored_pose.rotation.quat])
    logger.info(f"Wrote {paths.qr_anchor_poses}")

    points_xyz = np.array([p.xyz for p in triangulated.points3D.values()])
    points_color = np.array([p.color for p in triangulated.points3D.values()])
    anchored_rec = pycolmap.Reconstruction()
    if len(points_xyz):
        anchored_points = convert_colmap_points_to_qr_origin_auki(points_xyz, qr0_from_world)
        for xyz, color in zip(anchored_points, points_color):
            anchored_rec.add_point3D(xyz, pycolmap.Track(), color)
    anchored_rec.export_PLY(str(paths.qr_anchored_point_cloud))
    logger.info(f"Wrote {paths.qr_anchored_point_cloud}")


def export_qr_detections_csv(triangulated, qr_world_detections, image_ids_per_qr, paths, logger):
    """One row per detection (portals.csv aggregates them per marker), with the camera that saw
    it: short_id, image_id, sensor_id, camera_id, then the world pose in portals.csv's
    convention."""
    rows = []
    for short_id, poses in qr_world_detections.items():
        for image_id, pose in zip(image_ids_per_qr[short_id], poses):
            image = triangulated.images[image_id]
            sensor_id = image.name.split("/")[0]
            rows.append([short_id, image_id, sensor_id, image.camera_id, *pose.translation, *pose.rotation.quat])

    with open(paths.qr_detections, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["short_id", "image_id", "sensor_id", "camera_id",
                         "px", "py", "pz", "qx", "qy", "qz", "qw"])
        writer.writerows(rows)
    logger.info(f"Wrote {paths.qr_detections} ({len(rows)} detection(s))")


def export_qr_detection_crops(triangulated, image_ids_per_qr, corners_per_qr, paths, logger):
    """One crop per detection, <short_id>__<sensor_id>__<image_id>.jpg, with its four corners
    marked in qr-lab's order (red, green, blue, yellow) so the order PnP used stays visible."""
    n_written = 0
    for short_id, image_ids in image_ids_per_qr.items():
        for image_id, corners in zip(image_ids, corners_per_qr[short_id]):
            image = triangulated.images[image_id]
            sensor_id = image.name.split("/")[0]
            img = cv2.imread(str(paths.images / image.name))
            if img is None:
                logger.warning(f"Could not read {paths.images / image.name} -- skipping crop")
                continue

            corners_arr = np.array(corners, dtype=np.float64)
            x0, y0 = corners_arr.min(axis=0)
            x1, y1 = corners_arr.max(axis=0)
            w, h = x1 - x0, y1 - y0
            pad_x, pad_y = w * 0.6 + 20, h * 0.6 + 20
            x0c, y0c = max(0, int(x0 - pad_x)), max(0, int(y0 - pad_y))
            x1c, y1c = min(img.shape[1], int(x1 + pad_x)), min(img.shape[0], int(y1 + pad_y))

            radius = max(4, int(0.06 * max(w, h)))
            for (cx, cy), color in zip(corners_arr, _CORNER_COLORS_BGR):
                cv2.circle(img, (int(cx), int(cy)), radius, color, thickness=-1)
                cv2.circle(img, (int(cx), int(cy)), radius, (0, 0, 0), thickness=2)

            crop = img[y0c:y1c, x0c:x1c]
            cv2.imwrite(str(paths.qr_detection_crops / f"{short_id}__{sensor_id}__{image_id}.jpg"), crop)
            n_written += 1
    logger.info(f"Wrote {n_written} QR detection crop(s) to {paths.qr_detection_crops}")


def process_auki_qr(triangulated, image_ids_per_qr, corners_per_qr, portal_sizes, paths, logger,
                    qr_origin_id=None, qr_ignore_distortion=True):
    """The Auki counterpart of the DMT stage's QR step: re-estimate the marker poses with the
    refined intrinsics, then write portals.csv, qr_detections.csv, the detection crops and the
    QR-anchored outputs.

    Flat markers are rectified against _QR_UP_AXIS, and portals.csv and qr_detections.csv get
    the DMT marker convention (_DOMAIN_PORTAL_FRAME_RELABEL).

    qr_ignore_distortion: see reestimate_qr_camera_poses. It fixes the distortion-model
    mismatch, not every QR pose error: on auki_capture_slow, cameras that each agree with
    themselves still disagree with each other, most likely from oblique viewing angles at the
    periphery where self-calibration is weakest.
    """
    refined_detections_per_qr, refined_image_ids_per_qr, refined_corners_per_qr = reestimate_qr_camera_poses(
        triangulated, image_ids_per_qr, corners_per_qr, portal_sizes, ignore_distortion=qr_ignore_distortion
    )

    logger.info("Now save adjusted QR code poses")
    qr_world_detections = get_world_space_qr_codes(
        triangulated, refined_detections_per_qr, refined_image_ids_per_qr, logger=logger
    )
    qr_mean_poses = {
        qr_id: rectify_portal_pose(mean_pose(poses), reference_axis=_QR_UP_AXIS)
        for qr_id, poses in qr_world_detections.items() if poses
    }
    deviations = {}
    for qr_id, pose in qr_mean_poses.items():
        deviations[qr_id] = np.mean(np.std([det.translation for det in qr_world_detections[qr_id]], axis=0))
        logger.info(f'QR code id: {qr_id}, pose translation {pose.translation}, deviation: {deviations[qr_id]:.5f}')

    qr_world_detections = {
        qr_id: [rectify_portal_pose(p, reference_axis=_QR_UP_AXIS) for p in poses]
        for qr_id, poses in qr_world_detections.items()
    }
    portal_detections = {
        qr_id: [pose * _DOMAIN_PORTAL_FRAME_RELABEL for pose in poses]
        for qr_id, poses in qr_world_detections.items()
    }

    save_portal_csv(
        portal_detections, paths.sfm_dir / "portals.csv", refined_image_ids_per_qr, portal_sizes, refined_corners_per_qr
    )
    export_qr_detections_csv(triangulated, portal_detections, refined_image_ids_per_qr, paths, logger)
    export_qr_detection_crops(triangulated, refined_image_ids_per_qr, refined_corners_per_qr, paths, logger)
    export_qr_anchored_outputs(triangulated, qr_mean_poses, deviations, paths, logger, qr_origin_id=qr_origin_id)
