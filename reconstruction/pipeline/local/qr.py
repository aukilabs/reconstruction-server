"""World-space QR portal poses in a local reconstruction."""
import logging
import numpy as np


def precompute_arkit_offsets(image_ids, arkit_cam_from_world_transforms, arkit_precomputed={}):
    for image_id in image_ids:
        arkit_cam_from_world = arkit_cam_from_world_transforms[image_id]

        # For first image just use same image instead of previous (to get a zero offset instead)
        prev_image_id = image_id - 1
        if prev_image_id not in image_ids:
            prev_image_id = image_id

        prev_arkit_cam_from_world = arkit_cam_from_world_transforms[prev_image_id]
        arkit_offset = prev_arkit_cam_from_world.inverse() * arkit_cam_from_world

        arkit_gravity_direction = np.matmul(arkit_cam_from_world.matrix(), np.array([-1.0, 0.0, 0.0, 0.0]).transpose())[:3]

        arkit_precomputed[image_id] = {
            "offset_moved": arkit_offset.translation,
            "offset_rotated": arkit_offset.rotation,
            "gravity_direction": arkit_gravity_direction,
            "cam_from_world": arkit_cam_from_world_transforms[image_id]
        }

    return arkit_precomputed


def get_world_space_qr_codes(reconstruction, detections_per_qr, image_ids_per_qr, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)

    qr_world_detections = {}

    logger.info("Getting world space qr codes...")
    for qr_id, cam_space_detections in detections_per_qr.items():
        qr_world_detections[qr_id] = []
        corresponding_image_ids = image_ids_per_qr[qr_id]

        for image_id, qr_pose_in_cam in zip(corresponding_image_ids, cam_space_detections):
            cam_pose = reconstruction.images[image_id].frame.rig_from_world.inverse()
            qr_world_pose = cam_pose * qr_pose_in_cam
            qr_world_detections[qr_id].append(qr_world_pose)

    logger.info("DONE!")

    return qr_world_detections
