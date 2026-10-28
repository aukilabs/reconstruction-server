"""The initial reconstruction of an Auki session: cameras, one multi-sensor rig and its frames."""
from typing import Dict, List, NamedTuple, Optional, Tuple
import bisect
import logging
import math

import pycolmap

from reconstruction.geometry.auki_conventions import convert_to_colmap_world
from reconstruction.io.auki_session import AukiSessionData, ImageRecord, SensorInfo, nearest_sample

_DISTORTION_MODEL_TO_CAMERA_MODEL = {
    "kannala_brandt": "OPENCV_FISHEYE",
    "plumb_bob": "OPENCV",
}


def seed_camera_model(
    info: SensorInfo, records: Optional[List[ImageRecord]] = None, assumed_hfov_deg: float = 69.0,
    external_intrinsics: Optional[dict] = None,
) -> dict:
    """Initial camera model for a sensor, from the first source that has one:
    1. external_intrinsics ({"fx", "fy", "cx", "cy", "dist"}) from the caller, e.g. recorded
       intrinsics from another capture of the same device, for a capture that has none.
    2. The intrinsics recorded in the frames (CameraFrame.dynamic_intrinsics; constant over a
       session for a fixed-focus sensor).
    3. A pinhole or fisheye guess from an assumed horizontal FOV (fx = fy, zero distortion);
       bundle adjustment then self-calibrates intrinsics and distortion.

    Recorded or external distortion: kannala_brandt maps to OPENCV_FISHEYE (k1..k4, same
    order); plumb_bob maps to OPENCV (k1, k2, p1, p2), dropping OpenCV's k3, which COLMAP's
    OPENCV model lacks (self-calibration absorbs the residual if enabled).

    The result's "source" ("external_recorded", "recorded" or "seeded_fov_guess") lets callers
    decide whether to hold the intrinsics fixed in bundle adjustment.
    """
    model = _DISTORTION_MODEL_TO_CAMERA_MODEL.get(info.distortion_model)
    if model is None:
        raise ValueError(f"Unhandled distortion_model {info.distortion_model!r} for {info.sensor_id}")

    if external_intrinsics is not None:
        fx, fy, cx, cy = (external_intrinsics["fx"], external_intrinsics["fy"],
                          external_intrinsics["cx"], external_intrinsics["cy"])
        dist = (list(external_intrinsics["dist"]) + [0.0, 0.0, 0.0, 0.0])[:4]
        return {"model": model, "params": [fx, fy, cx, cy, *dist], "source": "external_recorded"}

    real = next((r.dynamic_intrinsics for r in (records or []) if r.dynamic_intrinsics is not None), None)
    if real is not None:
        fx, fy, cx, cy, dist = real
        dist = (list(dist) + [0.0, 0.0, 0.0, 0.0])[:4]
        return {"model": model, "params": [fx, fy, cx, cy, *dist], "source": "recorded"}

    fx = fy = (info.width / 2) / math.tan(math.radians(assumed_hfov_deg / 2))
    cx, cy = info.width / 2, info.height / 2
    params = [fx, fy, cx, cy, 0.0, 0.0, 0.0, 0.0]
    return {"model": model, "params": params, "source": "seeded_fov_guess"}


def _nearest_index(sorted_ts: List[int], timestamp_ns: int, max_dt_ns: int):
    idx = bisect.bisect_left(sorted_ts, timestamp_ns)
    candidates = [i for i in (idx - 1, idx) if 0 <= i < len(sorted_ts)]
    if not candidates:
        return None, None
    best = min(candidates, key=lambda i: abs(sorted_ts[i] - timestamp_ns))
    dt = abs(sorted_ts[best] - timestamp_ns)
    if dt > max_dt_ns:
        return None, dt
    return best, dt


class ReconstructionBuild(NamedTuple):
    reconstruction: pycolmap.Reconstruction
    main_rig_id: int  # the multi-sensor rig; every other rig is one movable camera's
    camera_id_per_sensor: Dict[str, int]
    image_id_per_record: Dict[Tuple[str, int], int]  # (sensor_id, timestamp_ns) -> image_id
    timestamp_per_image: Dict[str, int]  # image name (relpath) -> timestamp_ns
    dropped_images: int
    intrinsics_source_per_sensor: Dict[str, str]  # sensor_id -> seed_camera_model's "source"


def build_auki_rig_and_frames(
    data: AukiSessionData,
    assumed_hfov_deg: float = 69.0,
    max_pose_dt_ns: int = 200_000_000,
    logger: Optional[logging.Logger] = None,
    external_intrinsics_per_sensor: Optional[Dict[str, dict]] = None,
) -> ReconstructionBuild:
    """The initial reconstruction (the Auki counterpart of the DMT stage's
    initialize_reconstruction):
    - one multi-sensor rig of the rigid cameras, with fixed sensor_from_rig extrinsics and one
      frame per trajectory sample; the frames' rig_from_world is all bundle adjustment refines;
    - one single-camera rig per movable camera, with one frame per image whose pose is baked
      from that timestamp's pose logs and held constant.

    external_intrinsics_per_sensor: passed to seed_camera_model per sensor. Seeding alone does
    not stop bundle adjustment from refining away from the seed; refine_auki_session holds
    every camera whose source is not the FOV guess fixed.
    """
    logger = logger or logging.getLogger(__name__)
    rec = pycolmap.Reconstruction()

    # Cameras
    camera_id_per_sensor: Dict[str, int] = {}
    intrinsics_source_per_sensor: Dict[str, str] = {}
    for camera_id, sensor_id in enumerate(sorted(data.sensor_infos.keys()), start=1):
        info = data.sensor_infos[sensor_id]
        model_dict = seed_camera_model(
            info, records=data.images_per_sensor.get(sensor_id), assumed_hfov_deg=assumed_hfov_deg,
            external_intrinsics=(external_intrinsics_per_sensor or {}).get(sensor_id),
        )
        camera = pycolmap.Camera(
            model=model_dict["model"],
            width=info.width,
            height=info.height,
            params=model_dict["params"],
            camera_id=camera_id,
        )
        rec.add_camera(camera)
        camera_id_per_sensor[sensor_id] = camera_id
        intrinsics_source_per_sensor[sensor_id] = model_dict["source"]
        logger.info(f"{sensor_id}: intrinsics source={model_dict['source']}, "
                    f"fx={model_dict['params'][0]:.2f} fy={model_dict['params'][1]:.2f}")

    # Main rig: the rigid sensors, with fixed sensor_from_rig
    ref_sensor_id = data.ref_sensor_id
    base_from_ref = data.base_from_camera_rigid[ref_sensor_id]

    main_rig_id = 1
    rig = pycolmap.Rig()
    rig.rig_id = main_rig_id
    ref_sensor_t = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=camera_id_per_sensor[ref_sensor_id])
    rig.add_ref_sensor(ref_sensor_t)

    sensor_t_per_rigid: Dict[str, pycolmap.sensor_t] = {ref_sensor_id: ref_sensor_t}
    for sensor_id in data.rigid_sensor_ids:
        if sensor_id == ref_sensor_id:
            continue
        sensor_t = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=camera_id_per_sensor[sensor_id])
        # The rig's frame is the reference camera's: sensor_from_rig = sensor_from_base * base_from_ref.
        sensor_from_rig = data.base_from_camera_rigid[sensor_id].inverse() * base_from_ref
        rig.add_sensor(sensor_t, sensor_from_rig)
        sensor_t_per_rigid[sensor_id] = sensor_t
    rec.add_rig(rig)

    # Trajectory frames: each rigid sensor's image goes to its nearest trajectory sample
    trajectory_ts = [s.timestamp_ns for s in data.trajectory]
    assignments: Dict[int, List[Tuple[str, ImageRecord]]] = {}
    dropped_images = 0
    for sensor_id in data.rigid_sensor_ids:
        for record in data.images_per_sensor[sensor_id]:
            idx, dt = _nearest_index(trajectory_ts, record.timestamp_ns, max_pose_dt_ns)
            if idx is None:
                dropped_images += 1
                continue
            assignments.setdefault(idx, []).append((sensor_id, record))

    image_id_per_record: Dict[Tuple[str, int], int] = {}
    timestamp_per_image: Dict[str, int] = {}
    next_image_id = 1
    next_frame_id = 1

    for idx in sorted(assignments.keys()):
        records = assignments[idx]
        world_from_ref = data.trajectory[idx].pose * base_from_ref
        rig_from_world = convert_to_colmap_world(world_from_ref, data.world_frame).inverse()

        frame = pycolmap.Frame()
        frame.frame_id = next_frame_id
        frame.rig_id = main_rig_id
        frame.rig_from_world = rig_from_world

        record_image_ids = []
        for sensor_id, record in records:
            image_id = next_image_id
            next_image_id += 1
            record_image_ids.append((sensor_id, record, image_id))
            frame.add_data_id(pycolmap.data_t(sensor_id=sensor_t_per_rigid[sensor_id], id=image_id))

        rec.add_frame(frame)
        for sensor_id, record, image_id in record_image_ids:
            img = pycolmap.Image(
                record.relpath, pycolmap.Point2DList([]), camera_id_per_sensor[sensor_id], image_id
            )
            img.frame_id = frame.frame_id
            rec.add_image(img)
            image_id_per_record[(sensor_id, record.timestamp_ns)] = image_id
            timestamp_per_image[record.relpath] = record.timestamp_ns
        rec.register_frame(frame.frame_id)
        next_frame_id += 1

    # Movable sensors: one rig per sensor (the database allows a sensor in one rig only), and
    # one frame per image with its own rig_from_world from that timestamp's pose logs, held
    # constant, like the DMT stage's single-camera rigs.
    next_rig_id = main_rig_id + 1
    for sensor_id in data.movable_sensor_ids:
        rig = pycolmap.Rig()
        rig.rig_id = next_rig_id
        sensor_t = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=camera_id_per_sensor[sensor_id])
        rig.add_ref_sensor(sensor_t)
        rec.add_rig(rig)
        movable_rig_id = next_rig_id
        next_rig_id += 1

        base_from_camera_samples = data.base_from_camera_movable[sensor_id]
        for record in data.images_per_sensor[sensor_id]:
            world_from_base, _ = nearest_sample(data.trajectory, record.timestamp_ns, max_pose_dt_ns)
            base_from_camera, _ = nearest_sample(base_from_camera_samples, record.timestamp_ns, max_pose_dt_ns)
            if world_from_base is None or base_from_camera is None:
                dropped_images += 1
                continue

            rig_from_world = convert_to_colmap_world(world_from_base * base_from_camera, data.world_frame).inverse()

            image_id = next_image_id
            next_image_id += 1

            frame = pycolmap.Frame()
            frame.frame_id = next_frame_id
            frame.rig_id = movable_rig_id
            frame.rig_from_world = rig_from_world
            frame.add_data_id(pycolmap.data_t(sensor_id=sensor_t, id=image_id))
            rec.add_frame(frame)

            img = pycolmap.Image(
                record.relpath, pycolmap.Point2DList([]), camera_id_per_sensor[sensor_id], image_id
            )
            img.frame_id = frame.frame_id
            rec.add_image(img)
            rec.register_frame(frame.frame_id)

            image_id_per_record[(sensor_id, record.timestamp_ns)] = image_id
            timestamp_per_image[record.relpath] = record.timestamp_ns
            next_frame_id += 1

    if dropped_images:
        logger.warning(f"Dropped {dropped_images} image(s) with no pose sample within {max_pose_dt_ns/1e6:.0f}ms")

    return ReconstructionBuild(
        reconstruction=rec,
        main_rig_id=main_rig_id,
        camera_id_per_sensor=camera_id_per_sensor,
        image_id_per_record=image_id_per_record,
        timestamp_per_image=timestamp_per_image,
        dropped_images=dropped_images,
        intrinsics_source_per_sensor=intrinsics_source_per_sensor,
    )
