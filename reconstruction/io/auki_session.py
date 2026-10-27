"""Auki SDK capture sessions: registry, sensor logs and pose logs.

A capture folder (the SDK's app root) holds registries/, scan_report.json and one folder per
session with sensorlogs/<sensor>/ and poselogs/<from>__<to>/. load_auki_session reads one
session and extracts every camera frame to disk.

Which cameras sit on the shared rig is decided from the pose logs, not from the pose log
manifest's declared writer_mode: "movable" only means the SDK may log a new pose per frame,
and a head or arm camera that stayed parked for the whole capture is physically part of the
rig. analyze_sensor_motion measures each sensor's worst deviation from its own mean pose, and
load_auki_session folds the static ones in.
"""
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Tuple
import bisect
import logging

import numpy as np
import pycolmap

import auki_datatypes
import auki_layout
import auki_logs
import auki_registry

from reconstruction.geometry.frames import AUKI_ROBOT_WORLD
from reconstruction.geometry.pose import mean_pose


class PoseSample(NamedTuple):
    timestamp_ns: int
    pose: pycolmap.Rigid3d


class SensorInfo(NamedTuple):
    sensor_id: str
    frame_id: str
    width: int
    height: int
    distortion_model: str
    writer_mode: str  # "rigid" or "movable", exactly as the pose log manifest declares it


class SensorMotionStat(NamedTuple):
    """How much a sensor's base_link -> camera pose moves over the session: the worst
    deviation of any sample from the mean pose, which is the error that baking the mean in
    as a fixed sensor_from_rig would introduce. See analyze_sensor_motion."""
    sensor_id: str
    declared_mode: str        # what the pose log manifest says: "rigid" | "movable"
    effective_mode: str       # what this session's data says it should be treated as
    n_samples: int
    max_translation_m: float
    max_rotation_deg: float
    rms_translation_m: float
    is_static: bool


class ImageRecord(NamedTuple):
    sensor_id: str
    timestamp_ns: int
    relpath: str  # relative to images_dir
    dynamic_intrinsics: Optional[Tuple[float, float, float, float, List[float]]]


class AukiSessionData(NamedTuple):
    session_root: str
    images_dir: Path
    sensor_infos: Dict[str, SensorInfo]
    # The grouping this session's pose data supports, not always what
    # sensor_infos[...].writer_mode declares: a declared-movable sensor that never moved is
    # grouped with the rigid ones. sensor_motion records both.
    rigid_sensor_ids: List[str]
    movable_sensor_ids: List[str]
    ref_sensor_id: str
    images_per_sensor: Dict[str, List[ImageRecord]]
    base_from_camera_rigid: Dict[str, pycolmap.Rigid3d]
    base_from_camera_movable: Dict[str, List[PoseSample]]
    trajectory: List[PoseSample]  # world_from_base, in the session's own world frame
    world_frame: dict  # the session's registry-declared "world" frame
    sensor_motion: Dict[str, SensorMotionStat] = {}


def resolve_auki_session_id(app_root) -> str:
    """The one session in a capture folder: the subfolder that owns a sensorlogs/ directory.

    Some capture folders hold more than one session, and scan_report.json's
    clock.session_id names only one of them, so the folders are the source of truth. A
    capture with several sessions needs the session id passed explicitly.
    """
    app_root = Path(app_root)
    candidates = sorted(
        p.name for p in app_root.iterdir() if p.is_dir() and (p / "sensorlogs").is_dir()
    )
    if len(candidates) == 1:
        return candidates[0]
    raise ValueError(
        f"Could not auto-resolve a single Auki session under {app_root}: found "
        f"{len(candidates)} session folder(s) {candidates}. Pass session_id explicitly."
    )


def _rigid3d_from_spatial_transform(st) -> pycolmap.Rigid3d:
    quat_xyzw = np.array([st.orientation.x, st.orientation.y, st.orientation.z, st.orientation.w])
    trans = np.array([st.translation.x, st.translation.y, st.translation.z])
    return pycolmap.Rigid3d(pycolmap.Rotation3d(quat_xyzw), trans)


def _read_poselog(session_root, from_id: str, to_id: str) -> List[PoseSample]:
    path = auki_layout.poselog_path(session_root, from_id, to_id)
    entries = auki_logs.Log.read(path).entries()
    samples = []
    for e in entries:
        st = auki_datatypes.pose.SpatialTransform().parse(e.payload)
        samples.append(PoseSample(e.timestamp_ns, _rigid3d_from_spatial_transform(st)))
    samples.sort(key=lambda s: s.timestamp_ns)
    return samples


def _read_from_frame(app_root, session_root, from_id: str, to_id: str) -> dict:
    """The registry entry of a pose log's from-frame (e.g. "world" for world -> base_link):
    its declared axes, handedness and units, usable with auki_geometry directly."""
    manifest = auki_logs.Log.read(auki_layout.poselog_path(session_root, from_id, to_id)).manifest()
    from_frame = manifest["from_frame"]
    return auki_registry.read_frame(app_root, from_frame["peer_id"], from_frame["id"], from_frame["hash"])


def nearest_sample(samples: List[PoseSample], timestamp_ns: int, max_dt_ns: int):
    """Nearest-in-time sample. Returns (pose, dt_ns), or (None, dt_ns) if the nearest sample
    is more than max_dt_ns away."""
    if not samples:
        return None, None
    ts_list = [s.timestamp_ns for s in samples]
    idx = bisect.bisect_left(ts_list, timestamp_ns)
    candidates = [i for i in (idx - 1, idx) if 0 <= i < len(samples)]
    best = min(candidates, key=lambda i: abs(ts_list[i] - timestamp_ns))
    dt = abs(ts_list[best] - timestamp_ns)
    if dt > max_dt_ns:
        return None, dt
    return samples[best].pose, dt


def _rotation_angle_deg(rotation: pycolmap.Rotation3d) -> float:
    return float(np.degrees(2.0 * np.arcsin(np.clip(np.linalg.norm(rotation.quat[:3]), 0.0, 1.0))))


def analyze_sensor_motion(
    pose_samples_per_sensor: Dict[str, List[PoseSample]],
    declared_mode_per_sensor: Dict[str, str],
    static_translation_tol_m: float = 0.02,
    static_rotation_tol_deg: float = 1.0,
) -> Dict[str, SensorMotionStat]:
    """Classify every sensor's base_link -> camera pose log as static or moving, whatever its
    manifest declares.

    A declared-movable camera that did not move during the capture belongs on the shared
    rig: as a single-camera mini rig its poses are baked from the pose logs and never
    refined, so bundle adjustment can neither use its images to constrain the rig
    trajectory nor correct them.

    The test is the worst deviation of any sample from the mean pose. The default
    tolerances sit well below real articulation (centimetres, degrees) and well above pose
    log quantisation (sub-millimetre, hundredths of a degree on real captures).
    Declared-rigid sensors are analysed too, as diagnostics; they stay rigid.
    """
    stats: Dict[str, SensorMotionStat] = {}
    for sensor_id, samples in pose_samples_per_sensor.items():
        declared = declared_mode_per_sensor[sensor_id]
        if not samples:
            stats[sensor_id] = SensorMotionStat(
                sensor_id=sensor_id, declared_mode=declared, effective_mode=declared,
                n_samples=0, max_translation_m=float("nan"), max_rotation_deg=float("nan"),
                rms_translation_m=float("nan"), is_static=False,
            )
            continue

        mean = mean_pose([s.pose for s in samples])
        mean_inv = mean.inverse()
        trans_dev = np.array([np.linalg.norm(s.pose.translation - mean.translation) for s in samples])
        rot_dev = np.array([_rotation_angle_deg((mean_inv * s.pose).rotation) for s in samples])

        is_static = (trans_dev.max() <= static_translation_tol_m
                     and rot_dev.max() <= static_rotation_tol_deg)
        stats[sensor_id] = SensorMotionStat(
            sensor_id=sensor_id, declared_mode=declared,
            effective_mode="rigid" if (declared == "rigid" or is_static) else "movable",
            n_samples=len(samples), max_translation_m=float(trans_dev.max()),
            max_rotation_deg=float(rot_dev.max()),
            rms_translation_m=float(np.sqrt((trans_dev ** 2).mean())), is_static=bool(is_static),
        )
    return stats


def scan_sensor_motion(
    app_root: str,
    session_id: str,
    static_translation_tol_m: float = 0.02,
    static_rotation_tol_deg: float = 1.0,
) -> Dict[str, SensorMotionStat]:
    """analyze_sensor_motion from the base_link -> <sensor> pose logs alone, without
    extracting any frames (load_auki_session writes tens of GB on a long capture). For
    checking how a session would be grouped before reconstructing it."""
    session_root = auki_layout.session_root(app_root, session_id)
    sensorlogs_dir = Path(session_root) / "sensorlogs"
    pose_samples_per_sensor: Dict[str, List[PoseSample]] = {}
    declared_mode_per_sensor: Dict[str, str] = {}

    for sensor_id in sorted(p.name for p in sensorlogs_dir.iterdir() if p.is_dir()):
        manifest = auki_logs.Log.read(auki_layout.sensorlog_path(session_root, sensor_id)).manifest()
        frame_id = manifest["frame"]["id"]
        pose_path = auki_layout.poselog_path(session_root, "base_link", frame_id)
        pose_samples_per_sensor[sensor_id] = _read_poselog(session_root, "base_link", frame_id)
        declared_mode_per_sensor[sensor_id] = auki_logs.Log.read(pose_path).manifest()["writer_mode"]

    return analyze_sensor_motion(
        pose_samples_per_sensor, declared_mode_per_sensor,
        static_translation_tol_m=static_translation_tol_m,
        static_rotation_tol_deg=static_rotation_tol_deg,
    )


def load_auki_session(
    app_root: str,
    session_id: str,
    images_dir: Path,
    logger: Optional[logging.Logger] = None,
    regroup_static_movable: bool = True,
    static_translation_tol_m: float = 0.02,
    static_rotation_tol_deg: float = 1.0,
) -> AukiSessionData:
    """Read a session's registry and sensor and pose logs, write every camera frame to
    images_dir/<sensor_id>/<timestamp_ns>.jpg, and return the pose and sensor data
    build_auki_rig_and_frames needs.

    regroup_static_movable: fold declared-movable but static cameras into the rigid group
        (see analyze_sensor_motion). False takes every declared writer_mode at face value.
    static_translation_tol_m / static_rotation_tol_deg: how far a declared-movable sensor
        may deviate from its own mean pose and still count as static.
    """
    logger = logger or logging.getLogger(__name__)
    session_root = auki_layout.session_root(app_root, session_id)
    images_dir = Path(images_dir)

    sensorlogs_dir = Path(session_root) / "sensorlogs"
    sensor_ids = sorted(p.name for p in sensorlogs_dir.iterdir() if p.is_dir())

    sensor_infos: Dict[str, SensorInfo] = {}
    images_per_sensor: Dict[str, List[ImageRecord]] = {}
    pose_samples_per_sensor: Dict[str, List[PoseSample]] = {}
    declared_mode_per_sensor: Dict[str, str] = {}

    for sensor_id in sensor_ids:
        sensor_path = auki_layout.sensorlog_path(session_root, sensor_id)
        reader = auki_logs.Log.read(sensor_path)
        manifest = reader.manifest()
        sensor_ref = manifest["sensor"]
        frame_id = manifest["frame"]["id"]

        sensor_entry = auki_registry.read_sensor(
            app_root, sensor_ref["peer_id"], sensor_ref["id"], sensor_ref["hash"]
        )

        pose_samples = _read_poselog(session_root, "base_link", frame_id)
        pose_manifest = auki_logs.Log.read(
            auki_layout.poselog_path(session_root, "base_link", frame_id)
        ).manifest()
        writer_mode = pose_manifest["writer_mode"]

        sensor_infos[sensor_id] = SensorInfo(
            sensor_id=sensor_id,
            frame_id=frame_id,
            width=sensor_entry["width"],
            height=sensor_entry["height"],
            distortion_model=sensor_entry["distortion_model"],
            writer_mode=writer_mode,
        )

        if writer_mode not in ("rigid", "movable"):
            raise ValueError(f"Unexpected writer_mode {writer_mode!r} for sensor {sensor_id}")
        pose_samples_per_sensor[sensor_id] = pose_samples
        declared_mode_per_sensor[sensor_id] = writer_mode

        sensor_image_dir = images_dir / sensor_id
        sensor_image_dir.mkdir(parents=True, exist_ok=True)
        records = []
        seen_timestamps = set()
        duplicate_count = 0
        for entry in reader.entries():
            if entry.timestamp_ns in seen_timestamps:
                # Segment boundaries can repeat a timestamp, which would give two images the
                # same name (hloc's images.name is UNIQUE). Keep the first.
                duplicate_count += 1
                continue
            seen_timestamps.add(entry.timestamp_ns)

            cam_frame = auki_datatypes.camera.CameraFrame().parse(entry.payload)
            relpath = f"{sensor_id}/{entry.timestamp_ns}.jpg"
            (images_dir / relpath).write_bytes(cam_frame.frame)

            di = cam_frame.dynamic_intrinsics
            dynamic_intrinsics = None
            if di.fx != 0.0 or di.fy != 0.0:
                dynamic_intrinsics = (di.fx, di.fy, di.cx, di.cy, list(di.distortion_coefficients))

            records.append(ImageRecord(sensor_id, entry.timestamp_ns, relpath, dynamic_intrinsics))
        images_per_sensor[sensor_id] = records
        if duplicate_count:
            logger.warning(f"{sensor_id}: dropped {duplicate_count} duplicate-timestamp entry/entries")
        logger.info(f"{sensor_id}: {len(records)} frame(s) extracted, writer_mode={writer_mode}")

    trajectory = _read_poselog(session_root, "world", "base_link")
    logger.info(f"trajectory (world->base_link): {len(trajectory)} sample(s)")

    world_frame = _read_from_frame(app_root, session_root, "world", "base_link")
    logger.info(f"world frame convention (from registry): {world_frame['axes']}, "
                f"handedness={world_frame['handedness']}")
    if world_frame["axes"] != AUKI_ROBOT_WORLD["axes"] or world_frame["handedness"] != AUKI_ROBOT_WORLD["handedness"]:
        logger.warning(
            f"This session's registry-declared world frame convention ({world_frame['axes']}, "
            f"handedness={world_frame['handedness']}) differs from every session checked so far "
            f"({AUKI_ROBOT_WORLD['axes']}, handedness={AUKI_ROBOT_WORLD['handedness']}). "
            f"Converting from the declared one."
        )

    # Declared writer_mode is a capability, not an observation: measure, then group.
    sensor_motion = analyze_sensor_motion(
        pose_samples_per_sensor, declared_mode_per_sensor,
        static_translation_tol_m=static_translation_tol_m,
        static_rotation_tol_deg=static_rotation_tol_deg,
    )
    for sid in sensor_ids:
        st = sensor_motion[sid]
        logger.info(f"{sid}: declared={st.declared_mode}, max motion vs. mean pose "
                    f"{st.max_translation_m*1000:.2f}mm / {st.max_rotation_deg:.3f}deg "
                    f"over {st.n_samples} sample(s) -> static={st.is_static}")

    def _effective_mode(sid):
        if not regroup_static_movable:
            return declared_mode_per_sensor[sid]
        return sensor_motion[sid].effective_mode

    regrouped = sorted(sid for sid in sensor_ids
                       if declared_mode_per_sensor[sid] == "movable" and _effective_mode(sid) == "rigid")
    if regrouped:
        logger.info(f"Grouping {len(regrouped)} declared-movable but physically-static sensor(s) into the "
                    f"main rig: {regrouped} (within {static_translation_tol_m*1000:.0f}mm / "
                    f"{static_rotation_tol_deg:.2f}deg of a constant pose)")
    still_movable = sorted(sid for sid in sensor_ids if _effective_mode(sid) == "movable")
    if still_movable:
        logger.info(f"Keeping {len(still_movable)} genuinely-moving sensor(s) as per-sensor mini rigs: "
                    f"{still_movable}")

    rigid_sensor_ids, movable_sensor_ids = [], []
    base_from_camera_rigid: Dict[str, pycolmap.Rigid3d] = {}
    base_from_camera_movable: Dict[str, List[PoseSample]] = {}
    for sensor_id in sensor_ids:
        if _effective_mode(sensor_id) == "rigid":
            rigid_sensor_ids.append(sensor_id)
            base_from_camera_rigid[sensor_id] = mean_pose([s.pose for s in pose_samples_per_sensor[sensor_id]])
        else:
            movable_sensor_ids.append(sensor_id)
            base_from_camera_movable[sensor_id] = pose_samples_per_sensor[sensor_id]

    if not rigid_sensor_ids:
        raise ValueError("No rigid sensors found -- need at least one to anchor the main Rig")
    # Prefer a declared-rigid sensor as the rig's reference: its mounting is fixed by the
    # capture rig itself, while a regrouped sensor is only static as far as this session goes.
    ref_sensor_id = next(
        (sid for sid in rigid_sensor_ids if declared_mode_per_sensor[sid] == "rigid"),
        rigid_sensor_ids[0],
    )

    return AukiSessionData(
        session_root=session_root,
        images_dir=images_dir,
        sensor_infos=sensor_infos,
        rigid_sensor_ids=rigid_sensor_ids,
        movable_sensor_ids=movable_sensor_ids,
        ref_sensor_id=ref_sensor_id,
        images_per_sensor=images_per_sensor,
        base_from_camera_rigid=base_from_camera_rigid,
        base_from_camera_movable=base_from_camera_movable,
        trajectory=trajectory,
        world_frame=world_frame,
        sensor_motion=sensor_motion,
    )
