"""Auki SDK session loader -- the Auki-capture analogue of data_utils.py's ARKit loader.

Builds a multi-sensor pycolmap Reconstruction (one rigidly-mounted multi-camera Rig plus
independent single-camera "mini rigs" for movable/actuated cameras) from an Auki session
directory (registry + sensorlogs + poselogs), seeds per-camera intrinsics since none are
recorded, and runs qr-lab detection across every frame for portal registration.

Coordinate conventions
-----------------------
Auki's camera optical frames (registry axes: x=right, y=down, z=forward) already match
COLMAP's required camera-local convention -- no per-camera axis conversion is needed.

Auki's world frame (every session checked so far: base_link, world both x=forward,
y=left, z=up) does NOT generally match this codebase's established "colmap world"
convention (X=up, Y=right, Z=forward, reverse-engineered from
data_utils.convert_pose_opengl_to_colmap and its floor/gravity heuristics that treat
axis 0 as "up"). `load_auki_session` reads the world frame's *actual* declared
convention from the registry itself (`auki_registry.read_frame` on the world->base_link
poselog's own `from_frame` reference) rather than assuming it -- a session recorded by a
different robot/SDK build could declare different axes, and this way there's no silent
mismatch. `convert_to_colmap_world` then re-expresses just the world/reference side of a
pose into colmap-world convention, via `auki_geometry.convert_transform_target_convention`
(verified against a hand-derived rotation matrix on real session data -- bit-for-bit match
-- before switching to it; see convert_to_colmap_world's docstring for why this specific
auki_geometry function, not convert_pose_convention or convert_transform_source_convention).

Auki poselogs use "natural" parent-child semantics: `base_link -> camera` data satisfies
p_base_link = R * p_camera + t. This is exactly pycolmap's own `A_from_B` convention
(p_A = (A_from_B) * p_B, with chaining rule A_from_B * B_from_C = A_from_C), so poselog
data is loaded straight into pycolmap.Rigid3d and chained with plain `*` for composition
(world_from_base * base_from_camera, etc.) -- only the final world-axis relabeling goes
through auki_geometry, not the pose chaining itself.
"""
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Tuple
import bisect
import logging
import math

import numpy as np
import cv2
import pycolmap

import auki_registry
import auki_layout
import auki_logs
import auki_datatypes
import auki_geometry
import qr_lab
import auki_pnplab

from utils.data_utils import mean_pose

PEER_ID = "galbot"

# Every real session checked so far declares this world convention (base_link and world
# both x=forward, y=left, z=up, handedness=right) -- but load_auki_session now reads each
# session's actual declared convention from the registry rather than assuming this. Kept
# only as a reference value for a sanity-check log line (see load_auki_session) that flags
# it loudly if some future session's registry ever declares something different, since that
# would be a real, previously-unseen case worth a second look, not a silent axis mismatch.
_ROBOT_WORLD_FRAME_ENTRY = {
    "axes": {"x": "forward", "y": "left", "z": "up"},
    "handedness": "right", "units": "meters",
    "peer_id": PEER_ID, "frame_id": "world",
}
# This codebase's own "colmap world" convention (not an Auki concept at all -- a synthetic
# entry so auki_geometry has something to convert into).
_COLMAP_WORLD_FRAME_ENTRY = {
    "axes": {"x": "up", "y": "right", "z": "forward"},
    "handedness": "right", "units": "meters",
    "peer_id": "synthetic", "frame_id": "colmap_world",
}

_QR_FRAME_ENTRY = {
    "axes": {"x": "forward", "y": "left", "z": "up"},
    "handedness": "right", "units": "meters",
    "peer_id": "synthetic", "frame_id": "qr0",
}

# pnp-lab's own camera-local convention (OpenGL-style: x=right, y=up, z=backward) vs.
# COLMAP's required camera-local convention (OpenCV-style: x=right, y=down, z=forward).
# Both synthetic -- neither is an Auki registry concept, just a vocabulary auki_geometry
# needs to do the axis relabeling for detect_qr_codes.
_OPENGL_CAMERA_FRAME_ENTRY = {
    "axes": {"x": "right", "y": "up", "z": "backward"},
    "handedness": "right", "units": "meters",
    "peer_id": "synthetic", "frame_id": "opengl_camera",
}
_OPENCV_CAMERA_FRAME_ENTRY = {
    "axes": {"x": "right", "y": "down", "z": "forward"},
    "handedness": "right", "units": "meters",
    "peer_id": "synthetic", "frame_id": "opencv_camera",
}

# Same OpenGL axes as _OPENGL_CAMERA_FRAME_ENTRY (x=right, y=up, z=backward), used here
# as a *world*-frame convention (not camera-local) -- for relabeling the reconstruction's
# own global colmap-world axes into OpenGL convention (see convert_colmap_world_to_opengl).
# Also happens to be qr0's own native local-axis convention (see
# convert_colmap_pose_to_qr_origin_opengl's docstring), which is why re-origining at a QR
# marker needs no separate relabeling step at all.
_OPENGL_WORLD_FRAME_ENTRY = {
    "axes": {"x": "right", "y": "up", "z": "backward"},
    "handedness": "right", "units": "meters",
    "peer_id": "synthetic", "frame_id": "opengl_world",
}


_AUKI_WORLD_FRAME_ENTRY = {
    "axes": {"x": "forward", "y": "up", "z": "right"},
    "handedness": "right", "units": "meters",
    "peer_id": "synthetic", "frame_id": "opengl_world",
}


def _rigid3d_to_flat7(pose: pycolmap.Rigid3d) -> list:
    return [*pose.translation, *pose.rotation.quat]


def _flat7_to_rigid3d(flat) -> pycolmap.Rigid3d:
    return pycolmap.Rigid3d(pycolmap.Rotation3d(np.array(flat[3:])), np.array(flat[:3]))


def convert_to_colmap_world(world_from_x: pycolmap.Rigid3d, world_frame_entry: dict) -> pycolmap.Rigid3d:
    """Re-express a `world_from_X` pose (this session's own Auki world convention) into
    this codebase's colmap-world convention, leaving X's own local axes untouched.

    world_frame_entry: the "world" frame's *actual* registry-declared axes convention
    for this specific session (AukiSessionData.world_frame_entry, read dynamically by
    load_auki_session via auki_registry.read_frame -- not assumed/hardcoded, since a
    session from a different robot/SDK build could declare different axes).

    `world_from_x` (natural semantics, p_world = R*p_x + t) is exactly
    auki_geometry's compose-contract "x_to_world" (FROM=x, TO=world) -- so the world
    side is the *target* of that compose-sense transform, and
    `convert_transform_target_convention` is the one auki_geometry function that
    re-expresses only the target side, leaving the source (x's local axes) alone.
    (`convert_pose_convention` relabels both sides at once -- wrong here, since x's
    local axes must NOT change; `convert_transform_source_convention` relabels the
    wrong side entirely.)
    """
    flat = _rigid3d_to_flat7(world_from_x)
    converted = auki_geometry.convert_transform_target_convention(
        flat, world_frame_entry, _COLMAP_WORLD_FRAME_ENTRY
    )
    return _flat7_to_rigid3d(converted)


def convert_colmap_world_to_opengl(colmapworld_from_x: pycolmap.Rigid3d) -> pycolmap.Rigid3d:
    """The reverse of convert_to_colmap_world: re-express a `colmapworld_from_X` pose
    into OpenGL world axes (x=right, y=up, z=backward), leaving X's own local axes
    untouched. Same convert_transform_target_convention machinery, just the other
    direction -- for relabeling the reconstruction's own GLOBAL colmap-world axes (the
    scene's fixed reference frame, origin unchanged) into OpenGL convention. NOT the
    right tool for re-origining at a QR marker's own (arbitrarily-oriented) local frame
    -- see convert_colmap_pose_to_qr_origin_opengl for that case, which needs no
    separate relabeling step at all."""
    flat = _rigid3d_to_flat7(colmapworld_from_x)
    converted = auki_geometry.convert_transform_target_convention(
        flat, _COLMAP_WORLD_FRAME_ENTRY, _OPENGL_WORLD_FRAME_ENTRY
    )
    return _flat7_to_rigid3d(converted)


def convert_qr0_to_auki(qr0_from_x: pycolmap.Rigid3d) -> pycolmap.Rigid3d:
    """Re-express a pose already expressed relative to a QR marker's own local frame
    (qr0_from_x -- e.g. the output of convert_colmap_pose_to_qr_origin, NOT a raw
    colmap-world pose) into Auki's own world convention (x=forward, y=up, z=right),
    leaving X's own local axes untouched.

    CAUTION -- please confirm intent before relying on this: `_QR_FRAME_ENTRY` (the
    declared "from" convention, i.e. qr0's own axes) is currently the exact same dict
    as `_ROBOT_WORLD_FRAME_ENTRY` (x=forward, y=left, z=up), which describes the
    *robot's body frame* from the registry -- unrelated to a QR marker's geometry.
    qr0's actual native axes (right, up, plane-normal, established once by pnp-lab's
    square-pose fit and left untouched through the whole detection pipeline -- see
    detect_qr_codes' own comment) match `_OPENGL_CAMERA_FRAME_ENTRY` (x=right, y=up,
    z=backward), not this. If `_QR_FRAME_ENTRY` was meant to intentionally map the
    marker's own plane-normal to Auki's "up" (a real, valid AR-anchor convention: AR
    content always renders "upright" relative to whichever face the marker is stuck
    to, floor/wall/ceiling alike) it should be declared to do that explicitly, rather
    than happening to coincide with the robot's unrelated body-frame convention -- as
    currently declared, verified on real session data that both interpretations
    produce IDENTICAL results for a floor-mounted marker (this session's only kind)
    but genuinely different, untested results for a wall- or ceiling-mounted one.
    """
    flat = _rigid3d_to_flat7(qr0_from_x)
    converted = auki_geometry.convert_transform_target_convention(
        flat, _QR_FRAME_ENTRY, _AUKI_WORLD_FRAME_ENTRY
    )
    return _flat7_to_rigid3d(converted)


# Fixed rotation matrices (verified numerically: convert_transform_target_convention's
# target-side relabeling applies the same matrix to a pose's translation and rotation
# regardless of the pose's own translation) -- derived once from the identity pose
# rather than hardcoded, so each stays correct if its frame-entry dicts ever change.
_QR0_TO_AUKI_ROTATION = np.array(
    convert_qr0_to_auki(pycolmap.Rigid3d(pycolmap.Rotation3d(), np.zeros(3))).rotation.matrix()
)


def convert_colmap_pose_to_qr_origin(
    world_from_x: pycolmap.Rigid3d, qr0_from_world: pycolmap.Rigid3d
) -> pycolmap.Rigid3d:
    """Re-origin a colmap-world pose at a QR marker's own frame (qr0_from_world =
    that marker's mean world pose, inverted). Pure re-origining -- no axis relabeling
    is applied or needed; see convert_colmap_pose_to_qr_origin_opengl's docstring for
    why composing with qr0_from_world already produces OpenGL-convention axes for free."""
    return qr0_from_world * world_from_x


def convert_colmap_pose_to_qr_origin_opengl(
    world_from_x: pycolmap.Rigid3d, qr0_from_world: pycolmap.Rigid3d
) -> pycolmap.Rigid3d:
    """Re-origin a colmap-world pose at a QR marker's own frame, in OpenGL convention.

    Identical to convert_colmap_pose_to_qr_origin -- no separate axis-relabeling step
    is needed. Composing with qr0_from_world doesn't just move the origin -- its
    rotation is, by construction, the change of basis from colmap-world axes into
    qr0's OWN local axes (right, up, plane-normal), established once by pnp-lab's
    square-pose fit (`pose_from_points` in pnp-lab's square_pose.rs) and left untouched
    through the entire detection pipeline (detect_qr_codes' own comment: "leaving the
    marker's own local axes ... untouched"). Those local axes already numerically
    coincide with OpenGL's own convention (x=right, y=up, z=backward): both are
    defined as local-Z = cross(local-X, local-Y) in a right-handed system with
    local-X=right, local-Y=up -- pnp-lab just calls its own third axis "forward"
    rather than "backward", a naming choice, not a geometric difference. So composing
    with qr0_from_world is *simultaneously* the re-origining and the only axis
    relabeling this needs (verified on real data: re-origining a marker's own pose at
    itself returns an exact identity Rigid3d only when no extra relabeling is applied
    on top -- an earlier version that added one here returned a spurious 90-degree
    rotation instead).
    """
    return convert_colmap_pose_to_qr_origin(world_from_x, qr0_from_world)


def convert_colmap_pose_to_qr_origin_auki(
    world_from_x: pycolmap.Rigid3d, qr0_from_world: pycolmap.Rigid3d
) -> pycolmap.Rigid3d:
    """Re-origin a colmap-world pose at a QR marker's own frame, in Auki convention.

    Unlike the OpenGL variant, this one's relabeling step is NOT a no-op -- Auki's
    declared world convention (forward, up, right) genuinely differs from qr0's own
    native axes (right, up, backward-ish), so convert_qr0_to_auki does real work here.
    See that function's docstring for an open question about whether `_QR_FRAME_ENTRY`
    is declared correctly."""
    return convert_qr0_to_auki(convert_colmap_pose_to_qr_origin(world_from_x, qr0_from_world))


def convert_colmap_points_to_qr_origin(
    points_xyz: np.ndarray, qr0_from_world: pycolmap.Rigid3d
) -> np.ndarray:
    """Vectorized position-only counterpart of convert_colmap_pose_to_qr_origin, for a
    full point cloud/trajectory (no per-point orientation to carry)."""
    points_xyz = np.asarray(points_xyz, dtype=float)
    return (qr0_from_world.rotation.matrix() @ points_xyz.T).T + qr0_from_world.translation


def convert_colmap_points_to_qr_origin_opengl(
    points_xyz: np.ndarray, qr0_from_world: pycolmap.Rigid3d
) -> np.ndarray:
    """Vectorized position-only counterpart of convert_colmap_pose_to_qr_origin_opengl
    -- see that function's docstring for why no separate axis relabeling is needed."""
    return convert_colmap_points_to_qr_origin(points_xyz, qr0_from_world)


def convert_colmap_points_to_qr_origin_auki(
    points_xyz: np.ndarray, qr0_from_world: pycolmap.Rigid3d
) -> np.ndarray:
    """Vectorized position-only counterpart of convert_colmap_pose_to_qr_origin_auki."""
    return (_QR0_TO_AUKI_ROTATION @ convert_colmap_points_to_qr_origin(points_xyz, qr0_from_world).T).T

_DISTORTION_MODEL_TO_CAMERA_MODEL = {
    "kannala_brandt": "OPENCV_FISHEYE",
    "plumb_bob": "OPENCV",
}


def _rigid3d_from_spatial_transform(st) -> pycolmap.Rigid3d:
    quat_xyzw = np.array([st.orientation.x, st.orientation.y, st.orientation.z, st.orientation.w])
    trans = np.array([st.translation.x, st.translation.y, st.translation.z])
    return pycolmap.Rigid3d(pycolmap.Rotation3d(quat_xyzw), trans)


class PoseSample(NamedTuple):
    timestamp_ns: int
    pose: pycolmap.Rigid3d


def _read_poselog(session_root, from_id: str, to_id: str) -> List[PoseSample]:
    path = auki_layout.poselog_path(session_root, from_id, to_id)
    entries = auki_logs.Log.read(path).entries()
    samples = []
    for e in entries:
        st = auki_datatypes.pose.SpatialTransform().parse(e.payload)
        samples.append(PoseSample(e.timestamp_ns, _rigid3d_from_spatial_transform(st)))
    samples.sort(key=lambda s: s.timestamp_ns)
    return samples


def _read_from_frame_registry_entry(app_root, session_root, from_id: str, to_id: str) -> dict:
    """Read the *actual* registry-declared axes/handedness convention of a poselog's
    `from_id` frame (e.g. "world" for the world->base_link poselog) -- a
    FrameRegistryEntry-shaped dict (axes/handedness/units/peer_id/frame_id) usable
    directly with auki_geometry's convert_transform_*_convention functions. Same
    manifest -> registry lookup pattern load_auki_session already uses for each
    sensor's own optical frame (manifest["frame"] -> auki_registry.read_sensor), just
    for a poselog's `from_frame` reference instead."""
    manifest = auki_logs.Log.read(auki_layout.poselog_path(session_root, from_id, to_id)).manifest()
    from_frame = manifest["from_frame"]
    return auki_registry.read_frame(app_root, from_frame["peer_id"], from_frame["id"], from_frame["hash"])


def _nearest_sample(samples: List[PoseSample], timestamp_ns: int, max_dt_ns: int):
    """Nearest-in-time sample lookup. Returns (pose, dt_ns) or (None, dt_ns) if the
    nearest sample is farther than max_dt_ns away."""
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


class SensorInfo(NamedTuple):
    sensor_id: str
    frame_id: str
    width: int
    height: int
    distortion_model: str
    writer_mode: str  # "rigid" or "movable"


class ImageRecord(NamedTuple):
    sensor_id: str
    timestamp_ns: int
    relpath: str  # relative to images_dir
    dynamic_intrinsics: Optional[Tuple[float, float, float, float, List[float]]]


class AukiSessionData(NamedTuple):
    session_root: str
    images_dir: Path
    sensor_infos: Dict[str, SensorInfo]
    rigid_sensor_ids: List[str]
    movable_sensor_ids: List[str]
    ref_sensor_id: str
    images_per_sensor: Dict[str, List[ImageRecord]]
    base_from_camera_rigid: Dict[str, pycolmap.Rigid3d]
    base_from_camera_movable: Dict[str, List[PoseSample]]
    trajectory: List[PoseSample]  # world_from_base, raw Auki convention
    world_frame_entry: dict  # this session's own registry-declared "world" axes convention


def load_auki_session(
    app_root: str,
    session_id: str,
    images_dir: Path,
    peer_id: str = PEER_ID,
    logger: Optional[logging.Logger] = None,
) -> AukiSessionData:
    """Read an Auki session's registry + sensor/pose logs, extract every camera frame to
    disk under `images_dir/<sensor_id>/<timestamp_ns>.jpg`, and return the raw pose/sensor
    data needed to build a pycolmap Reconstruction (see build_auki_rig_and_frames).
    """
    logger = logger or logging.getLogger("auki_reconstruction")
    session_root = auki_layout.session_root(app_root, session_id)
    images_dir = Path(images_dir)

    sensorlogs_dir = Path(session_root) / "sensorlogs"
    sensor_ids = sorted(p.name for p in sensorlogs_dir.iterdir() if p.is_dir())

    sensor_infos: Dict[str, SensorInfo] = {}
    images_per_sensor: Dict[str, List[ImageRecord]] = {}
    base_from_camera_rigid: Dict[str, pycolmap.Rigid3d] = {}
    base_from_camera_movable: Dict[str, List[PoseSample]] = {}

    for sensor_id in sensor_ids:
        sensor_path = auki_layout.sensorlog_path(session_root, sensor_id)
        reader = auki_logs.Log.read(sensor_path)
        manifest = reader.manifest()
        sensor_ref = manifest["sensor"]
        frame_ref = manifest["frame"]
        frame_id = frame_ref["id"]

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

        if writer_mode == "rigid":
            base_from_camera_rigid[sensor_id] = mean_pose([s.pose for s in pose_samples])
        elif writer_mode == "movable":
            base_from_camera_movable[sensor_id] = pose_samples
        else:
            raise ValueError(f"Unexpected writer_mode {writer_mode!r} for sensor {sensor_id}")

        sensor_image_dir = images_dir / sensor_id
        sensor_image_dir.mkdir(parents=True, exist_ok=True)
        records = []
        entries = reader.entries()
        seen_timestamps = set()
        duplicate_count = 0
        for entry in entries:
            if entry.timestamp_ns in seen_timestamps:
                # Segment-boundary duplicates happen (confirmed on real data: a couple of
                # sensors log the exact same timestamp twice). Same timestamp -> same
                # relpath -> would violate hloc's images.name UNIQUE constraint. Keep the
                # first occurrence, drop the rest.
                duplicate_count += 1
                continue
            seen_timestamps.add(entry.timestamp_ns)

            cam_frame = auki_datatypes.camera.CameraFrame().parse(entry.payload)
            relpath = f"{sensor_id}/{entry.timestamp_ns}.jpg"
            out_path = images_dir / relpath
            out_path.write_bytes(cam_frame.frame)

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

    world_frame_entry = _read_from_frame_registry_entry(app_root, session_root, "world", "base_link")
    logger.info(f"world frame convention (from registry): {world_frame_entry['axes']}, "
                f"handedness={world_frame_entry['handedness']}")
    if world_frame_entry["axes"] != _ROBOT_WORLD_FRAME_ENTRY["axes"] or \
            world_frame_entry["handedness"] != _ROBOT_WORLD_FRAME_ENTRY["handedness"]:
        logger.warning(
            f"This session's registry-declared world frame convention ({world_frame_entry['axes']}, "
            f"handedness={world_frame_entry['handedness']}) differs from every session checked so far "
            f"({_ROBOT_WORLD_FRAME_ENTRY['axes']}, handedness={_ROBOT_WORLD_FRAME_ENTRY['handedness']}) "
            f"-- using it as-is, but this is a genuinely new case worth a second look."
        )

    rigid_sensor_ids = sorted(sid for sid, info in sensor_infos.items() if info.writer_mode == "rigid")
    movable_sensor_ids = sorted(sid for sid, info in sensor_infos.items() if info.writer_mode == "movable")
    if not rigid_sensor_ids:
        raise ValueError("No rigid sensors found -- need at least one to anchor the main Rig")
    ref_sensor_id = rigid_sensor_ids[0]

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
        world_frame_entry=world_frame_entry,
    )


def seed_camera_model(
    info: SensorInfo, records: Optional[List["ImageRecord"]] = None, assumed_hfov_deg: float = 69.0,
    external_intrinsics: Optional[dict] = None,
) -> dict:
    """Camera model for a sensor. Priority order:
    1. `external_intrinsics` ({"fx","fy","cx","cy","dist"}), when given by the caller --
       e.g. factory/recorded intrinsics sourced from a *different* capture of the same
       physical device, for sessions where this capture's own metadata has none (see 2).
    2. Real recorded intrinsics (`CameraFrame.dynamic_intrinsics`, confirmed constant
       across an entire session for a given fixed-focus sensor) when at least one
       frame's record carries them.
    3. Falls back to a documented assumed-HFOV pinhole/fisheye guess otherwise (captures
       with no calibration recorded at all -- fx=fy from the assumed FOV, distortion
       starts at zero and both intrinsics and distortion are self-calibrated by bundle
       adjustment).

    Distortion, when recorded/external intrinsics are used:
    - kannala_brandt -> OPENCV_FISHEYE (4 params k1..k4): recorded coefficients map
      straight across, same order.
    - plumb_bob -> OPENCV (4 params k1,k2,p1,p2): recorded coefficients are 5 values in
      OpenCV's own distCoeffs order (k1,k2,p1,p2,k3) -- k3 is dropped since COLMAP's
      OPENCV model has no third radial term; bundle adjustment's self-calibration
      (refine_extra_params) absorbs the small residual, if enabled.
    Returns an extra "source" key ("external_recorded", "recorded", or
    "seeded_fov_guess") so callers can decide e.g. whether to trust focal
    length/principal point as fixed in BA.
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
    camera_id_per_sensor: Dict[str, int]
    image_id_per_record: Dict[Tuple[str, int], int]  # (sensor_id, timestamp_ns) -> image_id
    timestamp_per_image: Dict[str, int]  # image filename (relpath) -> timestamp_ns
    dropped_images: int
    intrinsics_source_per_sensor: Dict[str, str]  # sensor_id -> "recorded" | "seeded_fov_guess"


def build_auki_rig_and_frames(
    data: AukiSessionData,
    assumed_hfov_deg: float = 69.0,
    max_pose_dt_ns: int = 200_000_000,
    logger: Optional[logging.Logger] = None,
    external_intrinsics_per_sensor: Optional[Dict[str, dict]] = None,
) -> ReconstructionBuild:
    """The Auki analogue of refinement_util.initialize_reconstruction: builds a
    pycolmap.Reconstruction with one multi-sensor Rig (rigid cameras, fixed
    sensor_from_rig extrinsics, one rig_from_world per trajectory sample -- the only
    thing bundle adjustment should refine) plus one independent single-camera "mini rig"
    per movable-camera image (pose baked directly from that timestamp's pose logs, held
    constant, matching the existing ARKit single-camera-per-rig pattern).

    external_intrinsics_per_sensor: forwarded to seed_camera_model per sensor -- see its
    own docstring. Use together with refine_auki_session's refine_intrinsics=False to
    actually trust these values (seeding alone doesn't stop BA from self-calibrating
    away from them).
    """
    logger = logger or logging.getLogger("auki_reconstruction")
    rec = pycolmap.Reconstruction()

    # --- Cameras -----------------------------------------------------------------
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

    # --- Main rig: rigid sensors, fixed sensor_from_rig ---------------------------
    ref_sensor_id = data.ref_sensor_id
    base_from_ref = data.base_from_camera_rigid[ref_sensor_id]

    main_rig_id = 1
    rig = pycolmap.Rig()
    rig.rig_id = main_rig_id
    ref_sensor_t = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=camera_id_per_sensor[ref_sensor_id])
    rig.add_ref_sensor(ref_sensor_t)

    sensor_t_per_rigid: Dict[str, "pycolmap.sensor_t"] = {ref_sensor_id: ref_sensor_t}
    for sensor_id in data.rigid_sensor_ids:
        if sensor_id == ref_sensor_id:
            continue
        sensor_t = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=camera_id_per_sensor[sensor_id])
        # sensor_from_rig: p_sensor = (sensor_from_rig) * p_rig, rig == ref camera.
        # other_camera_from_ref_camera = other_camera_from_base * base_from_ref_camera
        sensor_from_rig = data.base_from_camera_rigid[sensor_id].inverse() * base_from_ref
        rig.add_sensor(sensor_t, sensor_from_rig)
        sensor_t_per_rigid[sensor_id] = sensor_t
    rec.add_rig(rig)

    # --- Trajectory frames: assign each rigid-sensor image to its nearest trajectory sample
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
        world_from_base_raw = data.trajectory[idx].pose
        world_from_ref = world_from_base_raw * base_from_ref
        colmapworld_from_ref = convert_to_colmap_world(world_from_ref, data.world_frame_entry)
        rig_from_world = colmapworld_from_ref.inverse()

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

    # --- Movable sensors: one rig per SENSOR (a sensor may belong to only one rig --
    # the database schema enforces this), but one independent Frame per IMAGE, each
    # with its own rig_from_world baked from that timestamp's pose logs and held
    # constant (never refined) -- matching the existing single-camera-rig pattern,
    # just with many frames sharing one trivial ref-sensor-only rig per camera.
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
            world_from_base_raw, dt_world = _nearest_sample(data.trajectory, record.timestamp_ns, max_pose_dt_ns)
            base_from_camera, dt_base = _nearest_sample(base_from_camera_samples, record.timestamp_ns, max_pose_dt_ns)
            if world_from_base_raw is None or base_from_camera is None:
                dropped_images += 1
                continue

            world_from_cam = world_from_base_raw * base_from_camera
            colmapworld_from_cam = convert_to_colmap_world(world_from_cam, data.world_frame_entry)
            rig_from_world = colmapworld_from_cam.inverse()

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
            next_frame_id += 1

    if dropped_images:
        logger.warning(f"Dropped {dropped_images} image(s) with no pose sample within {max_pose_dt_ns/1e6:.0f}ms")

    return ReconstructionBuild(
        reconstruction=rec,
        camera_id_per_sensor=camera_id_per_sensor,
        image_id_per_record=image_id_per_record,
        timestamp_per_image=timestamp_per_image,
        dropped_images=dropped_images,
        intrinsics_source_per_sensor=intrinsics_source_per_sensor,
    )


def undistort_sensor_images(
    reconstruction: pycolmap.Reconstruction,
    camera_id_per_sensor: Dict[str, int],
    images_per_sensor: Dict[str, List[ImageRecord]],
    images_dir: Path,
    output_dir: Path,
    balance: float = 0.0,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, dict]:
    """Undistort every extracted frame for each sensor using that sensor's *refined*
    (post-bundle-adjustment, self-calibrated) camera model, writing undistorted copies to
    `output_dir/<sensor_id>/<timestamp_ns>.jpg` (same relpath layout as `images_dir`, so
    `ImageRecord.relpath`/`image_id_per_record` keys still line up). Distortion maps are
    computed once per sensor (fixed-focus cameras -- intrinsics are constant across an
    entire session) and reused via `cv2.remap` for every frame, rather than recomputed
    per frame.

    `balance` is OpenCV's usual undistort crop/FOV tradeoff knob (0 = crop to avoid black
    borders, 1 = keep the full FOV with black borders), passed as `alpha` to
    `cv2.getOptimalNewCameraMatrix` for the standard (plumb_bob-derived OPENCV) path.
    Not used for the fisheye path -- see the inline comment on why that model reuses the
    original K directly instead of estimating a new one.

    Returns a camera_model_per_sensor dict (fx, fy, cx, cy, dist=[0,0,0,0]) for the new,
    distortion-free camera matrix of each sensor's undistorted images -- pass this to
    detect_qr_codes's camera_model_per_sensor override instead of the (still-distorted)
    reconstruction camera when running QR detection against these undistorted images.
    """
    logger = logger or logging.getLogger("auki_reconstruction")
    images_dir = Path(images_dir)
    output_dir = Path(output_dir)
    camera_model_per_sensor: Dict[str, dict] = {}

    for sensor_id, camera_id in camera_id_per_sensor.items():
        records = images_per_sensor.get(sensor_id, [])
        if not records:
            continue
        camera = reconstruction.cameras[camera_id]
        w, h = camera.width, camera.height
        fx, fy, cx, cy = camera.params[:4]
        K = np.array([[fx, 0.0, cx], [0.0, fy, cy], [0.0, 0.0, 1.0]])
        dist = np.array(camera.params[4:], dtype=np.float64)

        if camera.model.name == "OPENCV_FISHEYE":
            D = dist.reshape(4, 1)
            # cv2.fisheye.estimateNewCameraMatrixForUndistortRectify degenerates here
            # (verified on a real frame): this lens's equidistant/kannala-brandt model
            # implies ~130 deg per-corner field angle, which a rectilinear pinhole can't
            # represent without the fitted focal length collapsing toward 0 trying to
            # fit the whole periphery in-frame. Reusing the original K as Knew instead
            # (standard fallback for this regime) undistorts the same central FOV the
            # camera already models and simply clips the extreme periphery -- confirmed
            # visually on a real frame (floor grid + door frame straighten out).
            K_new = K
            map1, map2 = cv2.fisheye.initUndistortRectifyMap(K, D, np.eye(3), K_new, (w, h), cv2.CV_16SC2)
        else:  # OPENCV (plumb_bob-derived)
            D = dist.reshape(4, 1)
            K_new, _ = cv2.getOptimalNewCameraMatrix(K, D, (w, h), alpha=balance)
            map1, map2 = cv2.initUndistortRectifyMap(K, D, np.eye(3), K_new, (w, h), cv2.CV_16SC2)

        out_sensor_dir = output_dir / sensor_id
        out_sensor_dir.mkdir(parents=True, exist_ok=True)
        for record in records:
            img = cv2.imread(str(images_dir / record.relpath), cv2.IMREAD_COLOR)
            if img is None:
                continue
            undistorted = cv2.remap(img, map1, map2, interpolation=cv2.INTER_LINEAR)
            cv2.imwrite(str(output_dir / record.relpath), undistorted)

        camera_model_per_sensor[sensor_id] = {
            "fx": float(K_new[0, 0]), "fy": float(K_new[1, 1]),
            "cx": float(K_new[0, 2]), "cy": float(K_new[1, 2]),
            "dist": [0.0, 0.0, 0.0, 0.0],
        }
        logger.info(f"{sensor_id}: undistorted {len(records)} frame(s) ({camera.model.name}) -> {out_sensor_dir}")

    return camera_model_per_sensor


def _camera_model_dict(camera: pycolmap.Camera) -> dict:
    return {
        "fx": camera.params[0], "fy": camera.params[1],
        "cx": camera.params[2], "cy": camera.params[3],
        "dist": list(camera.params[4:]),
    }


def _corners_to_camera_space_pose(corners, qr_size_m: float, camera_model: dict) -> Optional[pycolmap.Rigid3d]:
    """PnP pose estimate (auki_pnplab) for one QR detection's corners, converted from
    pnp-lab's OpenGL camera convention into COLMAP's OpenCV camera convention. Shared by
    detect_qr_codes (seeded intrinsics, pre-BA) and reestimate_qr_camera_poses (refined
    intrinsics, post-BA) -- only the camera_model passed in differs between the two."""
    try:
        est = auki_pnplab.estimate_square_pose_from_pixels(
            list(map(list, corners)), qr_size_m, camera_model
        )
    except RuntimeError:
        return None  # solver failed to converge on this detection

    p, r = est["pose"]["position"], est["pose"]["rotation"]
    # See detect_qr_codes' original comment for why convert_transform_target_convention
    # (not a symmetric conjugation) is the correct conversion here.
    cam_space_pose_flat = auki_geometry.convert_transform_target_convention(
        [p["x"], p["y"], p["z"], r["x"], r["y"], r["z"], r["w"]],
        _OPENGL_CAMERA_FRAME_ENTRY, _OPENCV_CAMERA_FRAME_ENTRY,
    )
    return _flat7_to_rigid3d(cam_space_pose_flat)


def reestimate_qr_camera_poses(
    reconstruction: pycolmap.Reconstruction,
    image_ids_per_qr: Dict[str, List[int]],
    corners_per_qr: Dict[str, List[list]],
    portal_sizes: Dict[str, float],
    ignore_distortion: bool = False,
    camera_model_per_sensor: Optional[Dict[str, dict]] = None,
):
    """Re-run just the PnP pose-estimation step for every existing QR detection, using
    each image's *refined* (post-bundle-adjustment, self-calibrated) camera intrinsics
    instead of the seeded-FOV guess detect_qr_codes had to use before BA ran. Corner
    pixel coordinates are unaffected by intrinsics, so this reuses corners_per_qr as-is
    rather than re-running qr_lab.scan.

    ignore_distortion: drop each camera's refined distortion coefficients (fx/fy/cx/cy
    only) before calling PnP. Confirmed by direct experiment (qr-pnp-diagnostics.ipynb)
    to raise convergence from ~25% to ~96% on a self-calibrated OPENCV_FISHEYE session
    with no real distortion this pipeline never fed into pnp-lab correctly (pnp-lab's
    Camera only implements Brown-Conrady distortion, not COLMAP's OPENCV_FISHEYE
    equidistant/Kannala-Brandt model -- k1-k4 get silently misapplied as k1/k2/p1/p2).
    Root cause isn't only the model mismatch, though: even cv2.fisheye's OWN correct
    inverse blows up well within the image bounds for this session's self-calibrated
    coefficients (BA fits them only where it has SfM tie points, with no constraint
    keeping the distortion curve invertible at the periphery), so properly modeling
    fisheye distortion here would still need a better-constrained self-calibration, not
    just a different undistort call. Dropping distortion entirely fixes *convergence*;
    it does not fully fix pose *accuracy* right at the image periphery.

    camera_model_per_sensor: use a fixed, given camera_model dict (same {fx,fy,cx,cy,
    dist} shape _camera_model_dict produces) per sensor id instead of that image's
    self-calibrated COLMAP camera -- e.g. recorded/factory intrinsics, when the
    self-calibrated ones are suspected unstable for some sensor (per-segment
    self-calibration on a teleop-scan session found the two rear surround cameras'
    fitted vertical focal length swinging by up to ~30deg of implied FOV across
    segments, while recorded intrinsics for this rig are flat, near-identical across
    every capture). Overrides ignore_distortion for the sensors it covers (a supplied
    camera_model's own "dist" is used as-is, not stripped); sensors absent from the
    dict still fall back to that image's self-calibrated camera. The camera's *pose*
    (reconstruction.images[image_id].cam_from_world(), used downstream by
    get_world_space_qr_codes) is never affected by this -- only the intrinsics fed into
    the corner-to-camera-space PnP solve.

    Returns (detections_per_qr, image_ids_per_qr, corners_per_qr) in the same
    triple-aligned shape detect_qr_codes produces (a detection is dropped from all three
    together if the refined PnP solve fails to converge, so callers can keep zipping
    them positionally, e.g. into save_portal_csv)."""
    detections_per_qr: Dict[str, List[pycolmap.Rigid3d]] = {}
    kept_image_ids_per_qr: Dict[str, List[int]] = {}
    kept_corners_per_qr: Dict[str, List[list]] = {}

    for short_id, image_ids in image_ids_per_qr.items():
        qr_size_m = portal_sizes[short_id]
        for image_id, corners in zip(image_ids, corners_per_qr[short_id]):
            image = reconstruction.images[image_id]
            sensor_id = image.name.split("/")[0]
            if camera_model_per_sensor is not None and sensor_id in camera_model_per_sensor:
                camera_model = dict(camera_model_per_sensor[sensor_id])
            else:
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


def detect_qr_codes(
    data: AukiSessionData,
    build: ReconstructionBuild,
    assumed_qr_size_m: float = 0.10,
    preset: str = "robust_fast",
    images_dir: Optional[Path] = None,
    camera_model_per_sensor: Optional[Dict[str, dict]] = None,
):
    """Run qr_lab.scan on every extracted frame and build the detections_per_qr /
    image_ids_per_qr / corners_per_qr structures refinement_util.process_QR and
    bundle_adjuster's QR loop-closure machinery already expect (camera-space
    pycolmap.Rigid3d detections, keyed by QR short id).

    Marker physical size is not recorded anywhere in this SDK (checked: registry,
    detection payload schema, and auki-domain's resource catalog all lack it) --
    assumed_qr_size_m is a documented placeholder, same caveat as the earlier
    auki-session-summary notebook's QR localization section.

    images_dir/camera_model_per_sensor let this run against a different image set than
    the one build_auki_rig_and_frames used -- e.g. undistorted copies from
    undistort_sensor_images, paired with their distortion-free camera model, so the QR
    corner detector and PnP solve both operate on already-undistorted pixels instead of
    raw fisheye-warped ones. When not given, defaults to the original extracted images
    and each sensor's camera as it exists in build.reconstruction at call time.
    """
    rec = build.reconstruction
    images_dir = Path(images_dir) if images_dir is not None else data.images_dir
    detections_per_qr: Dict[str, List[pycolmap.Rigid3d]] = {}
    image_ids_per_qr: Dict[str, List[int]] = {}
    corners_per_qr: Dict[str, List[list]] = {}
    detection_rows = []

    for sensor_id, records in data.images_per_sensor.items():
        if camera_model_per_sensor is not None:
            camera_model = camera_model_per_sensor[sensor_id]
        else:
            camera_id = build.camera_id_per_sensor[sensor_id]
            camera_model = _camera_model_dict(rec.cameras[camera_id])
        for record in records:
            image_id = build.image_id_per_record.get((sensor_id, record.timestamp_ns))
            if image_id is None:
                continue  # dropped for lack of a nearby pose sample
            gray = cv2.imread(str(images_dir / record.relpath), cv2.IMREAD_GRAYSCALE)
            if gray is None:
                continue
            result = qr_lab.scan(gray, preset=preset, refine=True)
            for code in result["codes"]:
                payload = code["code"]["payload"]
                short_id = payload.rsplit("/", 1)[-1]
                corners = list(map(list, code.get("refined_corners_source", code["corners_source"])))
                # pnp-lab returns "camera_from_marker" in its OpenGL camera convention
                # (x=right, y=up, z=backward) -- natural semantics, so in
                # auki_geometry's compose-contract terms this is "marker_to_camera"
                # (FROM=marker, TO=camera). We want to relabel *only* the camera side
                # into COLMAP's convention (x=right, y=down, z=forward), leaving the
                # marker's own local axes (pnp-lab's right/up/normal) untouched --
                # that's convert_transform_target_convention (camera is the target
                # side here), NOT a symmetric conjugation. A conjugation can never
                # turn an identity rotation into a non-identity one, but it must:
                # a marker facing the camera is identity in OpenGL (its normal =
                # OpenGL's own "backward" = toward the viewer) yet needs a real flip
                # in COLMAP terms (normal = -forward, since COLMAP's forward points
                # into the scene, away from the camera) -- verified against a real
                # detection of a floor-mounted marker (this session's floor tile
                # photo) whose normal must point up; the old conjugation-based
                # version got this backwards (pointed down) on every detection.
                cam_space_pose = _corners_to_camera_space_pose(corners, assumed_qr_size_m, camera_model)
                if cam_space_pose is None:
                    continue  # solver failed to converge on this detection, skip it

                detections_per_qr.setdefault(short_id, []).append(cam_space_pose)
                image_ids_per_qr.setdefault(short_id, []).append(image_id)
                corners_per_qr.setdefault(short_id, []).append(corners)
                detection_rows.append({
                    "sensor_id": sensor_id, "image_id": image_id,
                    "timestamp_ns": record.timestamp_ns, "payload": payload, "short_id": short_id,
                })

    portal_sizes = {short_id: assumed_qr_size_m for short_id in detections_per_qr}
    return detections_per_qr, image_ids_per_qr, corners_per_qr, portal_sizes, detection_rows
