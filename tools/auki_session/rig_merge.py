"""Merge several COLMAP reconstructions into one **without dissolving their rigs**.

`cpp/reconstruction_merge.cpp`'s `append_reconstruction` gives every registered image
its own fresh camera, rig, and frame. That is exactly right for the ARKit/DMT pipeline
it was written for -- there, every scan's images already sit in trivial one-camera rigs,
so flattening costs nothing and sidesteps id collisions between scans. It is wrong for
an Auki multi-sensor capture: those reconstructions carry a real Rig (several cameras
with fixed `sensor_from_rig` extrinsics sharing one `rig_from_world` per timestamp), and
flattening throws away the calibration, the shared-frame structure, and the fact that N
images were taken by the *same* physical camera -- a 7-sensor, 20-segment session comes
out the far side as 73k rigs and 73k cameras, one per image.

This module merges the same reconstructions the structure-preserving way:

* **Cameras** are deduplicated by *sensor*, not by image. Every segment's
  `left_rear_surround_rgb` images end up sharing one camera in the merged model, so its
  intrinsics stay one thing that can be reasoned about (and, in a fixed-intrinsics run,
  are verifiably identical everywhere).
* **Rigs** are deduplicated by *layout*: a rig is identified by its reference sensor
  plus the set of sensors it contains, and the first source to contribute a given layout
  defines its `sensor_from_rig` extrinsics. Later sources reusing that layout are
  checked against those extrinsics rather than trusted (see `extrinsics_tol_*`) -- a
  genuine disagreement is surfaced, not averaged or silently overwritten.
* **Frames** stay whole: one merged frame per source frame, keeping its `rig_from_world`
  and every image attached to it, so the merged model still knows which images were
  captured simultaneously by one rig.
* **Points3D** tracks are remapped onto the new image ids, dropping elements whose image
  was not carried over (unregistered frames).

Note on overlapping segments: this merges what it is given. Sliding-window segmentation
makes the same physical frame appear in two adjacent segments, and both copies survive
the merge as separate frames sharing an image name -- same as the flattening merge did.
Dedupe upstream if that matters for the consumer.
"""
from typing import Callable, Dict, List, NamedTuple, Optional, Sequence, Tuple
import gc
import logging

import numpy as np
import pycolmap


def sensor_key_from_image_name(name: str) -> str:
    """Default sensor identity: the `<sensor_id>/` prefix of an Auki image name
    (`left_arm_rgb/1753786956123456789.jpg`), which is how load_auki_session lays
    extracted frames out on disk."""
    return name.split("/")[0]


class RigMergeStats(NamedTuple):
    n_sources: int
    n_cameras: int
    n_rigs: int
    n_frames: int
    n_images: int
    n_points3D: int
    sensor_keys: List[str]
    rig_layouts: List[Tuple[str, Tuple[str, ...]]]  # (ref sensor key, other sensor keys)
    camera_param_conflicts: List[str]
    extrinsics_conflicts: List[str]
    dropped_track_elements: int
    duplicate_image_names: int  # same image contributed by >1 source (overlapping segments)


def _rigid3d_close(a: pycolmap.Rigid3d, b: pycolmap.Rigid3d, tol_m: float, tol_deg: float) -> bool:
    if np.linalg.norm(a.translation - b.translation) > tol_m:
        return False
    delta = (a.inverse() * b).rotation.quat
    angle_deg = np.degrees(2.0 * np.arcsin(np.clip(np.linalg.norm(delta[:3]), 0.0, 1.0)))
    return angle_deg <= tol_deg


def _as_sim3d(transform) -> pycolmap.Sim3d:
    if transform is None or isinstance(transform, pycolmap.Sim3d):
        return transform
    arr = np.asarray(transform, dtype=float)
    if arr.shape == (4, 4):
        arr = arr[:3, :4]
    if arr.shape != (3, 4):
        raise ValueError(f"Expected a Sim3d, a 4x4 or a 3x4 matrix, got shape {arr.shape}")
    return pycolmap.Sim3d(arr)


def _camera_sensor_keys(rec: pycolmap.Reconstruction, sensor_key_fn, label, logger) -> Dict[int, str]:
    """camera_id -> sensor key, derived from the images that camera actually took."""
    keys: Dict[int, str] = {}
    for image in rec.images.values():
        key = sensor_key_fn(image.name)
        previous = keys.setdefault(image.camera_id, key)
        if previous != key:
            raise ValueError(
                f"{label}: camera {image.camera_id} is used by images from more than one sensor "
                f"({previous!r} and {key!r}) -- sensor_key_fn cannot identify this reconstruction's "
                f"cameras, pass one that can"
            )
    for camera_id in rec.cameras:
        if camera_id not in keys:
            # A camera the rigs reference but no image in this source uses. Falling back
            # to the camera id keeps it distinguishable; it is only stable because every
            # segment of one Auki session assigns camera ids from the same sorted sensor
            # list, so the same id means the same sensor across sources.
            keys[camera_id] = f"__camera_id_{camera_id}"
            logger.warning(f"{label}: camera {camera_id} has no images -- keyed by camera id as a fallback")
    return keys


def merge_reconstructions_preserving_rigs(
    sources: Sequence[pycolmap.Reconstruction],
    transforms: Optional[Sequence] = None,
    labels: Optional[Sequence[str]] = None,
    sensor_key_fn: Callable[[str], str] = sensor_key_from_image_name,
    extrinsics_tol_m: float = 1e-6,
    extrinsics_tol_deg: float = 1e-4,
    logger: Optional[logging.Logger] = None,
) -> Tuple[pycolmap.Reconstruction, RigMergeStats]:
    """Merge `sources` into one reconstruction, keeping each source's rig/frame structure
    and sharing one camera per physical sensor across all of them.

    Args:
        sources: reconstructions to merge, in order. The first one to use a given sensor
            defines that sensor's camera, and the first to use a given rig layout defines
            that layout's extrinsics. An entry may instead be a zero-argument callable
            returning the reconstruction -- pass those when merging many segments, so
            each is read, consumed and released one at a time instead of every source
            being resident at once (20 segments of a 7-sensor session is tens of GB).
        transforms: optional per-source transform into the common frame -- a
            `pycolmap.Sim3d`, a 4x4, or a 3x4. **Applied in place** to the source
            reconstruction (`Reconstruction.transform`), matching how the stitching
            notebooks already read one reconstruction per segment and transform it.
            Pass None (or a None entry) for sources already in the common frame.
        labels: names used in log messages and conflict reports, one per source.
        sensor_key_fn: maps an image name to the identity of the sensor that took it.
        extrinsics_tol_m / extrinsics_tol_deg: how far a later source's `sensor_from_rig`
            may differ from the layout's established extrinsics before it is reported as
            a conflict. Defaults are near-exact on purpose: in this pipeline
            `sensor_from_rig` comes from the registry and is never a bundle-adjustment
            parameter (see pipeline/auki_session/refine.py's check_rig_offsets_unchanged), so any
            disagreement at all is a finding rather than expected drift. Merging
            *segments* of one session is the exception: each segment averages its own
            time window of the same poselog, so the extrinsics agree only to a few
            micrometres and every segment after the first reports a conflict at these
            defaults. Raise the tolerance (~1e-4 m) for that case if the log noise is
            unwanted -- but read the magnitudes first, since the same warning at
            millimetre scale would mean something real.

    Returns:
        (merged reconstruction, RigMergeStats). Conflicts are recorded in the stats and
        logged as warnings; the first source's value is kept in every case.
    """
    logger = logger or logging.getLogger(__name__)
    labels = list(labels) if labels is not None else [f"source_{i}" for i in range(len(sources))]
    transforms = list(transforms) if transforms is not None else [None] * len(sources)
    if not (len(labels) == len(transforms) == len(sources)):
        raise ValueError("sources, transforms and labels must have the same length")

    dest = pycolmap.Reconstruction()
    camera_id_by_sensor: Dict[str, int] = {}
    rig_id_by_layout: Dict[Tuple[str, Tuple[str, ...]], int] = {}
    sensor_from_rig_by_layout: Dict[Tuple[str, Tuple[str, ...]], Dict[str, pycolmap.Rigid3d]] = {}
    camera_param_conflicts: List[str] = []
    extrinsics_conflicts: List[str] = []
    dropped_track_elements = 0
    seen_image_names: set = set()
    duplicate_image_names = 0

    next_camera_id = next_rig_id = next_frame_id = next_image_id = 1

    for source, transform, label in zip(sources, transforms, labels):
        if callable(source):
            source = source()
        sim3d = _as_sim3d(transform)
        if sim3d is not None:
            source.transform(sim3d)

        sensor_key_per_camera = _camera_sensor_keys(source, sensor_key_fn, label, logger)

        # --- cameras: one per sensor, shared across every source ---------------------
        for camera_id, camera in sorted(source.cameras.items()):
            key = sensor_key_per_camera[camera_id]
            if key not in camera_id_by_sensor:
                new_camera = pycolmap.Camera(
                    model=camera.model, width=camera.width, height=camera.height,
                    params=list(camera.params), camera_id=next_camera_id,
                )
                dest.add_camera(new_camera)
                camera_id_by_sensor[key] = next_camera_id
                next_camera_id += 1
            else:
                existing = dest.cameras[camera_id_by_sensor[key]]
                if existing.model != camera.model or not np.allclose(existing.params, camera.params):
                    msg = (f"{label}: sensor {key!r} intrinsics differ from the merged model's "
                           f"({list(camera.params)} vs. {list(existing.params)}) -- keeping the first")
                    camera_param_conflicts.append(msg)
                    logger.warning(msg)

        def _new_sensor_t(sensor_key: str) -> pycolmap.sensor_t:
            return pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=camera_id_by_sensor[sensor_key])

        # --- rigs: one per distinct layout, extrinsics fixed by the first contributor -
        rig_id_map: Dict[int, int] = {}
        for rig_id, rig in sorted(source.rigs.items()):
            ref_key = sensor_key_per_camera[rig.ref_sensor_id.id]
            other_keys = tuple(sorted(sensor_key_per_camera[s.id] for s in rig.non_ref_sensors))
            layout = (ref_key, other_keys)

            if layout not in rig_id_by_layout:
                new_rig = pycolmap.Rig()
                new_rig.rig_id = next_rig_id
                new_rig.add_ref_sensor(_new_sensor_t(ref_key))
                extrinsics = {}
                for sensor_t in rig.non_ref_sensors:
                    key = sensor_key_per_camera[sensor_t.id]
                    sensor_from_rig = rig.sensor_from_rig(sensor_t)
                    new_rig.add_sensor(_new_sensor_t(key), sensor_from_rig)
                    extrinsics[key] = sensor_from_rig
                dest.add_rig(new_rig)
                rig_id_by_layout[layout] = next_rig_id
                sensor_from_rig_by_layout[layout] = extrinsics
                next_rig_id += 1
            else:
                established = sensor_from_rig_by_layout[layout]
                for sensor_t in rig.non_ref_sensors:
                    key = sensor_key_per_camera[sensor_t.id]
                    this = rig.sensor_from_rig(sensor_t)
                    if not _rigid3d_close(established[key], this, extrinsics_tol_m, extrinsics_tol_deg):
                        msg = (f"{label}: rig layout {layout[0]}+{list(layout[1])}: sensor {key!r} "
                               f"sensor_from_rig differs from the merged model's "
                               f"(t={this.translation} vs. {established[key].translation}) -- keeping the first")
                        extrinsics_conflicts.append(msg)
                        logger.warning(msg)
            rig_id_map[rig_id] = rig_id_by_layout[layout]

        # --- frames + images: structure carried over as-is ---------------------------
        image_id_old_to_new: Dict[int, int] = {}
        for frame_id in sorted(source.reg_frame_ids()):
            frame = source.frames[frame_id]
            new_frame = pycolmap.Frame()
            new_frame.frame_id = next_frame_id
            new_frame.rig_id = rig_id_map[frame.rig_id]
            new_frame.rig_from_world = frame.rig_from_world

            new_images = []
            # frame.image_ids yields data_t (sensor + image id), sorted here by image id
            # so a frame's images keep their capture order in the merged model.
            for old_image_id in sorted(d.id for d in frame.image_ids):
                image = source.images[old_image_id]
                key = sensor_key_per_camera[image.camera_id]
                new_frame.add_data_id(pycolmap.data_t(sensor_id=_new_sensor_t(key), id=next_image_id))
                new_images.append((image, key, next_image_id))
                image_id_old_to_new[old_image_id] = next_image_id
                next_image_id += 1
            dest.add_frame(new_frame)

            for image, key, new_image_id in new_images:
                # Fresh Point2Ds (pixel coordinates only) -- the point3D links are
                # re-established below by add_point3D, which writes them back into the
                # images named by each track.
                points2D = pycolmap.Point2DList([pycolmap.Point2D(p.xy) for p in image.points2D])
                new_image = pycolmap.Image(image.name, points2D, camera_id_by_sensor[key], new_image_id)
                new_image.frame_id = new_frame.frame_id
                dest.add_image(new_image)
                if image.name in seen_image_names:
                    duplicate_image_names += 1
                seen_image_names.add(image.name)
            dest.register_frame(new_frame.frame_id)
            next_frame_id += 1

        # --- points3D: tracks remapped onto the new image ids -------------------------
        for point3D in source.points3D.values():
            track = pycolmap.Track()
            for element in point3D.track.elements:
                new_image_id = image_id_old_to_new.get(element.image_id)
                if new_image_id is None:
                    dropped_track_elements += 1
                    continue
                track.add_element(new_image_id, element.point2D_idx)
            if track.length() == 0:
                continue
            dest.add_point3D(point3D.xyz, track, point3D.color)

        logger.info(f"{label}: merged -> {dest.num_rigs()} rig(s), {dest.num_cameras()} camera(s), "
                    f"{dest.num_frames()} frame(s), {dest.num_images()} image(s), "
                    f"{dest.num_points3D()} point(s)")
        # Release this source before reading the next one -- only meaningful when the
        # caller passed a callable, but harmless either way.
        del source
        gc.collect()

    stats = RigMergeStats(
        n_sources=len(sources), n_cameras=dest.num_cameras(), n_rigs=dest.num_rigs(),
        n_frames=dest.num_frames(), n_images=dest.num_images(), n_points3D=dest.num_points3D(),
        sensor_keys=sorted(camera_id_by_sensor), rig_layouts=sorted(rig_id_by_layout),
        camera_param_conflicts=camera_param_conflicts, extrinsics_conflicts=extrinsics_conflicts,
        dropped_track_elements=dropped_track_elements, duplicate_image_names=duplicate_image_names,
    )
    if duplicate_image_names:
        logger.info(f"{duplicate_image_names} image(s) were contributed by more than one source "
                    f"(expected with overlapping segments -- each copy keeps its own frame)")
    return dest, stats


def describe_rigs(rec: pycolmap.Reconstruction, sensor_key_fn=sensor_key_from_image_name) -> List[str]:
    """One human-readable line per rig: its sensors and how many frames/images use it.
    Handy as a before/after check that a merge kept the rig structure."""
    sensor_key_per_camera: Dict[int, str] = {}
    for image in rec.images.values():
        sensor_key_per_camera.setdefault(image.camera_id, sensor_key_fn(image.name))

    frames_per_rig: Dict[int, int] = {}
    images_per_rig: Dict[int, int] = {}
    for frame in rec.frames.values():
        frames_per_rig[frame.rig_id] = frames_per_rig.get(frame.rig_id, 0) + 1
        images_per_rig[frame.rig_id] = images_per_rig.get(frame.rig_id, 0) + frame.num_data_ids()

    lines = []
    for rig_id, rig in sorted(rec.rigs.items()):
        ref = sensor_key_per_camera.get(rig.ref_sensor_id.id, f"camera_{rig.ref_sensor_id.id}")
        others = sorted(sensor_key_per_camera.get(s.id, f"camera_{s.id}") for s in rig.non_ref_sensors)
        lines.append(f"rig {rig_id}: ref={ref}"
                     f"{' + ' + ', '.join(others) if others else ''} "
                     f"({rig.num_sensors()} sensor(s), {frames_per_rig.get(rig_id, 0)} frame(s), "
                     f"{images_per_rig.get(rig_id, 0)} image(s))")
    return lines
