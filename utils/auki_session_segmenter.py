"""Split one Auki capture session into multiple shorter, overlapping sub-sessions --
each a fully valid Auki session directory in its own right (same sensorlogs/poselogs/
registries layout `load_auki_session` already reads), covering a sliding time window of
the original. Built to experiment with per-segment reconstruction speed, and with
QR-marker overlap between adjacent segments as the anchor for stitching separate
per-segment reconstructions back into one.

Registries are content-addressed and identical across every segment (sensor/frame/clock
definitions don't change just because the time window does), so they're copied once,
verbatim, rather than rewritten. Only sensorlogs/poselogs are actually re-segmented --
same manifest as the source log (segment_duration_ns/retention_ns/clock/frame/sensor
refs all unchanged), just `session_id` patched and entries filtered to the window,
written via auki_logs.Log.open()/.append() (verified round-trips byte-identical
payloads before relying on it for the real segmentation).
"""
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional
import logging
import shutil

import auki_layout
import auki_logs


class SegmentInfo(NamedTuple):
    segment_id: str
    start_ns: int
    end_ns: int
    duration_s: float
    is_partial: bool  # True for a trailing segment shorter than the requested window


def _discover_sensor_ids(session_root) -> List[str]:
    sensorlogs_dir = Path(session_root) / "sensorlogs"
    return sorted(p.name for p in sensorlogs_dir.iterdir() if p.is_dir())


def _read_log(session_root, from_id: str, to_id: Optional[str] = None):
    """Read a sensorlog (to_id=None) or poselog (to_id given) -- returns (manifest, entries)."""
    path = (
        auki_layout.sensorlog_path(session_root, from_id)
        if to_id is None
        else auki_layout.poselog_path(session_root, from_id, to_id)
    )
    reader = auki_logs.Log.read(path)
    return reader.manifest(), reader.entries()


def compute_segment_windows(
    overall_start_ns: int, overall_end_ns: int, window_s: float, overlap_s: float
) -> List[SegmentInfo]:
    """Sliding windows of `window_s` seconds, `overlap_s` seconds of overlap between
    consecutive windows (i.e. stride = window_s - overlap_s), starting at
    overall_start_ns. The last window is clipped to overall_end_ns rather than shifted
    back to stay full-length -- so it can come out shorter than `window_s` (flagged via
    `is_partial`) instead of silently changing that segment's overlap with its
    predecessor."""
    if overlap_s >= window_s:
        raise ValueError(f"overlap_s ({overlap_s}) must be < window_s ({window_s})")
    window_ns = int(window_s * 1e9)
    stride_ns = int((window_s - overlap_s) * 1e9)

    segments = []
    idx = 0
    start = overall_start_ns
    while start < overall_end_ns:
        end = min(start + window_ns, overall_end_ns)
        segments.append(SegmentInfo(
            segment_id=f"segment_{idx:03d}",
            start_ns=start,
            end_ns=end,
            duration_s=(end - start) / 1e9,
            is_partial=(end - start) < window_ns,
        ))
        idx += 1
        start += stride_ns
    return segments


def segment_auki_session(
    src_app_root,
    src_session_id: str,
    dst_app_root,
    window_s: float = 60.0,
    overlap_s: float = 10.0,
    logger: Optional[logging.Logger] = None,
) -> List[SegmentInfo]:
    """Split one Auki session into sliding-window sub-sessions under dst_app_root, one
    subdirectory per segment (segment_000, segment_001, ...), each independently
    loadable via load_auki_session(dst_app_root, segment_id, ...).

    Returns the list of SegmentInfo actually written, in order.
    """
    logger = logger or logging.getLogger("auki_session_segmenter")
    src_app_root, dst_app_root = Path(src_app_root), Path(dst_app_root)
    src_session_root = auki_layout.session_root(str(src_app_root), src_session_id)

    sensor_ids = _discover_sensor_ids(src_session_root)

    # Read every source log once (sensorlogs + each sensor's own poselog + the shared
    # world->base_link trajectory) -- cheap at this scale (a few thousand entries per
    # log) and avoids re-reading from disk once per segment.
    sensor_logs: Dict[str, tuple] = {}   # sensor_id -> (manifest, entries)
    pose_logs: Dict[str, tuple] = {}     # frame_id -> (manifest, entries), base_link->frame_id
    sensor_to_frame: Dict[str, str] = {}

    overall_start_ns, overall_end_ns = None, None
    for sensor_id in sensor_ids:
        manifest, entries = _read_log(src_session_root, sensor_id)
        sensor_logs[sensor_id] = (manifest, entries)
        frame_id = manifest["frame"]["id"]
        sensor_to_frame[sensor_id] = frame_id

        pose_manifest, pose_entries = _read_log(src_session_root, "base_link", frame_id)
        pose_logs[frame_id] = (pose_manifest, pose_entries)

        for e in entries:
            overall_start_ns = e.timestamp_ns if overall_start_ns is None else min(overall_start_ns, e.timestamp_ns)
            overall_end_ns = e.timestamp_ns if overall_end_ns is None else max(overall_end_ns, e.timestamp_ns)

    world_manifest, world_entries = _read_log(src_session_root, "world", "base_link")
    for e in world_entries:
        overall_start_ns = min(overall_start_ns, e.timestamp_ns)
        overall_end_ns = max(overall_end_ns, e.timestamp_ns)

    logger.info(f"Session spans {(overall_end_ns - overall_start_ns) / 1e9:.2f}s "
                f"({len(sensor_ids)} sensor(s))")

    segments = compute_segment_windows(overall_start_ns, overall_end_ns, window_s, overlap_s)
    logger.info(f"Splitting into {len(segments)} segment(s), window={window_s}s overlap={overlap_s}s")

    # Registries are content-addressed and shared across every segment -- copy once,
    # verbatim, rather than rewriting.
    dst_app_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src_app_root / "registries", dst_app_root / "registries", dirs_exist_ok=True)

    def write_filtered(dst_root, from_id, to_id_or_none, manifest, entries, segment_id, start_ns, end_ns):
        new_manifest = dict(manifest)
        new_manifest["session_id"] = segment_id
        path = (
            auki_layout.sensorlog_path(dst_root, from_id)
            if to_id_or_none is None
            else auki_layout.poselog_path(dst_root, from_id, to_id_or_none)
        )
        log = auki_logs.Log.open(path, new_manifest)
        n = 0
        for e in entries:
            if start_ns <= e.timestamp_ns <= end_ns:
                log.append(e.timestamp_ns, e.payload)
                n += 1
        log.close()
        return n

    for seg in segments:
        dst_session_root = auki_layout.session_root(str(dst_app_root), seg.segment_id)
        counts = {}
        for sensor_id in sensor_ids:
            manifest, entries = sensor_logs[sensor_id]
            counts[sensor_id] = write_filtered(
                dst_session_root, sensor_id, None, manifest, entries, seg.segment_id, seg.start_ns, seg.end_ns
            )
            frame_id = sensor_to_frame[sensor_id]
            pose_manifest, pose_entries = pose_logs[frame_id]
            write_filtered(
                dst_session_root, "base_link", frame_id, pose_manifest, pose_entries,
                seg.segment_id, seg.start_ns, seg.end_ns
            )
        world_count = write_filtered(
            dst_session_root, "world", "base_link", world_manifest, world_entries,
            seg.segment_id, seg.start_ns, seg.end_ns
        )
        partial_tag = " (partial)" if seg.is_partial else ""
        logger.info(f"{seg.segment_id}{partial_tag}: {seg.duration_s:.2f}s, "
                    f"world_samples={world_count}, per-sensor frames={counts}")

    return segments
