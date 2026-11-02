"""Capture discovery and chronology, independent of the scientific pipeline."""
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)
LEGACY_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}[_-]\d{2}-\d{2}-\d{2}")
TIME_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", re.I)


def capture_folder_id(name: str):
    identity = name.removeprefix("dmt_scan_")
    return identity if UUID_PATTERN.fullmatch(identity) or LEGACY_PATTERN.fullmatch(identity) else None


def discover_capture_paths(directory: Path):
    return [path for path in directory.iterdir() if path.is_dir() and capture_folder_id(path.name)]


def _time_ms(value):
    if not isinstance(value, str) or not TIME_PATTERN.fullmatch(value):
        return None
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00").replace("z", "+00:00"))
        delta = instant.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
        # Match DMT's millisecond comparison, including .NET's 7-digit fractions.
        return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000
    except ValueError:
        return None


def capture_sort_key(dataset_path: Path, job_root: Path, logger=None):
    identity = capture_folder_id(dataset_path.name)
    if identity is None:
        raise ValueError(f"Invalid capture directory: {dataset_path.name}")
    # Global receives datasets/<id>; update receives refined/local/<id>.
    candidates = [
        dataset_path / "Manifest.json",
        dataset_path / "sfm" / "capture_metadata.json",
        job_root / "refined" / "local" / dataset_path.name / "sfm" / "capture_metadata.json",
        job_root / "datasets" / dataset_path.name / "Manifest.json",
    ]
    for path in dict.fromkeys(candidates):
        if not path.exists():
            continue
        with path.open() as source:
            data = json.load(source)
        raw_id = data.get("captureId" if path.name == "capture_metadata.json" else "id")
        normalized_id = capture_folder_id(raw_id) if isinstance(raw_id, str) else None
        if (path.name == "capture_metadata.json" or UUID_PATTERN.fullmatch(identity)) and normalized_id != identity:
            raise ValueError(f"Capture metadata identity does not match {identity}: {path}")
        timestamp = _time_ms(data.get("scanStartTime"))
        if timestamp is not None:
            return (0, timestamp, identity)
    if UUID_PATTERN.fullmatch(identity):
        raise ValueError(f"Capture {identity} has no valid recording start time")
    (logger or logging.getLogger(__name__)).warning(
        "Capture %s has unknown recording time; placing it after dated captures", identity
    )
    # Legacy wall-clock IDs have no timezone. Preserve their previous lexical
    # ordering without pretending to know an absolute recording time.
    return (1, 0, identity)


def sort_capture_paths(paths, job_root: Path, logger=None):
    return sorted(paths, key=lambda path: capture_sort_key(path, job_root, logger))
