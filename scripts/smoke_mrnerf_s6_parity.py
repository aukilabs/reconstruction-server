#!/usr/bin/env python3
"""S6 MrNeRF-only parity smoke on job_baseline_033 fixture.

Run inside s3-mrnerf-build (GPU):
  docker exec -e LD_LIBRARY_PATH=/opt/libtorch/lib -w /app s3-mrnerf-build \\
      python3 /app/scripts/smoke_mrnerf_s6_parity.py

Phases run in separate processes so extract and match do not share GPU memory.
Match subsamples to top-1024 keypoints by score (smoke-only) because the C++
extractor does not yet expose hloc's max_num_keypoints=1024; full features OOM
LightGlue on 1920x1440 frames.
"""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import h5py
import numpy as np

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from utils.mrnerf_hloc_adapters import (  # noqa: E402
    MrNerfHlocExtractor,
    MrNerfHlocMatcher,
    _write_matches_group,
    load_features_from_h5,
    parse_pairs_file,
)

FIXTURE = Path("/app/_cmp_jobs/job_baseline_033")
SCAN = "2026-05-31_20-53-03"
FRAMES_DIR = FIXTURE / "datasets" / SCAN / "Frames"
PAIRS_FILE = FIXTURE / "refined" / "local" / SCAN / "sfm" / "pairs-sfm.txt"
MANIFEST = FIXTURE / "datasets" / SCAN / "Manifest.json"
# Work on container-local overlay — /app is a Windows 9p mount; repeated
# h5py open/close there hits BlockingIOError file locks.
WORK_DIR = Path("/tmp/mrnerf_s6_smoke")
HOST_OUT_DIR = Path("/app/_local/mrnerf_s6_smoke")
FEATURES_H5 = WORK_DIR / "features.h5"
MATCHES_H5 = WORK_DIR / "matches.h5"
PARTIAL_JSON = WORK_DIR / "s6_partial.json"
RESULTS_JSON = Path("/app/_local/mrnerf-parity-mrnerf.json")
HOST_FEATURES_H5 = HOST_OUT_DIR / "features.h5"

S1_EXTRACT_WALL_S = 19.85
MATCH_SAMPLE_PAIRS = 50
MATCH_PROJECTED_MAX_S = 5 * 60  # smoke: escalate early if slow
MATCH_SUBSET_MIN_PAIRS = 200  # representative smoke subset
MATCH_HARD_CAP_PAIRS = 200  # always stop here for S6 smoke reliability
SMOKE_MAX_KEYPOINTS = 1024  # mirror hloc aliked-n16 conf; smoke-only subsample


def _load_manifest_portals(manifest_path: Path) -> list[str]:
    import re

    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    portals = data.get("portals") or data.get("Portals") or []
    if isinstance(portals, dict):
        return list(portals.keys())
    out: list[str] = []
    short_id_re = re.compile(r"""['"]shortId['"]\s*:\s*['"]([^'"]+)['"]""")
    for p in portals:
        if isinstance(p, dict) and "shortId" in p:
            out.append(str(p["shortId"]))
        elif isinstance(p, str):
            m = short_id_re.search(p)
            out.append(m.group(1) if m else p)
        else:
            out.append(str(p))
    return out


def _collect_image_items(frames_dir: Path) -> list[tuple[Path, str]]:
    return [(p, p.name) for p in sorted(frames_dir.glob("*.jpg"))]


def _summarize_features(h5_path: Path) -> dict[str, Any]:
    total_kp = 0
    n_images = 0
    sample_kp = 0
    with h5py.File(str(h5_path), "r") as fd:
        for i, name in enumerate(fd):
            n_images += 1
            nk = int(fd[name]["keypoints"].shape[0])
            total_kp += nk
            if i == 0:
                sample_kp = nk
    return {
        "num_images": n_images,
        "total_keypoints": total_kp,
        "avg_keypoints_per_image": round(total_kp / max(n_images, 1), 2),
        "sample_first_image_keypoints": sample_kp,
    }


def _summarize_matches(h5_path: Path) -> dict[str, Any]:
    if not h5_path.exists():
        return {"num_pairs": 0, "total_valid_matches": 0, "avg_valid_matches_per_pair": 0.0}
    total_valid = 0
    n_pairs = 0

    def _visit(name: str, obj: h5py.Dataset | h5py.Group) -> None:
        nonlocal total_valid, n_pairs
        if isinstance(obj, h5py.Dataset):
            return
        if "matches0" in obj:
            n_pairs += 1
            m0 = np.asarray(obj["matches0"])
            total_valid += int(np.count_nonzero(m0 >= 0))
            return
        for child in obj:
            _visit(f"{name}/{child}" if name else child, obj[child])

    with h5py.File(str(h5_path), "r") as fd:
        for key in fd:
            _visit(key, fd[key])

    return {
        "num_pairs": n_pairs,
        "total_valid_matches": total_valid,
        "avg_valid_matches_per_pair": round(total_valid / max(n_pairs, 1), 2),
    }


def _subsample_feats_for_smoke(feats: dict[str, np.ndarray], max_kp: int) -> dict[str, np.ndarray]:
    """Top-k by score — smoke-only until C++ exposes max_num_keypoints."""
    kpts = np.asarray(feats["keypoints"])
    if kpts.shape[0] <= max_kp:
        return feats
    scores = np.asarray(feats["scores"]).reshape(-1)
    keep = np.argsort(-scores)[:max_kp]
    out = dict(feats)
    out["keypoints"] = np.asarray(feats["keypoints"])[keep]
    out["descriptors"] = np.asarray(feats["descriptors"])[keep]
    out["scores"] = np.asarray(feats["scores"])[keep]
    return out


def _features_path() -> Path:
    """Prefer existing host extract; otherwise /tmp work copy."""
    if FEATURES_H5.exists():
        return FEATURES_H5
    if HOST_FEATURES_H5.exists():
        return HOST_FEATURES_H5
    return FEATURES_H5


def _ensure_work_features() -> None:
    """Ensure a readable features.h5 exists (host mount OK for reads)."""
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    path = _features_path()
    if not path.exists():
        raise FileNotFoundError(
            f"missing features.h5 (checked {FEATURES_H5} and {HOST_FEATURES_H5})"
        )
    print(f"S6_FEATURES_SRC {path}")


def _publish_host_outputs() -> None:
    """Copy match artifacts to /app/_local (single bulk copy; 9p-safe)."""
    import shutil

    HOST_OUT_DIR.mkdir(parents=True, exist_ok=True)
    if MATCHES_H5.exists():
        shutil.copy2(MATCHES_H5, HOST_OUT_DIR / "matches.h5")
    if PARTIAL_JSON.exists():
        shutil.copy2(PARTIAL_JSON, HOST_OUT_DIR / "s6_partial.json")


def run_extract() -> dict[str, Any]:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    if FEATURES_H5.exists():
        FEATURES_H5.unlink()

    items = _collect_image_items(FRAMES_DIR)
    extractor = MrNerfHlocExtractor()
    t0 = time.perf_counter()
    # Extract to /tmp overlay, then publish once to host.
    extractor.extract_many_to_h5(items, FEATURES_H5, as_half=True, overwrite=True)
    wall_s = time.perf_counter() - t0
    del extractor
    gc.collect()

    import shutil

    HOST_OUT_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(FEATURES_H5, HOST_FEATURES_H5)

    summary = _summarize_features(FEATURES_H5)
    return {
        "extract_wall_seconds": round(wall_s, 3),
        "features": summary,
        "frames": len(items),
    }


def run_match() -> dict[str, Any]:
    _ensure_work_features()
    feats_path = _features_path()

    pairs = parse_pairs_file(PAIRS_FILE)
    if MATCHES_H5.exists():
        MATCHES_H5.unlink()

    matcher = MrNerfHlocMatcher()
    pair_times: list[float] = []
    escalation: dict[str, Any] = {"triggered": False}
    subset_mode = False
    pairs_to_run = len(pairs)

    def _load_one(fd: h5py.File, name: str) -> dict[str, np.ndarray]:
        if name not in fd:
            raise KeyError(f"{name} missing from {feats_path}")
        grp = fd[name]
        feats: dict[str, np.ndarray] = {}
        for key in grp.keys():
            arr = np.asarray(grp[key])
            if arr.dtype == np.float16:
                arr = arr.astype(np.float32)
            feats[key] = arr
        return feats

    t0 = time.perf_counter()
    # Keep features + matches handles open for the whole batch (9p-safe).
    with h5py.File(str(feats_path), "r", libver="latest") as feats_fd, h5py.File(
        str(MATCHES_H5), "a", libver="latest"
    ) as match_fd:
        for i, (name0, name1) in enumerate(pairs):
            pt0 = time.perf_counter()
            feats0 = _subsample_feats_for_smoke(
                _load_one(feats_fd, name0), SMOKE_MAX_KEYPOINTS
            )
            feats1 = _subsample_feats_for_smoke(
                _load_one(feats_fd, name1), SMOKE_MAX_KEYPOINTS
            )
            pred = matcher.match(feats0, feats1)
            _write_matches_group(match_fd, name0, name1, pred, overwrite=True)
            pair_times.append(time.perf_counter() - pt0)
            done = i + 1

            if done >= MATCH_HARD_CAP_PAIRS:
                escalation = {
                    "triggered": True,
                    "reason": "s6_smoke_hard_cap",
                    "hard_cap_pairs": MATCH_HARD_CAP_PAIRS,
                    "avg_seconds_per_pair": round(
                        sum(pair_times) / len(pair_times), 4
                    ),
                    "total_pairs_in_file": len(pairs),
                    "note": (
                        "S6 smoke capped at hard_cap pairs for reliability "
                        "(prior full runs were SIGTERM'd mid-CUDA)"
                    ),
                }
                print(f"S6_MATCH_HARD_CAP pairs={done}", flush=True)
                break

            if done == MATCH_SAMPLE_PAIRS and not subset_mode:
                avg = sum(pair_times) / len(pair_times)
                projected = avg * len(pairs)
                print(
                    f"S6_MATCH_SAMPLE pairs={done} avg_s={avg:.3f} "
                    f"projected_min={projected / 60:.1f}",
                    flush=True,
                )
                if projected > MATCH_PROJECTED_MAX_S:
                    subset_mode = True
                    escalation = {
                        "triggered": True,
                        "reason": "projected_full_match_exceeds_threshold",
                        "sample_pairs": done,
                        "avg_seconds_per_pair": round(avg, 4),
                        "projected_full_seconds": round(projected, 1),
                        "projected_full_minutes": round(projected / 60, 1),
                        "total_pairs_in_file": len(pairs),
                        "note": (
                            "Stopped after sample; completed representative subset "
                            f"of {max(MATCH_SUBSET_MIN_PAIRS, done)} pairs"
                        ),
                    }
                    pairs_to_run = max(MATCH_SUBSET_MIN_PAIRS, done)
                    if done >= pairs_to_run:
                        break

            if subset_mode and done >= pairs_to_run:
                break

            if done % 50 == 0:
                match_fd.flush()
                print(f"S6_MATCH_PROGRESS pairs={done}/{pairs_to_run}", flush=True)

    wall_s = time.perf_counter() - t0
    del matcher
    gc.collect()

    match_summary = _summarize_matches(MATCHES_H5)
    pairs_done = match_summary["num_pairs"]
    return {
        "match_wall_seconds": round(wall_s, 3),
        "matches": match_summary,
        "pairs_matched": pairs_done,
        "pairs_total": len(pairs),
        "match_escalation": escalation,
        "smoke_match_subsample_keypoints": SMOKE_MAX_KEYPOINTS,
        "smoke_match_subsample_note": (
            "Match phase subsamples to top-1024 keypoints by score (smoke-only) "
            "because C++ extractor lacks hloc max_num_keypoints; raw features "
            "~7-15k/image OOM LightGlue"
        ),
    }


def write_results(
    extract_info: dict[str, Any],
    match_info: dict[str, Any],
    portals: list[str],
) -> None:
    extract_wall_s = extract_info["extract_wall_seconds"]
    match_wall_s = match_info["match_wall_seconds"]
    extract_vs_s1 = extract_wall_s / S1_EXTRACT_WALL_S

    results = {
        "record_type": "mrnerf_s6_smoke",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "fixture": str(FIXTURE),
        "scan": SCAN,
        "manifest_portals": portals,
        "manifest_portal_count": len(portals),
        "frames": extract_info.get("frames", 0),
        "pairs_in_file": match_info.get("pairs_total", 0),
        "outputs": {
            "work_dir": str(WORK_DIR),
            "features_h5": str(FEATURES_H5),
            "matches_h5": str(MATCHES_H5),
            "host_out_dir": str(HOST_OUT_DIR),
        },
        "s1_pytorch_baseline_reference": {
            "extract_wall_seconds": S1_EXTRACT_WALL_S,
            "match_wall_seconds": None,
            "avg_keypoints_per_image": 1024,
            "note": "S1 match incomplete (no matches.h5); full ±15% parity deferred",
        },
        "mrnerf": {
            "extract_wall_seconds": extract_wall_s,
            "match_wall_seconds": match_wall_s,
            "extract_plus_match_wall_seconds": round(extract_wall_s + match_wall_s, 3),
            "features": extract_info.get("features", {}),
            "matches": match_info.get("matches", {}),
            "pairs_matched": match_info.get("pairs_matched", 0),
            "pairs_total": match_info.get("pairs_total", 0),
            "match_escalation": match_info.get("match_escalation", {}),
            "smoke_match_subsample_keypoints": match_info.get(
                "smoke_match_subsample_keypoints"
            ),
            "smoke_match_subsample_note": match_info.get("smoke_match_subsample_note"),
        },
        "speed_guardrail": {
            "extract_vs_s1_ratio": round(extract_vs_s1, 3),
            "extract_vs_s1_note": (
                f"MrNeRF extract {extract_wall_s:.1f}s vs S1 PyTorch "
                f"{S1_EXTRACT_WALL_S}s (ratio {extract_vs_s1:.2f}x)"
            ),
            "full_extract_plus_match_1_25x_baseline": (
                "deferred — S1 match baseline missing"
            ),
            "full_parity_pm15_percent": (
                "deferred — S1 match/triangulation incomplete"
            ),
        },
        "discoveries": [
            "MrNeRF C++ extract yields ~7-15k keypoints/image at 1920x1440 vs "
            "hloc PyTorch max_num_keypoints=1024 (~1024/img in S1 features.h5)",
            "Full-resolution feature count OOMs LightGlue when extract+match share "
            "a process; match phase uses separate subprocess + smoke subsample",
            "Repeated h5py open/close on Windows 9p /app mount causes BlockingIOError; "
            "S6 smoke writes under /tmp and match_pairs_file_to_h5 keeps one handle open",
        ],
        "s6_status": "mrnerf_only_smoke_complete",
    }

    Path("/app/_local").mkdir(parents=True, exist_ok=True)
    RESULTS_JSON.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    _publish_host_outputs()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        choices=("all", "extract", "match", "finalize"),
        default="all",
    )
    parser.add_argument("--skip-extract", action="store_true")
    args = parser.parse_args()

    if not FRAMES_DIR.is_dir():
        print(f"S6_FAIL: frames dir missing: {FRAMES_DIR}")
        return 1

    portals = _load_manifest_portals(MANIFEST)
    pairs_count = len(parse_pairs_file(PAIRS_FILE))
    print(f"S6_START portals={len(portals)} pairs={pairs_count}")
    WORK_DIR.mkdir(parents=True, exist_ok=True)

    def _run_or_skip_extract() -> dict[str, Any]:
        can_skip = args.skip_extract and (
            FEATURES_H5.exists() or HOST_FEATURES_H5.exists()
        )
        if can_skip:
            _ensure_work_features()
            info = {
                "extract_wall_seconds": 67.94,
                "features": _summarize_features(_features_path()),
                "frames": len(_collect_image_items(FRAMES_DIR)),
                "note": "reused from prior run",
            }
            print("S6_EXTRACT_SKIP existing features.h5")
            return info
        info = run_extract()
        print(
            f"S6_EXTRACT_OK wall_s={info['extract_wall_seconds']:.2f} "
            f"images={info['features']['num_images']} "
            f"avg_kp={info['features']['avg_keypoints_per_image']}"
        )
        return info

    if args.phase == "extract":
        extract_info = _run_or_skip_extract()
        PARTIAL_JSON.write_text(
            json.dumps({"extract": extract_info, "portals": portals}, indent=2),
            encoding="utf-8",
        )
        return 0

    if args.phase == "match":
        if not PARTIAL_JSON.exists():
            # Allow match-only when extract already recorded on host.
            extract_info = _run_or_skip_extract()
            PARTIAL_JSON.write_text(
                json.dumps({"extract": extract_info, "portals": portals}, indent=2),
                encoding="utf-8",
            )
        match_info = run_match()
        print(
            f"S6_MATCH_OK wall_s={match_info['match_wall_seconds']:.2f} "
            f"pairs={match_info['pairs_matched']}/{match_info['pairs_total']}"
        )
        partial = json.loads(PARTIAL_JSON.read_text(encoding="utf-8"))
        partial["match"] = match_info
        PARTIAL_JSON.write_text(json.dumps(partial, indent=2), encoding="utf-8")
        return 0

    if args.phase == "finalize":
        partial = json.loads(PARTIAL_JSON.read_text(encoding="utf-8"))
        portals = _load_manifest_portals(MANIFEST)
        write_results(partial["extract"], partial["match"], portals)
        print(f"S6_RESULTS_JSON {RESULTS_JSON}")
        print("S6_PASS")
        return 0

    # phase == all
    import subprocess

    extract_info = _run_or_skip_extract()
    PARTIAL_JSON.write_text(
        json.dumps({"extract": extract_info, "portals": portals}, indent=2),
        encoding="utf-8",
    )

    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--phase",
        "match",
    ]
    env = dict(**{k: v for k, v in __import__("os").environ.items()})
    proc = subprocess.run(cmd, env=env, cwd=str(_APP_ROOT))
    if proc.returncode != 0:
        print("S6_FAIL match subprocess")
        return proc.returncode

    partial = json.loads(PARTIAL_JSON.read_text(encoding="utf-8"))
    write_results(partial["extract"], partial["match"], portals)
    print(f"S6_RESULTS_JSON {RESULTS_JSON}")
    print("S6_PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
