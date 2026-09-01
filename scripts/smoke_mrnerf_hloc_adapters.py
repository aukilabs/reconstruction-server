#!/usr/bin/env python3
"""S4 smoke: MrNeRF hloc-compatible HDF5 adapters.

Run inside the app container (GPU preferred):
  docker exec -e LD_LIBRARY_PATH=/opt/libtorch/lib s3-mrnerf-build \\
      python3 /app/scripts/smoke_mrnerf_hloc_adapters.py
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import cv2
import numpy as np

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))

from utils.mrnerf_hloc_adapters import (  # noqa: E402
    MrNerfHlocExtractor,
    MrNerfHlocMatcher,
    validate_features_h5,
    validate_matches_h5,
)


def _write_synthetic_images(tmpdir: Path) -> tuple[Path, Path, str, str]:
    rng = np.random.default_rng(0)
    img0 = rng.integers(0, 256, size=(480, 640, 3), dtype=np.uint8)
    img1 = rng.integers(0, 256, size=(480, 640, 3), dtype=np.uint8)
    name0, name1 = "0000.jpg", "0001.jpg"
    path0 = tmpdir / name0
    path1 = tmpdir / name1
    cv2.imwrite(str(path0), cv2.cvtColor(img0, cv2.COLOR_RGB2BGR))
    cv2.imwrite(str(path1), cv2.cvtColor(img1, cv2.COLOR_RGB2BGR))
    return path0, path1, name0, name1


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="mrnerf_hloc_smoke_") as tmp:
        tmpdir = Path(tmp)
        features_path = tmpdir / "features.h5"
        matches_path = tmpdir / "matches.h5"

        path0, path1, name0, name1 = _write_synthetic_images(tmpdir)

        extractor = MrNerfHlocExtractor()
        matcher = MrNerfHlocMatcher()

        feats = extractor.extract_many_to_h5(
            [(path0, name0), (path1, name1)],
            features_path,
            as_half=True,
        )
        print(
            f"extract_ok images=2 "
            f"keypoints0={feats[0]['keypoints'].shape[0]} "
            f"keypoints1={feats[1]['keypoints'].shape[0]}"
        )

        _, pair = matcher.match_to_h5(
            feats[0], feats[1], name0, name1, matches_path
        )
        match_summary = validate_matches_h5(matches_path, name0, name1)
        print(
            f"match_ok pair={pair} "
            f"valid_matches={match_summary['valid_match_count']} "
            f"matches0_shape={match_summary['matches0_shape']}"
        )

        feat_summary = validate_features_h5(features_path, [name0, name1])
        for name, info in feat_summary.items():
            print(
                f"features_h5_ok name={name} "
                f"kp={info['num_keypoints']} "
                f"desc_dim={info['descriptor_dim']} "
                f"datasets={info['datasets']}"
            )

        required_feature_datasets = {"keypoints", "descriptors", "scores", "image_size"}
        for name, info in feat_summary.items():
            missing = required_feature_datasets - set(info["datasets"])
            if missing:
                print(f"SMOKE_FAIL: {name} missing datasets {sorted(missing)}")
                return 1
            if info["keypoints_shape"][1] != 2:
                print(f"SMOKE_FAIL: bad keypoints shape for {name}")
                return 1

        if match_summary["matches0_dtype"] != "int16":
            print(f"SMOKE_FAIL: matches0 dtype {match_summary['matches0_dtype']}")
            return 1
        if match_summary["scores0_dtype"] != "float16":
            print(f"SMOKE_FAIL: scores0 dtype {match_summary['scores0_dtype']}")
            return 1
        if match_summary["valid_match_count"] < 1:
            print("SMOKE_FAIL: expected at least one valid match")
            return 1

    print("SMOKE_PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
