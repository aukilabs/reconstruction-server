#!/usr/bin/env python3
"""S3 smoke: import mrnerf_features, load weights, extract + match one pair.

Run inside the app container (GPU optional but preferred):
  docker exec -e LD_LIBRARY_PATH=/opt/libtorch/lib s3-mrnerf-build \\
      python3 /app/scripts/smoke_mrnerf_features.py
"""

import os
import sys

# Allow `import src.mrnerf_features` when /app is not already on PYTHONPATH.
_APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _APP_ROOT not in sys.path:
    sys.path.insert(0, _APP_ROOT)

import numpy as np


def main() -> int:
    try:
        import src.mrnerf_features as mf
    except ImportError as exc:
        print(f"IMPORT_FAIL: {exc}")
        return 1

    print(f"cuda_available={mf.cuda_available()}")

    device = "cuda" if mf.cuda_available() else "cpu"
    extractor = mf.AlikedExtractor("aliked-n16", device)
    matcher = mf.LightGlueMatcher(device)

    rng = np.random.default_rng(0)
    img0 = rng.integers(0, 256, size=(480, 640, 3), dtype=np.uint8)
    img1 = rng.integers(0, 256, size=(480, 640, 3), dtype=np.uint8)

    feats0 = extractor.extract_from_numpy(img0)
    feats1 = extractor.extract_from_numpy(img1)

    n0 = feats0["keypoints"].shape[0]
    n1 = feats1["keypoints"].shape[0]
    desc_dim = feats0["descriptors"].shape[1]
    print(f"extract_ok keypoints0={n0} keypoints1={n1} desc_dim={desc_dim}")

    match_out = matcher.match(feats0, feats1)
    matches0 = match_out["matches0"]
    valid = int(np.sum(matches0 >= 0))
    print(f"match_ok matches0_shape={matches0.shape} valid_matches={valid}")

    if n0 < 1 or n1 < 1:
        print("SMOKE_FAIL: expected keypoints from synthetic images")
        return 1

    print("SMOKE_PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
