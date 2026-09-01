#!/usr/bin/env python3
"""Subprocess worker: MrNeRF extract+match without importing Python torch.

Must run with LD_LIBRARY_PATH including /opt/libtorch/lib so the C++ extension
resolves LibTorch 2.7. Do not import torch/hloc here — they pull site-packages
torch 2.9 and collide on libc10 symbols in-process.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parents[1]
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--images-dir", type=Path, required=True)
    parser.add_argument("--names-json", type=Path, required=True)
    parser.add_argument("--features-h5", type=Path, required=True)
    parser.add_argument("--pairs-file", type=Path, required=True)
    parser.add_argument("--matches-h5", type=Path, required=True)
    parser.add_argument("--model-name", default="aliked-n16")
    parser.add_argument("--top-k", type=int, default=1024)
    parser.add_argument("--scores-th", type=float, default=0.3)
    parser.add_argument("--nms-radius", type=int, default=4)
    parser.add_argument("--resize-max", type=int, default=1024)
    args = parser.parse_args()

    from utils.mrnerf_hloc_adapters import MrNerfHlocExtractor, MrNerfHlocMatcher

    names: list[str] = json.loads(args.names_json.read_text(encoding="utf-8"))
    items = [(args.images_dir / name, name) for name in names]

    extractor = MrNerfHlocExtractor(
        model_name=args.model_name,
        top_k=args.top_k,
        scores_th=args.scores_th,
        nms_radius=args.nms_radius,
        resize_max=args.resize_max,
    )
    extractor.extract_many_to_h5(items, args.features_h5, as_half=True, overwrite=True)
    del extractor

    matcher = MrNerfHlocMatcher()
    n = matcher.match_pairs_file_to_h5(
        args.pairs_file,
        args.features_h5,
        args.matches_h5,
        overwrite=True,
    )
    print(f"MRNERF_WORKER_OK images={len(names)} pairs_matched={n}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
