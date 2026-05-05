#!/usr/bin/env python3
"""Smoke: ALIKED + LightGlue extract+match on two frames from a DMT-style MP4.

Uses the same extractor/matcher keys as ``utils/triangulation.py`` (aliked-n16, aliked+lightglue).
Inference follows normal torch/hloc device selection (override with ``HLOC_DEVICE``).
First run may download weights (needs network).

From the reconstruction-server repository root:

  python tests/test_hloc_feature_match_dmt_smoke.py --mp4 /path/to/dmt_recording.mp4

  python tests/test_hloc_feature_match_dmt_smoke.py --mp4 ... --plots-dir /tmp/hloc_dmt_plots

Required: ``--mp4`` **or** env ``DMT_RECORDING_MP4`` pointing at an existing ``dmt_recording_*.mp4``.

Optional env:
  ``HLOC_DEVICE=cpu`` — force CPU.
  ``DMT_SMOKE_PLOT_DIR`` — same as ``--plots-dir`` if the flag is omitted (directory for match visualization PNG).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


def _resolve_mp4(args: argparse.Namespace) -> Path | None:
    if getattr(args, "mp4", None) is not None:
        return Path(args.mp4).expanduser().resolve()
    env = os.environ.get("DMT_RECORDING_MP4")
    if env:
        return Path(env).expanduser().resolve()
    return None


def _extract_two_jpegs(mp4: Path, out_dir: Path, verbose: bool) -> tuple[str, str]:
    import cv2

    cap = cv2.VideoCapture(str(mp4))
    if not cap.isOpened():
        raise RuntimeError(f"could not open video: {mp4}")
    if verbose:
        fps = cap.get(cv2.CAP_PROP_FPS)
        nframes = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        print(f"  video open: fps={fps:.2f} frame_count={nframes}")

    names: list[str] = []
    for i in range(2):
        ok, frame = cap.read()
        if not ok or frame is None:
            raise RuntimeError(f"failed to read frame {i} from {mp4}")
        h, w = frame.shape[:2]
        if verbose:
            print(f"  frame {i} raw size: {w}x{h}")
        if max(h, w) > 640:
            scale = 640.0 / max(h, w)
            frame = cv2.resize(
                frame,
                (int(w * scale), int(h * scale)),
                interpolation=cv2.INTER_AREA,
            )
            if verbose:
                nh, nw = frame.shape[:2]
                print(f"  frame {i} resized to {nw}x{nh} (scale={scale:.4f})")
        name = f"dmt_smoke_{i}.jpg"
        out_path = out_dir / name
        cv2.imwrite(str(out_path), frame, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
        names.append(name)
        if verbose:
            print(f"  wrote {out_path} ({out_path.stat().st_size} bytes)")
    cap.release()
    return names[0], names[1]


def _h5_file_size(path: Path) -> int:
    return path.stat().st_size if path.is_file() else 0


def _resolve_hloc_match_group(f: Any, n0: str, n1: str, names_to_pair: Any, names_to_pair_old: Any) -> tuple[Any, str]:
    """Return (h5 group, label) for the pair (n0, n1).

    hloc stores the pair name from ``names_to_pair`` (``a/b``). h5py's ``create_group``
    interprets ``/`` as nesting, so data often lives at ``f[n0][n1]``, which is still
    reachable as ``f[names_to_pair(n0, n1)]`` — but ``names_to_pair(n0,n1)`` will *not*
    appear in ``list(f.keys())`` (only the first path segment is a root key).
    """
    candidates = [
        names_to_pair(n0, n1),
        names_to_pair(n1, n0),
        names_to_pair_old(n0, n1),
        names_to_pair_old(n1, n0),
    ]
    for c in candidates:
        if c in f and "matches0" in f[c]:
            return f[c], c
    raise KeyError(
        "no matches0 group for known hloc pair paths "
        + ", ".join(repr(c) for c in candidates)
        + f"; root keys={list(f.keys())!r}"
    )


def _describe_features_h5(features_path: Path, names: tuple[str, ...], verbose: bool) -> None:
    if not verbose:
        return
    import h5py

    with h5py.File(features_path, "r") as f:
        print(f"  features.h5: {_h5_file_size(features_path)} bytes, top-level keys: {len(f.keys())}")
        for name in names:
            if name not in f:
                print(f"    missing group {name!r}")
                continue
            g = f[name]
            keys = list(g.keys())
            print(f"    [{name}] datasets: {keys}")
            if "keypoints" in g:
                kp = g["keypoints"]
                print(f"      keypoints shape={kp.shape} dtype={kp.dtype}")
            if "image_size" in g:
                print(f"      image_size={g['image_size'][()]}")
            if "descriptors" in g:
                d = g["descriptors"]
                print(f"      descriptors shape={d.shape} dtype={d.dtype}")


def _save_match_plot_cv2(
    out_png: Path,
    image_dir: Path,
    n0: str,
    n1: str,
    kpts0: Any,
    kpts1: Any,
    matches0: Any,
    max_lines: int = 400,
) -> None:
    import cv2
    import numpy as np

    img0 = cv2.imread(str(image_dir / n0), cv2.IMREAD_COLOR)
    img1 = cv2.imread(str(image_dir / n1), cv2.IMREAD_COLOR)
    if img0 is None or img1 is None:
        raise RuntimeError(f"could not read images from {image_dir}")

    k0 = np.asarray(kpts0, dtype=np.float32)
    k1 = np.asarray(kpts1, dtype=np.float32)
    m = np.asarray(matches0).reshape(-1).astype(np.int64)

    h0, w0 = img0.shape[:2]
    h1, w1 = img1.shape[:2]
    H = max(h0, h1)
    pad0 = np.zeros((H, w0, 3), dtype=np.uint8)
    pad0[:h0, :w0] = img0
    pad1 = np.zeros((H, w1, 3), dtype=np.uint8)
    pad1[:h1, :w1] = img1
    vis = np.hstack([pad0, pad1])

    valid_idx = [i for i in range(len(m)) if m[i] >= 0]
    if len(valid_idx) > max_lines:
        rng = np.random.default_rng(0)
        valid_idx = list(rng.choice(valid_idx, size=max_lines, replace=False))
        valid_idx.sort()

    green = (0, 220, 0)
    for i in valid_idx:
        j = int(m[i])
        if j < 0 or j >= len(k1):
            continue
        x0, y0 = int(round(float(k0[i, 0]))), int(round(float(k0[i, 1])))
        x1, y1 = int(round(float(k1[j, 0]))), int(round(float(k1[j, 1])))
        x1s = x1 + w0
        cv2.line(vis, (x0, y0), (x1s, y1), green, 1, cv2.LINE_AA)
        cv2.circle(vis, (x0, y0), 2, green, -1, cv2.LINE_AA)
        cv2.circle(vis, (x1s, y1), 2, green, -1, cv2.LINE_AA)

    max_w = 2200
    if vis.shape[1] > max_w:
        scale = max_w / vis.shape[1]
        vis = cv2.resize(
            vis,
            (int(vis.shape[1] * scale), int(vis.shape[0] * scale)),
            interpolation=cv2.INTER_AREA,
        )

    out_png.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(out_png), vis):
        raise RuntimeError(f"cv2.imwrite failed: {out_png}")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--mp4",
        type=Path,
        default=None,
        help="path to dmt_recording_*.mp4 (or set DMT_RECORDING_MP4)",
    )
    p.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="less console output (still prints OK/FAIL essentials)",
    )
    p.add_argument(
        "--plots-dir",
        type=Path,
        default=None,
        help="save match-line visualization PNG here (overrides DMT_SMOKE_PLOT_DIR)",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    verbose = not args.quiet
    plots_dir = args.plots_dir
    if plots_dir is None and os.environ.get("DMT_SMOKE_PLOT_DIR"):
        plots_dir = Path(os.environ["DMT_SMOKE_PLOT_DIR"]).expanduser().resolve()

    mp4 = _resolve_mp4(args)
    if mp4 is None:
        print(
            "FAIL: pass --mp4 /path/to/dmt_recording.mp4 or set DMT_RECORDING_MP4",
            file=sys.stderr,
        )
        return 2
    if not mp4.is_file():
        print("FAIL: DMT recording not found:", mp4, file=sys.stderr)
        return 1

    try:
        import h5py
        import numpy as np
        import torch
        from hloc import extract_features, match_features
        from hloc.utils.inference_device import select_inference_device
        from hloc.utils.parsers import names_to_pair, names_to_pair_old
    except ModuleNotFoundError as e:
        print("FAIL:", e, file=sys.stderr)
        return 1

    dev_name = select_inference_device()
    print("torch:", torch.__version__)
    print("inference device:", dev_name)
    print("video:", mp4)
    if verbose:
        print("work dir: tempfile prefix hloc_dmt_smoke_*")
        if plots_dir:
            print("plots dir:", plots_dir)

    with tempfile.TemporaryDirectory(prefix="hloc_dmt_smoke_") as td:
        root = Path(td)
        image_dir = root / "images"
        image_dir.mkdir(parents=True)
        if verbose:
            print("extracting two JPEGs from mp4 …")
        t0 = time.perf_counter()
        n0, n1 = _extract_two_jpegs(mp4, image_dir, verbose)
        t_decode = time.perf_counter() - t0
        if verbose:
            print(f"  decode+write wall time: {t_decode:.2f}s")

        pairs_path = root / "pairs-sfm.txt"
        pairs_path.write_text(f"{n0} {n1}\n", encoding="utf-8")
        if verbose:
            print("pairs file:", pairs_path.read_text().strip())

        sfm_dir = root / "sfm"
        sfm_dir.mkdir(parents=True)
        features_path = root / "sfm" / "features.h5"
        matches_path = root / "sfm" / "matches.h5"

        feature_conf = extract_features.confs["aliked-n16"]
        feature_conf["model"]["max_num_keypoints"] = 256
        feature_conf["model"]["detection_threshold"] = 0.25
        feature_conf["model"]["nms_radius"] = 4
        feature_conf["preprocessing"]["resize_max"] = 640
        feature_conf["output"] = "aliked-n16"
        if verbose:
            print("feature conf keys:", list(feature_conf.keys()))
            print("  model:", feature_conf.get("model"))
            print("  preprocessing:", feature_conf.get("preprocessing"))

        if verbose:
            print("running extract_features.main …")
        t1 = time.perf_counter()
        extract_features.main(
            feature_conf,
            image_dir,
            sfm_dir,
            feature_path=features_path,
            as_half=True,
            image_list=[n0, n1],
        )
        t_extract = time.perf_counter() - t1
        print(f"extract done in {t_extract:.2f}s")
        _describe_features_h5(features_path, (n0, n1), verbose)

        matcher_conf = match_features.confs["aliked+lightglue"]
        matcher_conf["model"]["compile_network"] = False
        if verbose:
            print("matcher model name:", matcher_conf["model"].get("name"))
            print("running match_features.main …")
        t2 = time.perf_counter()
        match_features.main(
            matcher_conf,
            pairs_path,
            features=features_path,
            matches=matches_path,
        )
        t_match = time.perf_counter() - t2
        print(f"match done in {t_match:.2f}s")
        print(f"matches.h5 size: {_h5_file_size(matches_path)} bytes")

        with h5py.File(matches_path, "r") as f:
            if not len(f.keys()):
                print("FAIL: empty matches h5", file=sys.stderr)
                return 1
            root_keys = list(f.keys())
            if verbose:
                print("match h5 root keys (first 5):", root_keys[:5], "…" if len(root_keys) > 5 else "")
                pk = names_to_pair(n0, n1)
                if pk not in root_keys and pk in f:
                    print(
                        "  note: pair path",
                        repr(pk),
                        "is nested under h5py (slash in group name); using path access, not root key list.",
                    )
            grp, pair_key = _resolve_hloc_match_group(f, n0, n1, names_to_pair, names_to_pair_old)
            m0 = grp["matches0"][()]
            n_valid = int(np.sum(np.asarray(m0) >= 0))
            print(f"pair {pair_key!r}: matches0 len={len(m0)} valid={n_valid}")
            if verbose and "matching_scores0" in grp:
                sc = grp["matching_scores0"][()]
                valid_mask = m0 >= 0
                if valid_mask.any():
                    sv = sc[valid_mask].astype("float64")
                    print(f"  matching_scores0: min={sv.min():.4f} max={sv.max():.4f} mean={sv.mean():.4f}")

        if plots_dir is not None:
            with h5py.File(features_path, "r") as feat_f, h5py.File(matches_path, "r") as mat_f:
                k0 = feat_f[n0]["keypoints"][()]
                k1 = feat_f[n1]["keypoints"][()]
                _g, _pk = _resolve_hloc_match_group(mat_f, n0, n1, names_to_pair, names_to_pair_old)
                m0 = _g["matches0"][()]
            out_png = plots_dir / "dmt_smoke_aliked_lightglue_matches.png"
            if verbose:
                print(f"writing match visualization → {out_png}")
            _save_match_plot_cv2(out_png, image_dir, n0, n1, k0, k1, m0)
            print("saved plot:", out_png.resolve())

        print(
            "OK:",
            f"extract {t_extract:.2f}s + match {t_match:.2f}s;",
            f"{n_valid} valid correspondences on {pair_key!r}.",
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print("FAIL:", e, file=sys.stderr)
        raise SystemExit(1) from e
