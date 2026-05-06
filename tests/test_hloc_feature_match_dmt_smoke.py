#!/usr/bin/env python3
"""
Smoke test for the feature extraction and matching, on a small set of images.
Uses ALIKED + LightGlue in the same way as in triangulation.py.
The test frames are committed to the repo under tests/data/test_frames/frame_*.jpg.

Note: The test may download model weights, which needs internet access.

Usage:
    python tests/test_hloc_feature_match_dmt_smoke.py --frames-dir tests/data/test_frames

Optional: ``--plots-dir`` or env ``HLOC_SMOKE_PLOT_DIR`` — one match-line PNG per consecutive image pair
(``frame_00.jpg`` / ``frame_01.jpg``, …).
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}


def _list_frame_filenames(frames_dir: Path) -> list[str]:
    """Sorted basenames of image files directly under ``frames_dir``."""
    if not frames_dir.is_dir():
        raise FileNotFoundError(f"not a directory: {frames_dir}")
    names: list[str] = []
    for p in frames_dir.iterdir():
        if p.is_file() and p.suffix.lower() in _IMAGE_SUFFIXES:
            names.append(p.name)
    names.sort()
    return names


def _consecutive_pairs(names: list[str]) -> list[tuple[str, str]]:
    return list(zip(names, names[1:]))


def _list_frames_and_pairs(frames_dir: Path, verbose: bool) -> tuple[list[str], list[tuple[str, str]]]:
    names = _list_frame_filenames(frames_dir)
    if len(names) < 2:
        raise RuntimeError(
            f"need at least 2 images in {frames_dir}, found {len(names)}: {names!r}"
        )
    pairs = _consecutive_pairs(names)
    if verbose:
        print(f"  frames ({len(names)} in dir): {names!r}")
        print(f"  consecutive pairs ({len(pairs)}): {pairs!r}")
    return names, pairs


def _pair_plot_path(plots_dir: Path, pair_key: str) -> Path:
    """Filesystem-safe name from hloc pair key (may contain ``/``)."""
    safe = pair_key.replace("/", "__").replace("\\", "_")
    return plots_dir / f"hloc_smoke_matches__{safe}.png"


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
        "--frames-dir",
        type=Path,
        required=True,
        help="directory with image files (JPEG/PNG/…); all are used, matched as consecutive pairs after sort",
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
        help="save match-line visualization PNG here (overrides HLOC_SMOKE_PLOT_DIR)",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    verbose = not args.quiet
    plots_dir = args.plots_dir
    if plots_dir is None and os.environ.get("HLOC_SMOKE_PLOT_DIR"):
        plots_dir = Path(os.environ["HLOC_SMOKE_PLOT_DIR"]).expanduser().resolve()

    frames_dir = args.frames_dir.expanduser().resolve()
    if not frames_dir.is_dir():
        print("FAIL: frames directory not found:", frames_dir, file=sys.stderr)
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
    print("frames dir:", frames_dir)
    if verbose:
        print("work dir: tempfile prefix hloc_feature_match_smoke_*")
        if plots_dir:
            print("plots dir:", plots_dir)

    with tempfile.TemporaryDirectory(prefix="hloc_feature_match_smoke_") as td:
        root = Path(td)
        image_dir = frames_dir
        if verbose:
            print("listing frames and consecutive pairs …")
        t0 = time.perf_counter()
        try:
            image_names, pairs_list = _list_frames_and_pairs(image_dir, verbose)
        except (FileNotFoundError, RuntimeError) as e:
            print("FAIL:", e, file=sys.stderr)
            return 1
        t_pick = time.perf_counter() - t0
        if verbose:
            print(f"  listing wall time: {t_pick:.3f}s")

        pairs_path = root / "pairs-sfm.txt"
        pairs_path.write_text("".join(f"{a} {b}\n" for a, b in pairs_list), encoding="utf-8")
        if verbose:
            print("pairs file:\n" + pairs_path.read_text().rstrip())

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
            image_list=image_names,
        )
        t_extract = time.perf_counter() - t1
        print(f"extract done in {t_extract:.2f}s")
        _describe_features_h5(features_path, tuple(image_names), verbose)

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

        per_pair_valid: list[tuple[str, int]] = []
        saved_plot_paths: list[Path] = []

        with h5py.File(matches_path, "r") as mat_f, h5py.File(features_path, "r") as feat_f:
            if not len(mat_f.keys()):
                print("FAIL: empty matches h5", file=sys.stderr)
                return 1
            root_keys = list(mat_f.keys())
            if verbose:
                print("match h5 root keys (first 8):", root_keys[:8], "…" if len(root_keys) > 8 else "")
                n0a, n1a = pairs_list[0]
                pk = names_to_pair(n0a, n1a)
                if pk not in root_keys and pk in mat_f:
                    print(
                        "  note: pair path",
                        repr(pk),
                        "is nested under h5py (slash in group name); using path access, not root key list.",
                    )

            for n0, n1 in pairs_list:
                grp, pair_key = _resolve_hloc_match_group(
                    mat_f, n0, n1, names_to_pair, names_to_pair_old
                )
                m0 = grp["matches0"][()]
                n_valid = int(np.sum(np.asarray(m0) >= 0))
                per_pair_valid.append((pair_key, n_valid))
                if verbose:
                    print(f"pair {pair_key!r}: matches0 len={len(m0)} valid={n_valid}")
                if verbose and "matching_scores0" in grp:
                    sc = grp["matching_scores0"][()]
                    valid_mask = m0 >= 0
                    if valid_mask.any():
                        sv = sc[valid_mask].astype("float64")
                        print(
                            f"  matching_scores0: min={sv.min():.4f} max={sv.max():.4f} mean={sv.mean():.4f}"
                        )

                if plots_dir is not None:
                    k0 = feat_f[n0]["keypoints"][()]
                    k1 = feat_f[n1]["keypoints"][()]
                    out_png = _pair_plot_path(plots_dir, pair_key)
                    if verbose:
                        print(f"writing match visualization → {out_png}")
                    _save_match_plot_cv2(out_png, image_dir, n0, n1, k0, k1, m0)
                    saved_plot_paths.append(out_png.resolve())

        if plots_dir is not None and saved_plot_paths:
            if verbose:
                for p in saved_plot_paths:
                    print("saved plot:", p)
            else:
                print(f"saved {len(saved_plot_paths)} plot(s) under {plots_dir.resolve()}")

        total_valid = sum(v for _pk, v in per_pair_valid)
        print(
            "OK:",
            f"extract {t_extract:.2f}s + match {t_match:.2f}s;",
            f"{len(pairs_list)} pair(s), {total_valid} valid correspondences total.",
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print("FAIL:", e, file=sys.stderr)
        raise SystemExit(1) from e
