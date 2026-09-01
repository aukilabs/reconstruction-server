"""hloc-compatible HDF5 adapters for MrNeRF C++ feature extract/match.

Writes the same features.h5 / matches.h5 layout consumed by
``hloc.triangulation.import_features`` and ``import_matches``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

import h5py
import numpy as np
from hloc.utils.io import get_keypoints, get_matches
from hloc.utils.parsers import names_to_pair, names_to_pair_old, parse_retrieval

FeatureDict = Dict[str, np.ndarray]
MatchDict = Dict[str, np.ndarray]
PairNames = Tuple[str, str]


def _import_mrnerf_features():
    import src.mrnerf_features as mf

    return mf


def _default_device() -> str:
    mf = _import_mrnerf_features()
    return "cuda" if mf.cuda_available() else "cpu"


def _load_features_from_group(fd: h5py.File, image_name: str) -> FeatureDict:
    """Load one image group from an already-open features.h5 handle."""
    if image_name not in fd:
        raise KeyError(f"{image_name} missing from features file")
    grp = fd[image_name]
    feats: FeatureDict = {}
    for key in grp.keys():
        arr = np.asarray(grp[key])
        if arr.dtype == np.float16:
            arr = arr.astype(np.float32)
        feats[key] = arr
    return feats


def load_features_from_h5(
    feature_path: Union[Path, str],
    image_name: str,
) -> FeatureDict:
    """Load one image's feature group from hloc-style features.h5."""
    with h5py.File(str(feature_path), "r", libver="latest") as fd:
        return _load_features_from_group(fd, image_name)


def _find_unique_new_pairs(
    pairs_all: Sequence[PairNames],
    match_path: Optional[Path] = None,
) -> List[PairNames]:
    """Mirror hloc.match_features.find_unique_new_pairs."""
    pairs: set[PairNames] = set()
    for name0, name1 in pairs_all:
        if (name1, name0) not in pairs:
            pairs.add((name0, name1))
    unique_pairs = list(pairs)
    if match_path is not None and match_path.exists():
        filtered: List[PairNames] = []
        with h5py.File(str(match_path), "r", libver="latest") as fd:
            for name0, name1 in unique_pairs:
                if (
                    names_to_pair(name0, name1) in fd
                    or names_to_pair(name1, name0) in fd
                    or names_to_pair_old(name0, name1) in fd
                    or names_to_pair_old(name1, name0) in fd
                ):
                    continue
                filtered.append((name0, name1))
        return filtered
    return unique_pairs


def parse_pairs_file(pairs_path: Union[Path, str]) -> List[PairNames]:
    """Flatten hloc pairs file (one query/ref per line) to (name0, name1) list."""
    pairs_dict = parse_retrieval(Path(pairs_path))
    return [(query, ref) for query, refs in pairs_dict.items() for ref in refs]


def feats_to_hloc_arrays(
    feats: FeatureDict,
    *,
    as_half: bool = True,
    keypoint_uncertainty: float = 1.0,
) -> Tuple[FeatureDict, float]:
    """Normalize pybind feature dict to hloc HDF5 dataset arrays."""
    out: FeatureDict = {}
    for key, value in feats.items():
        arr = np.asarray(value)
        if key == "image_size":
            arr = np.asarray(arr, dtype=np.float32).reshape(-1)
            if arr.shape != (2,):
                raise ValueError(f"image_size must have 2 elements, got shape {arr.shape}")
        out[key] = arr

    if as_half:
        for key, value in out.items():
            if value.dtype == np.float32:
                out[key] = value.astype(np.float16)

    uncertainty = keypoint_uncertainty
    if "keypoints" not in out:
        raise ValueError("features dict missing keypoints")

    return out, uncertainty


def _write_features_group(
    fd: h5py.File,
    image_name: str,
    feats: FeatureDict,
    *,
    as_half: bool = True,
    overwrite: bool = True,
    keypoint_uncertainty: float = 1.0,
) -> None:
    """Write one image group into an already-open features.h5 handle."""
    pred, uncertainty = feats_to_hloc_arrays(
        feats, as_half=as_half, keypoint_uncertainty=keypoint_uncertainty
    )
    if image_name in fd:
        if not overwrite:
            return
        del fd[image_name]
    grp = fd.create_group(image_name)
    for key, value in pred.items():
        grp.create_dataset(key, data=value)
    grp["keypoints"].attrs["uncertainty"] = uncertainty


def write_features_h5(
    feature_path: Union[Path, str],
    image_name: str,
    feats: FeatureDict,
    *,
    as_half: bool = True,
    overwrite: bool = True,
    keypoint_uncertainty: float = 1.0,
    fd: Optional[h5py.File] = None,
) -> None:
    """Append one image group to an hloc-style features.h5 file."""
    if fd is not None:
        _write_features_group(
            fd,
            image_name,
            feats,
            as_half=as_half,
            overwrite=overwrite,
            keypoint_uncertainty=keypoint_uncertainty,
        )
        return

    feature_path = Path(feature_path)
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(str(feature_path), "a", libver="latest") as opened:
        _write_features_group(
            opened,
            image_name,
            feats,
            as_half=as_half,
            overwrite=overwrite,
            keypoint_uncertainty=keypoint_uncertainty,
        )


def _write_matches_group(
    fd: h5py.File,
    name0: str,
    name1: str,
    match_pred: MatchDict,
    *,
    overwrite: bool = True,
) -> str:
    """Write one pair group into an already-open matches.h5 handle."""
    pair = names_to_pair(name0, name1)

    matches0 = np.asarray(match_pred["matches0"])
    if matches0.ndim == 2:
        matches0 = matches0[0]
    matches0 = matches0.astype(np.int16)

    if "matching_scores0" not in match_pred:
        raise ValueError("match_pred missing matching_scores0")
    scores0 = np.asarray(match_pred["matching_scores0"])
    if scores0.ndim == 2:
        scores0 = scores0[0]
    scores0 = scores0.astype(np.float16)

    if pair in fd:
        if not overwrite:
            return pair
        del fd[pair]
    grp = fd.create_group(pair)
    grp.create_dataset("matches0", data=matches0)
    grp.create_dataset("matching_scores0", data=scores0)
    return pair


def write_matches_h5(
    match_path: Union[Path, str],
    name0: str,
    name1: str,
    match_pred: MatchDict,
    *,
    overwrite: bool = True,
) -> str:
    """Append one pair group to an hloc-style matches.h5 file."""
    match_path = Path(match_path)
    match_path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(str(match_path), "a", libver="latest") as fd:
        return _write_matches_group(
            fd, name0, name1, match_pred, overwrite=overwrite
        )


class MrNerfHlocExtractor:
    """MrNeRF ALIKED extractor that writes hloc-compatible features.h5."""

    def __init__(
        self,
        model_name: str = "aliked-n16",
        device: Optional[str] = None,
        *,
        top_k: int = 1024,
        scores_th: float = 0.3,
        nms_radius: int = 4,
        n_limit: int = 20000,
        resize_max: int = 1024,
    ) -> None:
        mf = _import_mrnerf_features()
        if device is None:
            device = _default_device()
        # top_k mirrors hloc aliked-n16 max_num_keypoints=1024 (avoids LightGlue OOM).
        self._extractor = mf.AlikedExtractor(
            model_name,
            device,
            top_k,
            scores_th,
            nms_radius,
            n_limit,
            resize_max,
        )

    def extract_from_path(self, image_path: Union[Path, str]) -> FeatureDict:
        return self._extractor.extract_from_path(str(image_path))

    def extract_from_numpy(self, image: np.ndarray) -> FeatureDict:
        return self._extractor.extract_from_numpy(image)

    def extract_to_h5(
        self,
        image_path: Union[Path, str],
        image_name: str,
        feature_path: Union[Path, str],
        *,
        as_half: bool = True,
        overwrite: bool = True,
    ) -> FeatureDict:
        feats = self.extract_from_path(image_path)
        write_features_h5(
            feature_path,
            image_name,
            feats,
            as_half=as_half,
            overwrite=overwrite,
        )
        return feats

    def extract_many_to_h5(
        self,
        items: Sequence[Tuple[Union[Path, str], str]],
        feature_path: Union[Path, str],
        *,
        as_half: bool = True,
        overwrite: bool = True,
    ) -> List[FeatureDict]:
        # Keep one write handle open — repeated open/close on Windows 9p hits locks.
        feature_path = Path(feature_path)
        feature_path.parent.mkdir(parents=True, exist_ok=True)
        all_feats: List[FeatureDict] = []
        with h5py.File(str(feature_path), "a", libver="latest") as fd:
            for image_path, image_name in items:
                feats = self.extract_from_path(image_path)
                write_features_h5(
                    feature_path,
                    image_name,
                    feats,
                    as_half=as_half,
                    overwrite=overwrite,
                    fd=fd,
                )
                all_feats.append(feats)
        return all_feats


class MrNerfHlocMatcher:
    """MrNeRF LightGlue matcher that writes hloc-compatible matches.h5."""

    def __init__(self, device: Optional[str] = None) -> None:
        mf = _import_mrnerf_features()
        if device is None:
            device = _default_device()
        self._matcher = mf.LightGlueMatcher(device)

    def match(self, feats0: FeatureDict, feats1: FeatureDict) -> MatchDict:
        return self._matcher.match(feats0, feats1)

    def match_to_h5(
        self,
        feats0: FeatureDict,
        feats1: FeatureDict,
        name0: str,
        name1: str,
        match_path: Union[Path, str],
        *,
        overwrite: bool = True,
    ) -> Tuple[MatchDict, str]:
        pred = self.match(feats0, feats1)
        pair = write_matches_h5(
            match_path, name0, name1, pred, overwrite=overwrite
        )
        return pred, pair

    def match_pairs_file_to_h5(
        self,
        pairs_path: Union[Path, str],
        feature_path: Union[Path, str],
        match_path: Union[Path, str],
        *,
        feature_path_ref: Optional[Union[Path, str]] = None,
        overwrite: bool = False,
    ) -> int:
        """Match all pairs listed in an hloc pairs file into matches.h5."""
        pairs_path = Path(pairs_path)
        feature_path = Path(feature_path)
        feature_path_ref = (
            Path(feature_path_ref) if feature_path_ref is not None else feature_path
        )
        match_path = Path(match_path)

        if not feature_path.exists():
            raise FileNotFoundError(f"Feature file not found: {feature_path}")
        if not feature_path_ref.exists():
            raise FileNotFoundError(f"Reference feature file not found: {feature_path_ref}")
        if not pairs_path.exists():
            raise FileNotFoundError(f"Pairs file not found: {pairs_path}")

        pairs = _find_unique_new_pairs(
            parse_pairs_file(pairs_path),
            None if overwrite else match_path,
        )
        if not pairs:
            return 0

        # Keep features + matches handles open for the whole batch. Re-opening
        # per pair on Windows 9p Docker mounts hits h5py BlockingIOError locks.
        match_path.parent.mkdir(parents=True, exist_ok=True)
        same_features = feature_path.resolve() == feature_path_ref.resolve()
        if same_features:
            with h5py.File(str(feature_path), "r", libver="latest") as feats_fd, h5py.File(
                str(match_path), "a", libver="latest"
            ) as match_fd:
                for name0, name1 in pairs:
                    feats0 = _load_features_from_group(feats_fd, name0)
                    feats1 = _load_features_from_group(feats_fd, name1)
                    pred = self.match(feats0, feats1)
                    _write_matches_group(
                        match_fd, name0, name1, pred, overwrite=overwrite
                    )
        else:
            with h5py.File(str(feature_path), "r", libver="latest") as feats0_fd, h5py.File(
                str(feature_path_ref), "r", libver="latest"
            ) as feats1_fd, h5py.File(str(match_path), "a", libver="latest") as match_fd:
                for name0, name1 in pairs:
                    feats0 = _load_features_from_group(feats0_fd, name0)
                    feats1 = _load_features_from_group(feats1_fd, name1)
                    pred = self.match(feats0, feats1)
                    _write_matches_group(
                        match_fd, name0, name1, pred, overwrite=overwrite
                    )
        return len(pairs)


def validate_features_h5(
    feature_path: Union[Path, str],
    image_names: Iterable[str],
) -> Dict[str, Dict[str, object]]:
    """Sanity-check features.h5 using hloc readers."""
    feature_path = Path(feature_path)
    summary: Dict[str, Dict[str, object]] = {}
    with h5py.File(str(feature_path), "r", libver="latest") as fd:
        for name in image_names:
            if name not in fd:
                raise KeyError(f"{name} missing from {feature_path}")
            grp = fd[name]
            keypoints = get_keypoints(feature_path, name)
            summary[name] = {
                "keypoints_shape": keypoints.shape,
                "datasets": sorted(grp.keys()),
                "image_size": np.asarray(grp["image_size"]).tolist(),
                "descriptor_dim": int(grp["descriptors"].shape[1]),
                "num_keypoints": int(grp["keypoints"].shape[0]),
            }
    return summary


def validate_matches_h5(
    match_path: Union[Path, str],
    name0: str,
    name1: str,
) -> Dict[str, object]:
    """Sanity-check matches.h5 using hloc readers."""
    match_path = Path(match_path)
    matches, scores = get_matches(match_path, name0, name1)
    with h5py.File(str(match_path), "r", libver="latest") as fd:
        pair, _reverse = _find_pair_in_file(fd, name0, name1)
        grp = fd[pair]
        raw_matches0 = np.asarray(grp["matches0"])
        raw_scores0 = np.asarray(grp["matching_scores0"])
    return {
        "pair": pair,
        "matches0_shape": raw_matches0.shape,
        "matches0_dtype": str(raw_matches0.dtype),
        "scores0_dtype": str(raw_scores0.dtype),
        "valid_match_count": int(matches.shape[0]),
        "scores_shape": scores.shape,
    }


def _find_pair_in_file(fd: h5py.File, name0: str, name1: str) -> Tuple[str, bool]:
    pair = names_to_pair(name0, name1)
    if pair in fd:
        return pair, False
    pair_rev = names_to_pair(name1, name0)
    if pair_rev in fd:
        return pair_rev, True
    raise KeyError(f"pair ({name0}, {name1}) not found in matches file")


def run_extract_and_match_subprocess(
    *,
    images_dir: Union[Path, str],
    image_names: Sequence[str],
    features_h5: Union[Path, str],
    pairs_file: Union[Path, str],
    matches_h5: Union[Path, str],
    model_name: str = "aliked-n16",
    top_k: int = 1024,
    scores_th: float = 0.3,
    nms_radius: int = 4,
    resize_max: int = 1024,
    libtorch_lib: str = "/opt/libtorch/lib",
    logger: Optional[object] = None,
) -> None:
    """Run MrNeRF extract+match in a child process (LibTorch 2.7 isolation).

    Parent may already have imported Python torch (EigenPlaces / hloc). Mixing
    that with ``src.mrnerf_features`` (LibTorch 2.7) in-process fails on libc10
    ABI. The worker script must not import torch/hloc.
    """
    import json
    import logging
    import os
    import subprocess
    import sys
    import tempfile

    log = logger if logger is not None else logging.getLogger(__name__)
    images_dir = Path(images_dir)
    features_h5 = Path(features_h5)
    pairs_file = Path(pairs_file)
    matches_h5 = Path(matches_h5)
    worker = Path(__file__).resolve().parents[1] / "scripts" / "mrnerf_extract_match_worker.py"
    if not worker.is_file():
        raise FileNotFoundError(f"MrNeRF worker script missing: {worker}")

    features_h5.parent.mkdir(parents=True, exist_ok=True)
    matches_h5.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False, encoding="utf-8"
    ) as tmp:
        json.dump(list(image_names), tmp)
        names_path = Path(tmp.name)

    env = os.environ.copy()
    # Prefer C++ LibTorch for the child only; keep system paths so OpenCV resolves.
    prev = env.get("LD_LIBRARY_PATH", "")
    extras = [p for p in prev.split(":") if p and p != libtorch_lib]
    env["LD_LIBRARY_PATH"] = ":".join(
        [
            libtorch_lib,
            "/usr/local/cuda-12.8/lib64",
            "/usr/local/cuda/lib64",
            "/usr/lib/x86_64-linux-gnu",
            "/lib/x86_64-linux-gnu",
        ]
        + extras
    )
    env.setdefault("PYTHONUNBUFFERED", "1")

    cmd = [
        sys.executable,
        str(worker),
        "--images-dir",
        str(images_dir),
        "--names-json",
        str(names_path),
        "--features-h5",
        str(features_h5),
        "--pairs-file",
        str(pairs_file),
        "--matches-h5",
        str(matches_h5),
        "--model-name",
        model_name,
        "--top-k",
        str(top_k),
        "--scores-th",
        str(scores_th),
        "--nms-radius",
        str(nms_radius),
        "--resize-max",
        str(resize_max),
    ]
    log.info("MrNeRF worker: %s", " ".join(cmd))
    try:
        proc = subprocess.run(
            cmd,
            env=env,
            cwd=str(Path(__file__).resolve().parents[1]),
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        try:
            names_path.unlink(missing_ok=True)
        except OSError:
            pass

    if proc.stdout:
        for line in proc.stdout.strip().splitlines():
            log.info("[mrnerf] %s", line)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(
            f"MrNeRF extract/match worker failed (exit {proc.returncode}): {err[-2000:]}"
        )
