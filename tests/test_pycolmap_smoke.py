#!/usr/bin/env python3
"""Smoke test for pycolmap basic functionality, using synthetic data."""

from __future__ import annotations

import sys

import numpy as np


def _synthetic_correspondences() -> tuple[np.ndarray, np.ndarray]:
    """Pinhole camera at origin, identity cam_from_world; points on plane z=2."""
    fx, cx, cy = 500.0, 320.0, 240.0
    pts3d = np.array(
        [
            [0.0, 0.0, 2.0],
            [1.0, 0.0, 2.0],
            [0.0, 1.0, 2.0],
            [1.0, 1.0, 2.0],
        ],
        dtype=np.float64,
    )
    pts2d = np.empty((4, 2), dtype=np.float64)
    for i, p in enumerate(pts3d):
        pts2d[i, 0] = fx * p[0] / p[2] + cx
        pts2d[i, 1] = fx * p[1] / p[2] + cy
    return pts2d, pts3d


def main() -> int:
    import pycolmap

    print("pycolmap:", getattr(pycolmap, "__file__", "?"))
    print("version:", getattr(pycolmap, "__version__", "?"))

    pts2d, pts3d = _synthetic_correspondences()
    camera = pycolmap.Camera(
        model="SIMPLE_PINHOLE",
        width=640,
        height=480,
        params=np.array([500.0, 320.0, 240.0], dtype=np.float64),
        camera_id=0,
    )

    out = pycolmap.estimate_and_refine_absolute_pose(
        pts2d,
        pts3d,
        camera,
        estimation_options={"ransac": {"max_error": 1.0}},
        refinement_options={
            "refine_focal_length": False,
            "refine_extra_params": False,
        },
    )
    if out is None:
        print("FAIL: estimate_and_refine_absolute_pose returned None", file=sys.stderr)
        return 1

    n_in = int(out["num_inliers"])
    if n_in < 4:
        print("FAIL: expected 4 inliers, got", n_in, file=sys.stderr)
        return 1

    cfw = out["cam_from_world"]
    t = np.asarray(cfw.translation, dtype=np.float64).ravel()
    if float(np.linalg.norm(t)) > 0.1:
        print("FAIL: expected near-zero translation, got", t, file=sys.stderr)
        return 1

    print("OK: absolute pose", n_in, "inliers, cam_from_world ~ identity")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ModuleNotFoundError as e:
        print("FAIL:", e, file=sys.stderr)
        raise SystemExit(1) from e
