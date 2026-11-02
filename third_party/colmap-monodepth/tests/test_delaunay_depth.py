"""Unit tests for screen-space Delaunay depth rasterize."""

from __future__ import annotations

import numpy as np
import pytest

pytest.importorskip("numba")
pytest.importorskip("scipy")

from colmap_monodepth.delaunay_depth import rasterize_depth, tri_ok


def test_rasterize_fills_triangle_interior():
    h, w = 32, 32
    pts = np.array([[4.0, 4.0], [28.0, 4.0], [16.0, 28.0]], dtype=np.float64)
    zs = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    faces = np.array([[0, 1, 2]], dtype=np.int32)
    depth = rasterize_depth(h, w, pts, zs, faces)
    assert depth.shape == (h, w)
    assert np.count_nonzero(depth > 0) > 50
    # Center-ish pixel should be covered with interpolated z in [1, 3].
    zc = depth[14, 16]
    assert zc > 0
    assert 1.0 <= zc <= 3.0


def test_tri_ok_rejects_long_edge():
    pts = np.array([[0.0, 0.0], [100.0, 0.0], [0.0, 1.0]], dtype=np.float64)
    zs = np.array([1.0, 1.0, 1.0], dtype=np.float32)
    face = np.array([0, 1, 2], dtype=np.int32)
    assert not tri_ok(pts, zs, face, max_edge_px=10.0, max_rel_dz=0.5)
    assert tri_ok(pts, zs, face, max_edge_px=200.0, max_rel_dz=0.5)
