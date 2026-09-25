"""Light mesh cleanup after TSDF extract (local + global share this path)."""

from __future__ import annotations

from typing import Any


def postprocess_tsdf_mesh(mesh, *, config: Any | None = None) -> tuple[Any, dict[str, Any]]:
    """Taubin smooth then mild quadric decimate (error-bounded when supported).

    Uses mild defaults: 2 Taubin iterations, then iterative decimation stopping when
    ``maximum_error`` is reached or triangle count falls to ~50% of the post-Taubin mesh.
    """
    import open3d as o3d

    taubin_iterations = 2
    taubin_lambda = 0.5
    taubin_mu = -0.52
    max_geometric_error = 0.008
    min_triangle_ratio = 0.5

    if config is not None:
        taubin_iterations = int(getattr(config, "postprocess_taubin_iterations", taubin_iterations))
        taubin_lambda = float(getattr(config, "postprocess_taubin_lambda", taubin_lambda))
        taubin_mu = float(getattr(config, "postprocess_taubin_mu", taubin_mu))
        max_geometric_error = float(
            getattr(config, "postprocess_max_geometric_error", max_geometric_error)
        )
        min_triangle_ratio = float(
            getattr(config, "postprocess_min_triangle_ratio", min_triangle_ratio)
        )

    n_tris_before = len(mesh.triangles)
    if n_tris_before == 0 or len(mesh.vertices) == 0:
        return mesh, {
            "postprocess_skipped": "empty_mesh",
            "triangles_before": n_tris_before,
            "triangles_after": n_tris_before,
        }

    if taubin_iterations > 0:
        mesh = mesh.filter_smooth_taubin(
            number_of_iterations=taubin_iterations,
            lambda_filter=taubin_lambda,
            mu=taubin_mu,
        )
    mesh.compute_vertex_normals()
    n_after_taubin = len(mesh.triangles)

    target_floor = max(4, int(n_after_taubin * min_triangle_ratio))
    n_tris_after = n_after_taubin
    decimate_method = "none"

    if n_after_taubin > target_floor:
        simplified = mesh
        decimate_method = "quadric_max_error"
        try:
            candidate = mesh.simplify_quadric_decimation(
                target_number_of_triangles=target_floor,
                maximum_error=max_geometric_error,
            )
            if len(candidate.triangles) > 0:
                simplified = candidate
        except TypeError:
            decimate_method = "quadric_target_only"
            candidate = mesh.simplify_quadric_decimation(
                target_number_of_triangles=target_floor
            )
            if len(candidate.triangles) > 0:
                simplified = candidate
        mesh = simplified
        mesh.compute_vertex_normals()
        n_tris_after = len(mesh.triangles)

    meta = {
        "postprocess_taubin_iterations": taubin_iterations,
        "postprocess_max_geometric_error": max_geometric_error,
        "postprocess_decimate_method": decimate_method,
        "triangles_before": n_tris_before,
        "triangles_after_taubin": n_after_taubin,
        "triangles_after": n_tris_after,
    }
    return mesh, meta
