"""Ceres refinement of the scan alignment."""
import pycolmap
import numpy as np
import sys
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
import itertools
import pyceres
from reconstruction.pipeline.update.update import transform_with_scale
from reconstruction.geometry.costs import QuaternionNormalizationCostFunction, TranslationPriorCostFunction
from reconstruction.native.cost_functions import RelativeTransformationSim3CostFunction
from reconstruction.pipeline.global_.alignment import AlignedScans
from reconstruction.pipeline.global_.portal_metrics import PORTAL_DOWNWEIGHT_COV_SCALE, PortalAlignmentMetrics, accept_portal_spread_rmse, build_portal_alignment_metrics, build_portal_overlap_edges, compute_overlap_rmse, compute_portal_spread_metrics, compute_portal_spread_rmse, flag_outlier_portal_edges, logical_portal_edge_key, portal_edge_cov_multiplier


# Soft gravity prior: keep each scan's local-refine up (COLMAP -X) after Sim3 delta.
PORTAL_GRAVITY_PRIOR_WEIGHT = 20.0


# Huber scale a ≈ weight * 2*sin(θ/2); a=1.0 ≈ firm below ~3° tilt, linear above.
PORTAL_GRAVITY_PRIOR_HUBER_SCALE = 1.0


# Soft pin of origin-scan Sim3-delta translation to 0 (stops gauge float off floor_origin).
# Modest vs portal residual mass — enough to hold ~cm–dm, not override overlaps.
PORTAL_ORIGIN_PRIOR_WEIGHT = 10.0


# Local-refine frames are ARKit/gravity aligned → down is COLMAP -X.
_PORTAL_LOCAL_GRAVITY = np.array([-1.0, 0.0, 0.0], dtype=float)


def _run_ceres_alignment_solve(
    aligned_scans: AlignedScans,
    logger: logging.Logger,
    downweighted_edges: Optional[Set[Tuple[str, str, str]]] = None,
) -> AlignedScans:
    """Build Ceres problem from aligned scans, solve, return refined AlignedScans."""
    scan_ids = aligned_scans.scan_ids
    t_local_scan_quat = [pycolmap.Rigid3d().rotation.quat for _ in range(len(scan_ids))]
    t_local_scan_translation = [pycolmap.Rigid3d().translation for _ in range(len(scan_ids))]
    problem = pyceres.Problem()

    loss = None

    qr_ids_per_scan = [set() for _ in range(len(scan_ids))]
    detections_per_qr: Dict[str, List[tuple[int, pycolmap.Rigid3d]]] = {}
    for scan_idx, scan_id in enumerate(scan_ids):
        for qr_id, aligned_qr_poses in aligned_scans.aligned_portal_detections[scan_id].items():
            if qr_id not in detections_per_qr:
                detections_per_qr[qr_id] = []
            detections_per_qr[qr_id].extend(
                (scan_idx, aligned_qr_pose)
                for aligned_qr_pose in aligned_qr_poses
            )

    for qr_id, world_space_detections in detections_per_qr.items():
        scan_pairs = itertools.combinations(world_space_detections, 2)
        for (scan_idx_ref, t_refworld_qr), (scan_idx_tgt, t_tgtworld_qr) in scan_pairs:
            if scan_idx_ref == scan_idx_tgt:
                continue

            cov_multiplier = portal_edge_cov_multiplier(
                scan_ids[scan_idx_ref],
                scan_ids[scan_idx_tgt],
                qr_id,
                downweighted_edges,
            )
            cov = np.eye(6)
            cov[3:, 3:] *= 0.01
            cov *= cov_multiplier

            cost_1 = RelativeTransformationSim3CostFunction(
                t_refworld_qr.rotation.quat,
                t_refworld_qr.translation,
                t_tgtworld_qr.rotation.quat,
                t_tgtworld_qr.translation,
                cov,
            )
            params_1 = [
                t_local_scan_quat[scan_idx_tgt],
                t_local_scan_translation[scan_idx_tgt],
                t_local_scan_quat[scan_idx_ref],
                t_local_scan_translation[scan_idx_ref],
            ]
            problem.add_residual_block(cost_1, loss, params_1)

            cost_2 = RelativeTransformationSim3CostFunction(
                t_tgtworld_qr.rotation.quat,
                t_tgtworld_qr.translation,
                t_refworld_qr.rotation.quat,
                t_refworld_qr.translation,
                cov,
            )
            params_2 = [
                t_local_scan_quat[scan_idx_ref],
                t_local_scan_translation[scan_idx_ref],
                t_local_scan_quat[scan_idx_tgt],
                t_local_scan_translation[scan_idx_tgt],
            ]
            problem.add_residual_block(cost_2, loss, params_2)

            qr_ids_per_scan[scan_idx_ref].add(qr_id)
            qr_ids_per_scan[scan_idx_tgt].add(qr_id)

    # Soft origin-position prior: first aligned scan's stitch maps floor portal → (0,0,0);
    # pin that scan's Sim3-delta translation so the gauge cannot float off origin.
    origin_scan_idx = 0
    origin_scan_id = scan_ids[origin_scan_idx]
    origin_trans = t_local_scan_translation[origin_scan_idx]
    origin_prior = TranslationPriorCostFunction(
        target=(0.0, 0.0, 0.0),
        weight=PORTAL_ORIGIN_PRIOR_WEIGHT,
    )
    problem.add_residual_block(origin_prior, None, [origin_trans])
    logger.info(
        f"Origin prior: scan={origin_scan_id} idx={origin_scan_idx} "
        f"target=(0,0,0) w={PORTAL_ORIGIN_PRIOR_WEIGHT}"
    )

    # Soft gravity prior: trust local-refinement up (COLMAP -X) per scan.
    # residual = w * (R_delta * R_stitch * local_g - world_g), Huber-robustified.
    from reconstruction.native.cost_functions import GravityDirectionPriorSim3DeltaCostFunction

    gravity_loss = pyceres.HuberLoss(PORTAL_GRAVITY_PRIOR_HUBER_SCALE)
    gravity_costs = []
    for scan_idx, scan_id in enumerate(scan_ids):
        stitch = aligned_scans.alignment_transforms[scan_id]
        cost = GravityDirectionPriorSim3DeltaCostFunction(
            np.asarray(stitch.rotation.quat, dtype=float),
            _PORTAL_LOCAL_GRAVITY,
            PORTAL_GRAVITY_PRIOR_WEIGHT,
        )
        quat = t_local_scan_quat[scan_idx]
        gravity_costs.append((scan_id, cost, quat))
        problem.add_residual_block(cost, gravity_loss, [quat])
        if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
            problem.set_manifold(quat, pyceres.QuaternionManifold())

    def _gravity_cost_stats(label: str) -> Tuple[float, float]:
        """Return (sum raw ||r||^2, sum Huber rho) and log per-pass contribution."""
        raw_sq = []
        huber_vals = []
        tilts_deg = []
        world_down = _PORTAL_LOCAL_GRAVITY
        for scan_id, cost, quat in gravity_costs:
            residuals, _ = cost.evaluate(quat)
            r = np.asarray(residuals, dtype=float).reshape(-1)
            s = float(np.dot(r, r))
            raw_sq.append(s)
            # Ceres LossFunction.evaluate(s) -> [rho, ...]
            huber_vals.append(float(gravity_loss.evaluate(s)[0]))
            # Effective world-down tilt after delta*stitch (independent of weight).
            delta = pycolmap.Rotation3d(quat)
            stitch_R = aligned_scans.alignment_transforms[scan_id].rotation
            mapped = (delta * stitch_R).matrix() @ world_down
            c = float(np.clip(np.dot(mapped, world_down), -1.0, 1.0))
            tilts_deg.append(float(np.degrees(np.arccos(c))))
        sum_raw = float(np.sum(raw_sq))
        sum_huber = float(np.sum(huber_vals))
        logger.info(
            f"Gravity prior [{label}]: n={len(gravity_costs)} "
            f"raw_cost={sum_raw:.3f} huber_cost={sum_huber:.3f} "
            f"(w={PORTAL_GRAVITY_PRIOR_WEIGHT}, Huber a={PORTAL_GRAVITY_PRIOR_HUBER_SCALE}); "
            f"tilt_deg mean={np.mean(tilts_deg):.2f} max={np.max(tilts_deg):.2f} "
            f"p90={np.percentile(tilts_deg, 90):.2f}"
        )
        return sum_raw, sum_huber

    grav_raw0, grav_huber0 = _gravity_cost_stats("pre-solve")

    for scan_idx in range(len(scan_ids)):
        if len(qr_ids_per_scan[scan_idx]) < 2:
            quat = t_local_scan_quat[scan_idx]
            logger.info(f"Scan {scan_idx} has less than 2 correspondences, fixing scale")
            if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
                problem.set_manifold(quat, pyceres.QuaternionManifold())
        else:
            weight = 5000.0
            scale_cost = QuaternionNormalizationCostFunction(weight=weight)
            params = [t_local_scan_quat[scan_idx]]
            problem.add_residual_block(scale_cost, None, params)

    solver_options = pyceres.SolverOptions()
    solver_options.linear_solver_type = pyceres.LinearSolverType.SPARSE_NORMAL_CHOLESKY
    solver_options.minimizer_progress_to_stdout = False
    solver_options.function_tolerance = 0.0
    solver_options.gradient_tolerance = 0.0
    solver_options.parameter_tolerance = 0.0
    solver_options.max_num_iterations = 100
    solver_options.logging_type = pyceres.LoggingType.SILENT

    summary = pyceres.SolverSummary()
    pyceres.solve(solver_options, problem, summary)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"{summary.FullReport()}")
    else:
        logger.info(f"{summary.BriefReport()}")

    grav_raw1, grav_huber1 = _gravity_cost_stats("post-solve")
    # Ceres reports 0.5 * sum rho; compare gravity huber sum on same footing.
    initial = float(summary.initial_cost)
    final = float(summary.final_cost)
    # Our huber_cost is sum rho; Ceres cost is typically 1/2 sum rho.
    grav_share0 = (0.5 * grav_huber0 / initial) if initial > 0 else 0.0
    grav_share1 = (0.5 * grav_huber1 / final) if final > 0 else 0.0
    logger.info(
        f"Gravity prior vs total Ceres cost: "
        f"pre {0.5*grav_huber0:.3f}/{initial:.3f} ({100*grav_share0:.1f}%), "
        f"post {0.5*grav_huber1:.3f}/{final:.3f} ({100*grav_share1:.1f}%)"
    )
    origin_t = np.asarray(t_local_scan_translation[origin_scan_idx], dtype=float).reshape(3)
    logger.info(
        f"Origin prior post-solve: scan={origin_scan_id} "
        f"delta_t={origin_t.tolist()} |t|={float(np.linalg.norm(origin_t)):.4f}m"
    )

    t_local_scans = [
        pycolmap.Sim3d(
            pycolmap.Rotation3d(quat).norm() ** 2,
            pycolmap.Rotation3d(quat),
            translation,
        )
        for quat, translation in zip(t_local_scan_quat, t_local_scan_translation)
    ]
    for t_local_scan in t_local_scans:
        t_local_scan.rotation.normalize()

    logger.debug("Refined Sim3 transforms:")
    for scan_idx, t_local_scan in enumerate(t_local_scans):
        logger.debug(f"Scan {scan_idx} ({scan_ids[scan_idx]}): {t_local_scan}")

    refined_alignment_transforms = {
        scan_id: t_local_scans[scan_idx] * aligned_scans.alignment_transforms[scan_id]
        for scan_idx, scan_id in enumerate(scan_ids)
    }
    refined_aligned_portal_detections = {
        scan_id: {
            qr_id: [
                transform_with_scale(refined_alignment_transforms[scan_id], pose)
                for pose in poses
            ]
            for qr_id, poses in aligned_scans.raw_portal_detections[scan_id].items()
        }
        for scan_id in scan_ids
    }

    return AlignedScans(
        scan_ids=scan_ids,
        alignment_transforms=refined_alignment_transforms,
        raw_portal_detections=aligned_scans.raw_portal_detections,
        aligned_portal_detections=refined_aligned_portal_detections,
    )


def refine_alignment(
    aligned_scans: AlignedScans,
    job_root_path: Path,
    logger: Optional[logging.Logger] = None,
    enable_portal_reweight: bool = True,
) -> Tuple[AlignedScans, PortalAlignmentMetrics]:
    if logger is None:
        logger = logging.getLogger("scan_alignment")
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.info("Going to optimize chunk alignment...")

    first_result = _run_ceres_alignment_solve(aligned_scans, logger)

    if not enable_portal_reweight:
        logger.info("Portal reweight pass disabled; using first solve only.")
        metrics = build_portal_alignment_metrics(
            first_result,
            first_result,
            flagged_edges=[],
            applied=False,
            accepted=False,
        )
        logger.info("Scan alignment refined successfully!")
        return first_result, metrics

    overlap_edges = build_portal_overlap_edges(first_result)
    flagged_edges = flag_outlier_portal_edges(overlap_edges)

    if not flagged_edges:
        logger.info("No outlier portal overlap edges flagged; skipping reweight pass.")
        metrics = build_portal_alignment_metrics(
            first_result,
            first_result,
            flagged_edges=[],
            applied=False,
            accepted=False,
        )
        logger.info("Scan alignment refined successfully!")
        return first_result, metrics

    downweighted_keys: Set[Tuple[str, str, str]] = {
        logical_portal_edge_key(edge.scan_a, edge.scan_b, edge.portal_id)
        for edge in flagged_edges
    }
    logger.info(
        "Outlier portal overlap edges flagged (%d); running reweight pass (cov×%d).",
        len(flagged_edges),
        PORTAL_DOWNWEIGHT_COV_SCALE,
    )
    for edge in flagged_edges:
        logger.info(
            "  downweight %s↔%s portal %s (Δp=%.4f m, Δr=%.2f deg)",
            edge.scan_a,
            edge.scan_b,
            edge.portal_id,
            edge.dp_m,
            edge.dr_deg,
        )

    second_result = _run_ceres_alignment_solve(
        aligned_scans,
        logger,
        downweighted_edges=downweighted_keys,
    )

    spread_rmse_before = compute_portal_spread_rmse(compute_portal_spread_metrics(first_result))
    spread_rmse_after = compute_portal_spread_rmse(compute_portal_spread_metrics(second_result))
    edges_before = build_portal_overlap_edges(first_result)
    edges_after = build_portal_overlap_edges(second_result)
    trusted_before = compute_overlap_rmse(edges_before, exclude_keys=downweighted_keys)
    trusted_after = compute_overlap_rmse(edges_after, exclude_keys=downweighted_keys)
    accepted = accept_portal_spread_rmse(trusted_before, trusted_after)

    if accepted:
        logger.info(
            "Reweight pass accepted (trusted RMSE pos %.6f→%.6f m, rot %.6f→%.6f deg; "
            "portal-spread pos %.6f→%.6f m, rot %.6f→%.6f deg).",
            trusted_before.position_m,
            trusted_after.position_m,
            trusted_before.rotation_deg,
            trusted_after.rotation_deg,
            spread_rmse_before.position_m,
            spread_rmse_after.position_m,
            spread_rmse_before.rotation_deg,
            spread_rmse_after.rotation_deg,
        )
        final_result = second_result
    else:
        logger.info(
            "Reweight pass reverted (trusted RMSE pos %.6f→%.6f m, rot %.6f→%.6f deg; "
            "portal-spread pos %.6f→%.6f m, rot %.6f→%.6f deg).",
            trusted_before.position_m,
            trusted_after.position_m,
            trusted_before.rotation_deg,
            trusted_after.rotation_deg,
            spread_rmse_before.position_m,
            spread_rmse_after.position_m,
            spread_rmse_before.rotation_deg,
            spread_rmse_after.rotation_deg,
        )
        final_result = first_result

    metrics = build_portal_alignment_metrics(
        first_result,
        second_result,
        flagged_edges=flagged_edges,
        applied=True,
        accepted=accepted,
    )
    logger.info("Scan alignment refined successfully!")
    return final_result, metrics


def print_alignment_comparison(
    aligned_scans: AlignedScans,
    refined_aligned_scans: AlignedScans,
    logger: logging.Logger
) -> Tuple[Dict[str, float], Dict[str, float], List[str]]:
    """Print comparison of alignment transforms between aligned and refined aligned scans."""
    move_deltas = {} # cm
    rotation_deltas = {} # degrees
    for scan_id in aligned_scans.scan_ids:
        aligned_transform = aligned_scans.alignment_transforms[scan_id]
        refined_transform = refined_aligned_scans.alignment_transforms[scan_id]
        rel_pose = aligned_transform.inverse() * refined_transform
        rel_cm = np.linalg.norm(rel_pose.translation) * 100
        rel_deg = np.rad2deg(np.arccos(rel_pose.rotation.quat.dot(np.array([0.0, 0.0, 0.0, 1.0]))))
        move_deltas[scan_id] = rel_cm
        rotation_deltas[scan_id] = rel_deg
        logger.debug(f"Scan {scan_id} moved by {rel_cm:.2f} cm, turned by {rel_deg:.2f} degrees")

    logger.info(f"Refined alignment compared to basic alignment:")
    move_vals = list(move_deltas.values())
    rot_vals = list(rotation_deltas.values())

    logger.info(
        f"Position change (cm): "
        f"mean={np.mean(move_vals):.2f}, "
        f"max={np.max(move_vals):.2f}, "
        f"90%={np.percentile(move_vals, 90):.2f}"
    )
    logger.info(
        f"Rotation change (deg): "
        f"mean={np.mean(rot_vals):.2f}, "
        f"max={np.max(rot_vals):.2f}, "
        f"90%={np.percentile(rot_vals, 90):.2f}"
    )

    # Find and warn for any scans which were moved or rotated significantly by refinement.
    # If we see any weird setup accuracy issues checking these logs manually can help us figure out why.
    # Relative AND absolute thresholds must be met, so we catch outliers but avoid false warnings.
    high_move_rel = move_vals > np.mean(move_vals) + 3 * np.std(move_vals)
    high_move_abs = np.array(move_vals) > 100.0
    high_rot_rel = rot_vals > np.mean(rot_vals) + 3 * np.std(rot_vals)
    high_rot_abs = np.array(rot_vals) > 10
    high_indices = np.where(
        (high_move_rel & high_move_abs) | (high_rot_rel & high_rot_abs)
    )[0]

    high_scan_ids = []
    for index in high_indices:
        high_scan_ids.append(aligned_scans.scan_ids[index])
        logger.warning(
            f"WARNING: Scan {aligned_scans.scan_ids[index]} changed significantly by alignment refining, "
            f"which may indicate accuracy issues. Please check if your reconstruction seems off. "
            f"Changed by: {move_vals[index]:.2f} cm, {rot_vals[index]:.2f} deg")

    return move_deltas, rotation_deltas, high_scan_ids
