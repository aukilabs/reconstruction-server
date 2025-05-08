import itertools
from typing import Dict, List
import numpy as np
import pycolmap
import pyceres
from pathlib import Path

from utils.data_utils import get_world_space_qr_codes
from utils.bundle_adjuster import PyBundleAdjuster

from src.cost_functions import RelativeTransformationSim3CostFunction


def dmt_global_stitching(detections_per_qr,
                         image_ids_per_qr,
                         timestamp_per_image,
                         arkit_precomputed,
                         reconstruction,
                         ba_options,
                         ba_config):

    refinement_config = {
        'add_rel_constraints': True,
        'use_arkit_relposes': False,
        'use_arkit_centerdist': False
    }

    bundle_adjuster = PyBundleAdjuster(ba_options, ba_config, refinement_config)
    bundle_adjuster.set_up_problem(
        reconstruction,
        ba_options.create_loss_function(),
        timestamp_per_image,
        detections_per_qr,
        image_ids_per_qr,
        arkit_precomputed,
        verbose=False
    )

    solver_options = bundle_adjuster.set_up_solver_options(
        bundle_adjuster.problem, ba_options.solver_options
    )
    solver_options.linear_solver_type = pyceres.LinearSolverType.SPARSE_SCHUR
    solver_options.minimizer_progress_to_stdout = True
    solver_options.logging_type = pyceres.LoggingType.PER_MINIMIZER_ITERATION

    initial_loss_breakdown, initial_loss_breakdown_per_image_id = bundle_adjuster.evaluate_loss_breakdown()
    print("------------")
    print("INITIAL LOSS BREAKDOWN:")
    print("\n".join(f"{category}: {loss}" for category, loss in initial_loss_breakdown.items()))
    print("------------")

    summary = pyceres.SolverSummary()
    pyceres.solve(solver_options, bundle_adjuster.problem, summary)
    final_loss_breakdown, final_loss_breakdown_per_image_id = bundle_adjuster.evaluate_loss_breakdown()

    print("------------")
    print("INITIAL LOSS BREAKDOWN:")
    print("\n".join(f"{category}: {loss}" for category, loss in initial_loss_breakdown.items()))
    print("------------")
    print("FINAL LOSS BREAKDOWN:")
    print("\n".join(f"{category}: {loss}" for category, loss in final_loss_breakdown.items()))
    print("------------")

    if not summary.IsSolutionUsable():
        print("\n".join(summary.FullReport().split(",")))
        print("Solution not usable!")
        raise Exception("Solver failed! No usable solution found.")

    print("Solved")
    loss_details = (
        initial_loss_breakdown,
        initial_loss_breakdown_per_image_id,
        final_loss_breakdown,
        final_loss_breakdown_per_image_id
    )

    return (summary, loss_details)


def run_stitching(detections_per_qr,
                  image_ids_per_qr,
                  timestamp_per_image,
                  arkit_precomputed,
                  combined_rec,
                  sorted_image_ids,
                  global_ba=True):

    if global_ba:
        # Run bundle adjustment
        ba_options = pycolmap.BundleAdjustmentOptions()
        ba_options.refine_focal_length = False
        ba_options.refine_extra_params = False
        ba_options.refine_principal_point = False
        ba_options.solver_options.max_num_iterations = 1000

        # Configure bundle adjustment
        ba_config = pycolmap.BundleAdjustmentConfig()

        for image_id in sorted_image_ids:
            ba_config.add_image(image_id)

        for point_id in combined_rec.point3D_ids():
            ba_config.add_variable_point(point_id)

        # Fix 7-DOFs of the bundle adjustment problem
        # ba_config.set_constant_cam_pose(sorted_image_ids[0])
        # ba_config.set_constant_cam_positions(sorted_image_ids[1], [0])

        for image_id in combined_rec.images:
            if image_id not in arkit_precomputed:
                ba_config.set_constant_cam_pose(image_id)

        print("Start Global Bundle Adjustment")
        summary, loss_details = dmt_global_stitching(detections_per_qr,
                                                     image_ids_per_qr,
                                                     timestamp_per_image,
                                                     arkit_precomputed,
                                                     combined_rec,
                                                     ba_options,
                                                     ba_config)

        if summary is not None:
            print("\n".join(summary.FullReport().split(",")))

    combined_out_dir = Path("/tmp/stitched_rec")
    combined_out_dir.mkdir(exist_ok=True, parents=True)
    combined_rec.write(combined_out_dir)

    combined_detections = get_world_space_qr_codes(combined_rec, detections_per_qr, image_ids_per_qr)
    #save_qr_poses_csv(combined_detections, combined_out_dir / "refined_portal_poses.csv")
    
    print("\n-------------\n")

    return combined_rec, combined_detections


def robust_scale(A, B) -> float:
    """Median distance ratio with MAD-based outlier rejection."""
    A = np.asarray(A)[::2]
    B = np.asarray(B)[::2]
    dA = np.linalg.norm(A[1:] - A[:-1], axis=1)
    dB = np.linalg.norm(B[1:] - B[:-1], axis=1)
    mask = dA > 0.01
    ratios = dB[mask] / dA[mask]
    med = np.median(ratios)
    devs = np.abs(ratios - med)
    mad = np.mean(devs) + 1e-7
    inlier = devs < 2 * mad
    print(f"Estimating scale with {np.sum(inlier)} out of {len(ratios)} relative movements. (mask: {np.sum(mask)})")
    print("median: ", med)
    print("mad: ", mad)
    scale = np.median(ratios[inlier])
    print("scale: ", scale)
    return scale


def filter_reconstruction(reconstruction, normalize_points=False):
    reconstruction.filter_all_points3D(4.0, 1.5)
    reconstruction.filter_observations_with_negative_depth()
    if normalize_points:
        reconstruction.normalize(5.0, 0.1, 0.9, True)
    return reconstruction


def align_reconstruction_chunks(
        reconstruction: pycolmap.Reconstruction,
        chunks_image_ids: List[List[int]],
        detections_per_qr: Dict[str, List[pycolmap.Rigid3d]],
        image_ids_per_qr: Dict[str, List[int]],
        with_scale: bool = True
    ):

    t_local_chunk_quat = [pycolmap.Rigid3d().rotation.quat for _ in range(len(chunks_image_ids))]
    t_local_chunk_translation = [pycolmap.Rigid3d().translation for _ in range(len(chunks_image_ids))]
    image_id_to_chunk_id = {image_id : chunk_id for chunk_id, image_ids in enumerate(chunks_image_ids) for image_id in image_ids}
    problem = pyceres.Problem()

    # TODO: Care less about qr rotation "loop closure", more about position.
    # TODO: Add gravity prior
    # TODO: Use reprojection error of QR code corners in cameras which observed it
    # TODO: Robust loss (cauchy / huber) to care less about occasional bad QR detections.
    # TODO: Human constraints optional support

    #loss = pyceres.HuberLoss(1.0)
    loss = None
    
    qr_ids_per_chunk = [set() for _ in range(len(chunks_image_ids))]
    connected_chunks = [set() for _ in range(len(chunks_image_ids))]
    for qr_id, cam_space_detections in detections_per_qr.items():
        assert qr_id in image_ids_per_qr and len(image_ids_per_qr[qr_id]) == len(cam_space_detections)
        image_ids = image_ids_per_qr[qr_id]

        for (image_id_ref, t_refcam_qr), (image_id_tgt, t_tgtcam_qr) in set(itertools.combinations(zip(image_ids, cam_space_detections), 2)):
            assert image_id_ref != image_id_tgt

            chunk_id_ref, chunk_id_tgt = image_id_to_chunk_id[image_id_ref], image_id_to_chunk_id[image_id_tgt]

            if chunk_id_ref == chunk_id_tgt:
                continue

            t_refworld_qr = reconstruction.image(image_id_ref).cam_from_world.inverse() * t_refcam_qr
            t_tgtworld_qr = reconstruction.image(image_id_tgt).cam_from_world.inverse() * t_tgtcam_qr

            cov = np.eye(6)
            #cov[0:3, 0:3] *= 0.01
            cov[3:, 3:] *= 0.01 # Care more about rotation
            cost = RelativeTransformationSim3CostFunction(t_refworld_qr.rotation.quat,
                                                          t_refworld_qr.translation,
                                                          t_tgtworld_qr.rotation.quat,
                                                          t_tgtworld_qr.translation, cov)

            params = [
                t_local_chunk_quat[chunk_id_tgt],
                t_local_chunk_translation[chunk_id_tgt],
                t_local_chunk_quat[chunk_id_ref],
                t_local_chunk_translation[chunk_id_ref]
            ]

            problem.add_residual_block(cost, loss, params)
            qr_ids_per_chunk[chunk_id_ref].add(qr_id)
            qr_ids_per_chunk[chunk_id_tgt].add(qr_id)
            connected_chunks[chunk_id_ref].add(chunk_id_tgt)
            connected_chunks[chunk_id_tgt].add(chunk_id_ref)

    if with_scale:
        for chunk_idx in range(len(chunks_image_ids)):
            if len(qr_ids_per_chunk[chunk_idx]) < 2:
                chunks_to_fix_scale = list(connected_chunks[chunk_idx]) + [chunk_idx]
                print(f'Chunk {chunk_idx} has less than 2 correspondences, fixing scale for chunks {chunks_to_fix_scale}.')
                for chunk_fix_idx in chunks_to_fix_scale:
                    quat = t_local_chunk_quat[chunk_fix_idx]
                    if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
                        problem.set_manifold(quat, pyceres.QuaternionManifold())
    else:
        for quat in t_local_chunk_quat:
            if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
                problem.set_manifold(quat, pyceres.QuaternionManifold())

    solver_options = pyceres.SolverOptions()
    solver_options.linear_solver_type = pyceres.LinearSolverType.SPARSE_SCHUR
    solver_options.minimizer_progress_to_stdout = True
    solver_options.function_tolerance = 0.0
    solver_options.gradient_tolerance = 0.0
    solver_options.parameter_tolerance = 0.0
    solver_options.max_num_iterations = 50
    solver_options.logging_type = pyceres.LoggingType.PER_MINIMIZER_ITERATION

    summary = pyceres.SolverSummary()
    pyceres.solve(solver_options, problem, summary)
    print(summary.FullReport())

    t_local_chunks = [pycolmap.Sim3d(pycolmap.Rotation3d(quat).norm()**2, pycolmap.Rotation3d(quat), translation) for quat, translation in zip(t_local_chunk_quat, t_local_chunk_translation)]
    for t_local_chunk in t_local_chunks:
        t_local_chunk.rotation.normalize()

    print('Refined Sim3 transforms:')
    for chunk_idx, t_local_chunk in enumerate(t_local_chunks):
        print(f'Chunk {chunk_idx} ({len(chunks_image_ids[chunk_idx]):5,d} images): {t_local_chunk}')

    for image_id in reconstruction.images.keys():
        chunk_id = image_id_to_chunk_id[image_id]
        reconstruction.images[image_id].cam_from_world = pycolmap.Sim3d.transform_camera_world(t_local_chunks[chunk_id], reconstruction.images[image_id].cam_from_world)

    for point3D_id, point3D in reconstruction.points3D.items():
        if len(point3D.track.elements) == 0:
            continue

        chunk_id = image_id_to_chunk_id[point3D.track.elements[0].image_id] # assumes 3D points are never seen from multiple chunks
        reconstruction.points3D[point3D_id].xyz = t_local_chunks[chunk_id] * point3D.xyz

    for qr_id, cam_space_detections in detections_per_qr.items():
        for det_idx, (image_id, _) in enumerate(zip(image_ids_per_qr[qr_id], cam_space_detections)):
            chunk_id = image_id_to_chunk_id[image_id]
            detections_per_qr[qr_id][det_idx].translation *= t_local_chunks[chunk_id].scale

    return