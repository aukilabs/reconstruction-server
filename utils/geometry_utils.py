import itertools
from typing import Dict, List
import numpy as np
import pycolmap
import pyceres

from src.cost_functions import RelativeTransformationSim3CostFunction


def filter_reconstruction(reconstruction, normalize_points=False):
    reconstruction.filter_all_points3D(4.0, 1.5)
    reconstruction.filter_observations_with_negative_depth()
    if normalize_points:
        reconstruction.normalize(5.0, 0.1, 0.9, True)
    return reconstruction

# TODO: Move to C++. Low prio since the global refinement is pretty fast anyway.
class QuaternionNormalizationCostFunction(pyceres.CostFunction):
    def __init__(self, weight=1.0):
        super().__init__()
        self.set_num_residuals(1)
        self.set_parameter_block_sizes([4])
        self.weight = weight

    def Evaluate(self, parameters, residuals, jacobians):
        q = parameters[0]
        n = np.linalg.norm(q)
        # residual = (1 - ||q||)^2
        residuals[0] = (1.0 - n)**2 * self.weight

        if jacobians is not None:
            # avoid division by zero
            if n > 0:
                factor = -2.0 * (1.0 - n) / n * self.weight
                for i in range(4):
                    jacobians[0][i] = factor * q[i]
            else:
                # Jacobian undefined at zero; you could set it to zero or some large value
                for i in range(4):
                    jacobians[0][i] = 0.0

        return True


def align_reconstruction_chunks(
        reconstruction: pycolmap.Reconstruction,
        chunks_image_ids: List[List[int]],
        detections_per_qr: Dict[str, List[pycolmap.Rigid3d]],
        image_ids_per_qr: Dict[str, List[int]],
        with_scale: bool = True
    ) -> List[pycolmap.Sim3d]:

    print("Going to optimize chunk alignment...")

    t_local_chunk_quat = [pycolmap.Rigid3d().rotation.quat for _ in range(len(chunks_image_ids))]
    t_local_chunk_translation = [pycolmap.Rigid3d().translation for _ in range(len(chunks_image_ids))]
    image_id_to_chunk_id = {image_id : chunk_id for chunk_id, image_ids in enumerate(chunks_image_ids) for image_id in image_ids}
    problem = pyceres.Problem()


    #loss = pyceres.HuberLoss(0.1)
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

            t_refworld_qr = reconstruction.image(image_id_ref).cam_from_world().inverse() * t_refcam_qr
            t_tgtworld_qr = reconstruction.image(image_id_tgt).cam_from_world().inverse() * t_tgtcam_qr

            cov = np.eye(6)
            cov[3:, 3:] *= 0.01

            # First relative to second
            cost_1 = RelativeTransformationSim3CostFunction(t_refworld_qr.rotation.quat,
                                                          t_refworld_qr.translation,
                                                          t_tgtworld_qr.rotation.quat,
                                                          t_tgtworld_qr.translation, cov)

            params_1 = [
                t_local_chunk_quat[chunk_id_tgt],
                t_local_chunk_translation[chunk_id_tgt],
                t_local_chunk_quat[chunk_id_ref],
                t_local_chunk_translation[chunk_id_ref]
            ]

            problem.add_residual_block(cost_1, loss, params_1)

            # Second relative to first (to ensure scale impacts in both ways symetrically)
            cost_2 = RelativeTransformationSim3CostFunction(t_tgtworld_qr.rotation.quat,
                                                          t_tgtworld_qr.translation,
                                                          t_refworld_qr.rotation.quat,
                                                          t_refworld_qr.translation, cov)
            params_2 = [
                t_local_chunk_quat[chunk_id_ref],
                t_local_chunk_translation[chunk_id_ref],
                t_local_chunk_quat[chunk_id_tgt],
                t_local_chunk_translation[chunk_id_tgt]
            ]
            problem.add_residual_block(cost_2, loss, params_2)

            qr_ids_per_chunk[chunk_id_ref].add(qr_id)
            qr_ids_per_chunk[chunk_id_tgt].add(qr_id)
            connected_chunks[chunk_id_ref].add(chunk_id_tgt)
            connected_chunks[chunk_id_tgt].add(chunk_id_ref)

    if with_scale:
        for chunk_idx in range(len(chunks_image_ids)):
            if len(qr_ids_per_chunk[chunk_idx]) < 2:
                chunks_to_fix_scale = [chunk_idx]
                print(f'Chunk {chunk_idx} has less than 2 correspondences, fixing scale') # for chunks {chunks_to_fix_scale}.')
                for chunk_fix_idx in chunks_to_fix_scale:
                    quat = t_local_chunk_quat[chunk_fix_idx]
                    if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
                        problem.set_manifold(quat, pyceres.QuaternionManifold())
            else:
                # When refining scale, the quaternion magnitude represents scale.
                # Don't put QuaternionManifold (which would hard-pin magnitude to 1)
                # Instead add a soft constraint keeping scale close to 1 but allow some change.
                # Higher weight to stay closer to 1 (trust initial scale more)
                weight = 5000.0
                scale_cost = QuaternionNormalizationCostFunction(weight=weight)
                params = [t_local_chunk_quat[chunk_idx]]
                problem.add_residual_block(scale_cost, None, params)
    else:
        for quat in t_local_chunk_quat:
            if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
                problem.set_manifold(quat, pyceres.QuaternionManifold())

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
    print(summary.FullReport())

    t_local_chunks = [pycolmap.Sim3d(pycolmap.Rotation3d(quat).norm()**2, pycolmap.Rotation3d(quat), translation) for quat, translation in zip(t_local_chunk_quat, t_local_chunk_translation)]
    for t_local_chunk in t_local_chunks:
        t_local_chunk.rotation.normalize()

    print('Refined Sim3 transforms:')
    for chunk_idx, t_local_chunk in enumerate(t_local_chunks):
        print(f'Chunk {chunk_idx} ({len(chunks_image_ids[chunk_idx]):5,d} images): {t_local_chunk}')

    for image_id in reconstruction.images.keys():
        chunk_id = image_id_to_chunk_id[image_id]
        reconstruction.images[image_id].frame.rig_from_world = pycolmap.Sim3d.transform_camera_world(
            t_local_chunks[chunk_id],
            reconstruction.images[image_id].frame.rig_from_world
        )

    for point3D_id, point3D in reconstruction.points3D.items():
        if len(point3D.track.elements) == 0:
            continue

        chunk_id = image_id_to_chunk_id[point3D.track.elements[0].image_id] # assumes 3D points are never seen from multiple chunks
        reconstruction.points3D[point3D_id].xyz = t_local_chunks[chunk_id] * point3D.xyz

    for qr_id, cam_space_detections in detections_per_qr.items():
        for det_idx, (image_id, _) in enumerate(zip(image_ids_per_qr[qr_id], cam_space_detections)):
            chunk_id = image_id_to_chunk_id[image_id]
            detections_per_qr[qr_id][det_idx].translation *= t_local_chunks[chunk_id].scale

    print("Chunk alignment optimization DONE\n")

    return t_local_chunks