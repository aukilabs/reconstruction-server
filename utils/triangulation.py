from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional, List
import logging

import pycolmap
import pyceres

from hloc.triangulation import create_db_from_model, import_features, import_matches
from hloc import pairs_from_poses, extract_features, match_features
import utils.pairs_from_sequential as pairs_from_sequential

from utils.bundle_adjuster import PyBundleAdjuster


def run_triangulation(
    database_path: Path,
    image_dir: Path,
    reference_model: pycolmap.Reconstruction,
    options: Dict[str, Any],
    timestamp_per_image: Optional[Dict[str, int]] = None,
    arkit_precomputed=None,
    detections_per_qr=None,
    image_ids_per_qr=None
) -> pycolmap.Reconstruction:
    # Grab logger by name
    logger = logging.getLogger('refine_dataset')

    mapper_options = pycolmap.IncrementalMapperOptions(options)

    database = pycolmap.Database(database_path)

    min_num_matches = 15
    ignore_watermarks = True
    image_names = set()
    database_cache = pycolmap.DatabaseCache.create(database, min_num_matches, ignore_watermarks, image_names)

    reconstruction = deepcopy(reference_model)

    clear_points = True
    if clear_points:
        for point3D_id in reconstruction.point3D_ids():
            reconstruction.delete_point3D(point3D_id)
    
    mapper = pycolmap.IncrementalMapper(database_cache)
    mapper.begin_reconstruction(reconstruction)

    tri_options = pycolmap.IncrementalTriangulatorOptions()
    tri_options.re_min_ratio = 0.8
    tri_options.re_max_angle_error = 8.0
    tri_options.re_max_trials = 3

    for image_id in reconstruction.reg_image_ids():
        image = reconstruction.images[image_id]
        num_existing_points = image.num_points3D
        mapper.triangulate_image(tri_options, image_id)
        logger.info(f'Image {image_id}: seen {num_existing_points} points, triangulated {image.num_points3D - num_existing_points} points.')

    mapper.complete_and_merge_tracks(tri_options)

    ba_options = pycolmap.BundleAdjustmentOptions()

    ba_options.refine_focal_length = False
    ba_options.refine_principal_point = False
    ba_options.refine_extra_params = False
    ba_options.refine_extrinsics = True
    ba_options.solver_options.max_num_iterations = 150
    ba_options.solver_options.gradient_tolerance = 1.0
    ba_options.solver_options.logging_type = pyceres.LoggingType.PER_MINIMIZER_ITERATION
    ba_options.solver_options.minimizer_progress_to_stdout = True

    num_ba_iterations_total = 5

    sorted_image_ids = sorted(reconstruction.reg_image_ids())

    retriangulated = False
    ba_iterations_remaining = num_ba_iterations_total
    while ba_iterations_remaining > 0:
        mapper.observation_manager.filter_observations_with_negative_depth()

        num_observations = reconstruction.compute_num_observations()

        logger.info(f'Bundle adjustment ({num_ba_iterations_total - ba_iterations_remaining + 1}/{num_ba_iterations_total})')

        ba_config = pycolmap.BundleAdjustmentConfig()

        for image_id in sorted_image_ids:
            ba_config.add_image(image_id)

        loss = ba_options.create_loss_function()

        # Fix 7-DOFs of the bundle adjustment problem
        ba_config.set_constant_cam_pose(sorted_image_ids[0])
        ba_config.set_constant_cam_positions(sorted_image_ids[1], [0])

        # Adjust refinement config to add more weight to relative se3 poses (to keep scale from changing),
        # and set max speed between adjacent frames (to filter out potential arkit pose jumps).
        # These values were selected experimentally, and might require some adjustment.
        # TODO: rewrite bundle adjuster to choose appropriate weights automatically
        refinement_config = {
            'add_rel_constraints': False,
            'use_arkit_relposes': False,
            'use_arkit_centerdist': True,
            'centerdist_weight': 1e2
        }

        bundle_adjuster = PyBundleAdjuster(ba_options, ba_config, refinement_config=refinement_config)
        bundle_adjuster.set_up_problem(
            reconstruction, 
            loss, 
            timestamp_per_image=timestamp_per_image, 
            arkit_precomputed=arkit_precomputed, 
            detections_per_qr=detections_per_qr,
            image_ids_per_qr=image_ids_per_qr
        )

        solver_options = bundle_adjuster.set_up_solver_options(
            bundle_adjuster.problem, ba_options.solver_options
        )
        solver_options.linear_solver_type = pyceres.LinearSolverType.SPARSE_SCHUR

        initial_loss_breakdown, initial_loss_breakdown_per_image_id = bundle_adjuster.evaluate_loss_breakdown()

        summary = pyceres.SolverSummary()
        pyceres.solve(solver_options, bundle_adjuster.problem, summary)
        logger.info("Solved!")

        final_loss_breakdown, final_loss_breakdown_per_image_id = bundle_adjuster.evaluate_loss_breakdown()

        logger.info("------------")
        logger.info("INITIAL LOSS BREAKDOWN:")
        for category, loss in initial_loss_breakdown.items():
            logger.info(f"{category}: {loss}")
        logger.info("------------")
        logger.info("FINAL LOSS BREAKDOWN:")
        for category, loss in final_loss_breakdown.items():
            logger.info(f"{category}: {loss}")
        logger.info("------------")

        # logger.info("\n".join(summary.FullReport().split(",")))
        logger.info(f"{summary.FullReport()}")

        num_changed_observations = 0
        num_changed_observations += mapper.complete_and_merge_tracks(tri_options)
        num_changed_observations += mapper.filter_points(mapper_options)

        changed = num_changed_observations / num_observations
        logger.info(f'Changed observations: {changed}')

        ba_iterations_remaining -= 1

        # Retriangulate underreconstructed image pairs after first BA success
        if not retriangulated and summary.termination_type == pyceres.TerminationType.CONVERGENCE:
            logger.info('Retriangulating...')
            num_retriangulated = mapper.retriangulate(tri_options)
            logger.info(f'Retriangulated {num_retriangulated} observations')
            retriangulated = True

            # make sure there are at least two more BA iterations after retriangulation
            # (to finish loop closure + filter outliers)
            additional_iterations = max(0, 2 - ba_iterations_remaining)
            num_ba_iterations_total += additional_iterations
            ba_iterations_remaining += additional_iterations


    logger.info('Extracting colors...')
    reconstruction.extract_colors_for_all_images(image_dir)

    mapper.end_reconstruction(False)

    return reconstruction


def triangulate_model(
    sfm_dir: Path,
    reference_model: Path,
    image_dir: Path,
    pairs: Path,
    features: Path,
    matches: Path,
    skip_geometric_verification: bool = False,
    estimate_two_view_geometries: bool = False,
    min_match_score: Optional[float] = None,
    verbose: bool = False,
    mapper_options: Optional[Dict[str, Any]] = None,
    timestamp_per_image: Optional[Dict[str, int]] = None,
    arkit_precomputed=None,
    detections_per_qr=None,
    image_ids_per_qr=None
) -> pycolmap.Reconstruction:
    assert reference_model.exists(), reference_model
    assert features.exists(), features
    assert pairs.exists(), pairs
    assert matches.exists(), matches

    sfm_dir.mkdir(parents=True, exist_ok=True)
    database = sfm_dir / "database.db"
    reference = pycolmap.Reconstruction(reference_model)

    image_ids = create_db_from_model(reference, database)
    import_features(image_ids, database, features)
    import_matches(
        image_ids,
        database,
        pairs,
        matches,
        min_match_score,
        skip_geometric_verification,
    )

    assert skip_geometric_verification and not estimate_two_view_geometries # TODO: support this later as well?

    reconstruction = run_triangulation(
        database, image_dir, reference, mapper_options if mapper_options is not None else {},
        timestamp_per_image, arkit_precomputed, detections_per_qr, image_ids_per_qr
    )
    # Grab logger by name
    logger = logging.getLogger('refine_dataset')
    logger.info(f"Finished the triangulation with statistics: {reconstruction.summary()}")
    return reconstruction


def process_features_and_matching(
    references,
    colmap_rec_path,
    paths,
    logger
):
    """Process feature extraction and matching."""
    # Generate pairs from poses
    logger.info("Generating image pairs from poses")

    use_pairs_from_sequential = True
    if use_pairs_from_sequential:
        pairs_from_sequential.main(
            paths.sfm_pairs, 
            references, 
            features=None,
            window_size=5,
            quadratic_overlap=True,
            use_loop_closure=False,
            retrieval_path=None,
            retrieval_interval=2,
            num_loc=5
        )
    else:
        pairs_from_poses.main(
            colmap_rec_path,  # Input reconstruction path
            paths.sfm_pairs,   # Output pairs file path
            num_matched=20,    # Number of closest images to match
            rotation_threshold=360  # Maximum rotation difference in degrees
        )

    # Feature extraction
    feature_conf = extract_features.confs["superpoint_max"]
    feature_conf["output"] = paths.features
    feature_conf["model"]["max_keypoints"] = 1024
    logger.info(f"Extracting features with config: {feature_conf}")

    extract_features.main(
        feature_conf,
        paths.images,
        paths.sfm_dir,
        feature_path=paths.features,
        as_half=True,
        image_list=references
    )

    # Feature matching
    logger.info("Matching features")
    matcher_conf = match_features.confs["superpoint+lightglue"]
    match_features.main(
        matcher_conf, 
        paths.sfm_pairs, 
        features=paths.features, 
        matches=paths.matches
    )
