from pathlib import Path
from datetime import datetime
from typing import NamedTuple, Optional
import csv
import shutil

import numpy as np
import pycolmap

from hloc import extract_features, match_features
from hloc.triangulation import import_features, import_matches

from utils.auki_data_utils import (
    load_auki_session,
    build_auki_rig_and_frames,
    detect_qr_codes,
    reestimate_qr_camera_poses,
    convert_colmap_pose_to_qr_origin_auki,
    convert_colmap_points_to_qr_origin_auki,
)
from utils.triangulation import run_triangulation
from utils.data_utils import (
    setup_logger,
    get_world_space_qr_codes,
    mean_pose,
    rectify_portal_pose,
    save_portal_csv,
)


class AukiRefinementPaths(NamedTuple):
    """Container for all paths used in Auki-session refinement. Auki analogue of
    refinement_util.RefinementPaths -- same output/log-path roles (sfm_dir holds the
    final reconstruction + portals.csv, colmap_rec the initial unrefined reconstruction,
    log_path the job log), plus a few paths ARKit refinement has no equivalent for
    (hloc_dir, database, point_cloud) since this flow builds the COLMAP database and
    match pairs itself instead of going through triangulate_model."""
    session_root: Path
    session_id: str
    output: Path
    images: Path
    sfm_dir: Path
    colmap_rec: Path
    hloc_dir: Path
    features: Path
    matches: Path
    database: Path
    pairs: Path
    point_cloud: Path
    qr_anchor_poses: Path
    qr_anchored_point_cloud: Path
    log_path: Path


# colmap-world convention (see utils/auki_data_utils.py: X=up, Y=right, Z=forward) --
# a correctly-oriented floor QR marker's normal points *up* here (pnp-lab/qr-lab
# convention, verified against real grout-line photo evidence), the opposite of the
# ARKit/DMT pipeline's convention rectify_portal_pose's default reference_axis assumes.
_QR_UP_AXIS = np.array([1.0, 0.0, 0.0])


def resolve_auki_session_id(app_root) -> str:
    """
    Auto-discover the session id inside an Auki capture folder: the subfolder that
    actually owns a sensorlogs/ directory (registries/ and scan_report.json live
    alongside sessions, not inside them). Some capture folders hold more than one
    session (confirmed on real sample data) and scan_report.json's own
    clock.session_id field only ever names one of them, so directory scanning is the
    only unambiguous source of truth here -- callers with multiple sessions under one
    app_root must pass session_id explicitly instead of relying on auto-discovery.
    """
    app_root = Path(app_root)
    candidates = sorted(
        p.name for p in app_root.iterdir() if p.is_dir() and (p / "sensorlogs").is_dir()
    )
    if len(candidates) == 1:
        return candidates[0]
    raise ValueError(
        f"Could not auto-resolve a single Auki session under {app_root}: found "
        f"{len(candidates)} session folder(s) {candidates}. Pass session_id explicitly."
    )


def setup_auki_refinement_paths(session_root_path, session_id, output_path) -> AukiRefinementPaths:
    """
    Setup and create necessary directories for Auki-session refinement.

    Args:
        session_root_path: Path to the Auki capture folder (app root)
        session_id: The session id inside session_root_path to refine
        output_path: Base output path

    Returns:
        AukiRefinementPaths object containing all necessary paths
    """
    output = Path(output_path) / session_id
    sfm_dir = output / 'sfm'
    hloc_dir = output / 'hloc'
    paths = AukiRefinementPaths(
        session_root=Path(session_root_path),
        session_id=session_id,
        output=output,
        images=output / 'images',
        sfm_dir=sfm_dir,
        colmap_rec=output / 'colmap_rec',
        hloc_dir=hloc_dir,
        features=hloc_dir / 'features.h5',
        matches=hloc_dir / 'matches.h5',
        database=output / 'database.db',
        pairs=sfm_dir / 'pairs-sfm.txt',
        point_cloud=output / 'point_cloud.ply',
        qr_anchor_poses=sfm_dir / 'qr_anchor_poses.csv',
        qr_anchored_point_cloud=output / 'point_cloud_qr_anchored.ply',
        log_path=output,
    )

    for path in [paths.output, paths.images, paths.sfm_dir, paths.colmap_rec, paths.hloc_dir, paths.log_path]:
        path.mkdir(parents=True, exist_ok=True)

    return paths


def _subsample_auki_session(data, every_nth_image, logger):
    """Auki analogue of process_frames' every_nth_image slicing. Frames are already
    extracted to disk by load_auki_session, so this just subsamples each sensor's
    record list before the rig/reconstruction is built from it."""
    original_count = sum(len(r) for r in data.images_per_sensor.values())
    if every_nth_image > 1:
        data = data._replace(images_per_sensor={
            sensor_id: records[::every_nth_image]
            for sensor_id, records in data.images_per_sensor.items()
        })
    new_count = sum(len(r) for r in data.images_per_sensor.values())
    logger.info(f'{new_count} frames selected, out of, {original_count}')
    return data


def create_colmap_database(reconstruction, database_path, logger):
    """Write cameras/rigs/frames/images directly into a fresh COLMAP database -- the
    modern (post-4.x) schema, which natively persists rig/frame calibration. Unlike
    ARKit's process_features_and_matching (which goes through
    hloc.triangulation.create_db_from_model, cameras+images only -- fine there since
    every rig is trivial, one ref sensor each), this session has a real multi-sensor
    Rig whose rig/frame tables need to exist from the start.
    """
    database_path = Path(database_path)
    if database_path.exists():
        database_path.unlink()

    db = pycolmap.Database.open(database_path)
    for camera in reconstruction.cameras.values():
        db.write_camera(camera, use_camera_id=True)
    for rig in reconstruction.rigs.values():
        db.write_rig(rig, use_rig_id=True)
    for frame in reconstruction.frames.values():
        db.write_frame(frame, use_frame_id=True)
    for image in reconstruction.images.values():
        db.write_image(image, use_image_id=True)
    db.close()

    image_ids = {image.name: image.image_id for image in reconstruction.images.values()}
    logger.info(
        f"Database created with {len(reconstruction.cameras)} cameras, {len(reconstruction.rigs)} rigs, "
        f"{len(reconstruction.frames)} frames, {len(image_ids)} images."
    )
    return image_ids


def generate_rig_match_pairs(build, image_ids, paths, logger):
    """Auki analogue of ARKit's pairs_from_sequential/pairs_from_poses step, but with an
    explicit pairing strategy suited to a real multi-camera rig instead of retrieval or
    single-camera-sequence pairs: (a) per-camera strided temporal pairs -- the
    within-camera depth signal, same stride list as production's pipeline; (b)
    same-timestamp cross-camera pairs among the rigidly-mounted cameras (all sharing a
    trajectory Frame) -- the signal that actually ties the rig together."""
    STRIDE_STEPS = [3, 5, 8, 12, 17, 23, 30, 40]
    reconstruction = build.reconstruction

    images_by_sensor = {}
    for (sensor_id, timestamp_ns), image_id in build.image_id_per_record.items():
        images_by_sensor.setdefault(sensor_id, []).append((timestamp_ns, image_id))
    for sensor_id in images_by_sensor:
        images_by_sensor[sensor_id].sort()

    name_by_id = {v: k for k, v in image_ids.items()}
    pairs = set()

    # (a) per-camera strided temporal pairs
    for sensor_id, ordered in images_by_sensor.items():
        ids = [image_id for _, image_id in ordered]
        n = len(ids)
        for i, id_i in enumerate(ids):
            for step in STRIDE_STEPS:
                j = i + step
                if j >= n:
                    continue
                pairs.add(tuple(sorted((id_i, ids[j]))))

    # (b) same-timestamp cross-camera pairs (main rig only -- the rigidly-mounted cameras)
    frames_with_images = {}
    for image in reconstruction.images.values():
        frames_with_images.setdefault(image.frame_id, []).append(image.image_id)
    for frame_id, image_ids_in_frame in frames_with_images.items():
        if reconstruction.frames[frame_id].rig_id != 1 or len(image_ids_in_frame) < 2:
            continue
        for i, id_i in enumerate(image_ids_in_frame):
            for id_j in image_ids_in_frame[i + 1:]:
                pairs.add(tuple(sorted((id_i, id_j))))

    with open(paths.pairs, "w") as f:
        for id1, id2 in sorted(pairs):
            f.write(f"{name_by_id[id1]} {name_by_id[id2]}\n")

    logger.info(f"Generated {len(pairs)} match pairs (strided temporal + same-timestamp cross-camera).")


def extract_and_match_features(build, image_ids, paths, logger):
    """Feature extraction + matching for the multi-sensor rig. Same ALIKED/LightGlue
    hloc confs as ARKit's process_features_and_matching, applied over the explicit rig
    pairing from generate_rig_match_pairs instead of pose/retrieval-based pairs -- this
    rig's cameras already give a well-defined pairing structure, so no loop-closure
    retrieval step (EigenPlaces global features) is needed here."""
    image_names = list(image_ids.keys())

    feature_conf = extract_features.confs["aliked-n16"]
    feature_conf["model"]["max_num_keypoints"] = 1024
    feature_conf["model"]["detection_threshold"] = 0.3
    feature_conf["model"]["nms_radius"] = 4
    feature_conf["preprocessing"]["resize_max"] = 1024
    logger.info(f"Extracting features with config: {feature_conf}")

    extract_features.main(
        feature_conf,
        paths.images,
        paths.hloc_dir,
        feature_path=paths.features,
        as_half=True,
        image_list=image_names,
    )
    import_features(image_ids, paths.database, paths.features)

    generate_rig_match_pairs(build, image_ids, paths, logger)

    matcher_conf = match_features.confs["aliked+lightglue"]
    matcher_conf["model"]["compile_network"] = True
    logger.info(f"Matching features with config: {matcher_conf}")

    match_features.main(
        matcher_conf,
        paths.pairs,
        features=paths.features,
        matches=paths.matches,
    )
    import_matches(image_ids, paths.database, paths.pairs, paths.matches, skip_geometric_verification=True)

    with pycolmap.Database.open(paths.database) as db:
        logger.info(
            f"{db.num_matches()} raw matches across {db.num_verified_image_pairs()} pairs "
            f"-- geometric verification skipped."
        )


def remove_3d_outliers(triangulated, logger, mad_multiplier=5):
    """Robust 3D outlier cleanup (median + k*MAD, k=5) -- same idiom as the DMT
    reconstruction notebooks. run_triangulation() already ran its own internal
    mapper.filter_points each BA round; this is an additional, purely-3D-distance
    sanity pass on top."""
    before = triangulated.num_points3D()
    camera_centers = np.array(
        [img.cam_from_world().inverse().translation for img in triangulated.images.values()]
    )
    scene_center = camera_centers.mean(axis=0)
    point_distances = {pid: np.linalg.norm(p.xyz - scene_center) for pid, p in triangulated.points3D.items()}
    dist_values = np.array(list(point_distances.values()))
    median_dist = np.median(dist_values)
    mad_dist = np.median(np.abs(dist_values - median_dist))
    distance_threshold = median_dist + mad_multiplier * mad_dist

    outlier_ids = [pid for pid, d in point_distances.items() if d > distance_threshold]
    for pid in outlier_ids:
        triangulated.delete_point3D(pid)

    logger.info(
        f"Removed {len(outlier_ids)} of {before} points beyond {distance_threshold:.2f}m from the scene "
        f"center (median={median_dist:.2f}m, MAD={mad_dist:.2f}m, k={mad_multiplier})"
    )
    logger.info(
        f"Final: {triangulated.num_points3D()} points, "
        f"mean reprojection error {triangulated.compute_mean_reprojection_error():.3f}px"
    )


def check_rig_offsets_unchanged(build, data, triangulated, logger):
    """Regression check specific to a real multi-sensor rig: every rigid camera's
    sensor_from_rig extrinsic must be bit-for-bit unchanged by bundle adjustment, since
    it is never a ceres parameter -- only the shared rig_from_world trajectory pose is
    refined. Raises if PyBundleAdjuster's rig-aware RigReprojErrorCost branch regresses."""
    main_rig_offsets_ok = True
    for sensor_id in data.rigid_sensor_ids:
        if sensor_id == data.ref_sensor_id:
            continue
        camera_id = build.camera_id_per_sensor[sensor_id]
        sensor_t = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=camera_id)
        before = build.reconstruction.rigs[1].sensor_from_rig(sensor_t)
        after = triangulated.rigs[1].sensor_from_rig(sensor_t)
        matches = (
            np.allclose(before.translation, after.translation)
            and np.allclose(before.rotation.quat, after.rotation.quat)
        )
        main_rig_offsets_ok &= matches
        logger.info(f"{sensor_id}: sensor_from_rig unchanged by BA = {matches}")
    assert main_rig_offsets_ok, "A rigid camera's sensor_from_rig changed during BA -- refine-rig-only is broken"


def export_qr_anchored_outputs(triangulated, qr_mean_poses, deviations, paths, logger, qr_origin_id=None):
    """Re-origin the reconstruction's point cloud at one QR marker and re-express it in
    Auki's own world convention (forward, up, right) -- what an Auki AR client anchored
    to that physical marker needs, as opposed to portals.csv's colmap-world poses (this
    pipeline's own internal SfM convention, meaningless to an AR consumer). See
    utils/auki_data_utils.py's convert_colmap_pose_to_qr_origin_auki docstring for an
    open question about the Auki axis relabeling this depends on.

    Writes qr_anchor_poses.csv (every surviving marker's pose relative to the origin
    marker, Auki convention) and point_cloud_qr_anchored.ply.

    qr_origin_id: which marker to re-origin at. Defaults to the marker with the lowest
    cross-detection deviation (the most spatially consistent, hence most trustworthy,
    pose) -- callers that know in advance which physical marker their AR client will
    scan first should pass that marker's id explicitly instead.
    """
    if not qr_mean_poses:
        logger.warning("No QR markers survived re-estimation -- skipping QR-anchored export")
        return

    if qr_origin_id is None:
        qr_origin_id = min(deviations, key=deviations.get)
        logger.info(f"Re-origining at QR {qr_origin_id} (lowest cross-detection deviation: "
                    f"{deviations[qr_origin_id]:.5f}m)")
    else:
        logger.info(f"Re-origining at QR {qr_origin_id} (explicitly requested)")

    qr0_from_world = qr_mean_poses[qr_origin_id].inverse()

    with open(paths.qr_anchor_poses, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for qr_id, pose in qr_mean_poses.items():
            anchored_pose = convert_colmap_pose_to_qr_origin_auki(pose, qr0_from_world)
            pos, quat = anchored_pose.translation, anchored_pose.rotation.quat
            writer.writerow([qr_id, qr_id == qr_origin_id, *pos, *quat])
    logger.info(f"Wrote {paths.qr_anchor_poses}")

    points_xyz = np.array([p.xyz for p in triangulated.points3D.values()])
    points_color = np.array([p.color for p in triangulated.points3D.values()])
    anchored_rec = pycolmap.Reconstruction()
    if len(points_xyz):
        anchored_points = convert_colmap_points_to_qr_origin_auki(points_xyz, qr0_from_world)
        for xyz, color in zip(anchored_points, points_color):
            anchored_rec.add_point3D(xyz, pycolmap.Track(), color)
    anchored_rec.export_PLY(str(paths.qr_anchored_point_cloud))
    logger.info(f"Wrote {paths.qr_anchored_point_cloud}")


def process_auki_qr(triangulated, image_ids_per_qr, corners_per_qr, portal_sizes, paths, logger, qr_origin_id=None):
    """Auki analogue of refinement_util.process_QR: re-estimate QR poses against the
    refined, self-calibrated intrinsics (Step 3's corner pixels don't depend on
    intrinsics, only the PnP solve does), then export world-space portal poses to
    sfm_dir/portals.csv. Unlike process_QR, rectify_portal_pose is called with
    reference_axis=_QR_UP_AXIS -- see that constant's docstring for why.

    Also exports a QR-anchored point cloud + per-marker anchor poses -- see
    export_qr_anchored_outputs.
    """
    refined_detections_per_qr, refined_image_ids_per_qr, refined_corners_per_qr = reestimate_qr_camera_poses(
        triangulated, image_ids_per_qr, corners_per_qr, portal_sizes
    )

    logger.info("Now save adjusted QR code poses")
    qr_world_detections = get_world_space_qr_codes(
        triangulated, refined_detections_per_qr, refined_image_ids_per_qr
    )
    qr_mean_poses = {
        qr_id: rectify_portal_pose(mean_pose(poses), reference_axis=_QR_UP_AXIS)
        for qr_id, poses in qr_world_detections.items() if poses
    }
    deviations = {}
    for qr_id, pose in qr_mean_poses.items():
        deviations[qr_id] = np.mean(np.std([det.translation for det in qr_world_detections[qr_id]], axis=0))
        logger.info(f'QR code id: {qr_id}, pose translation {pose.translation}, deviation: {deviations[qr_id]:.5f}')

    qr_world_detections = {
        qr_id: [rectify_portal_pose(p, reference_axis=_QR_UP_AXIS) for p in poses]
        for qr_id, poses in qr_world_detections.items()
    }

    stitched_qr_csv_path = paths.sfm_dir / "portals.csv"
    save_portal_csv(
        qr_world_detections, stitched_qr_csv_path, refined_image_ids_per_qr, portal_sizes, refined_corners_per_qr
    )

    export_qr_anchored_outputs(triangulated, qr_mean_poses, deviations, paths, logger, qr_origin_id=qr_origin_id)


def refine_auki_session_part_two(
    paths,
    build,
    data,
    image_ids_per_qr,
    corners_per_qr,
    portal_sizes,
    logger,
    remove_outputs,
    start_time,
    qr_origin_id=None,
):
    logger.info("Start triangulation")
    triangulated = run_triangulation(
        paths.database,
        paths.images,
        build.reconstruction,
        timestamp_per_image=build.timestamp_per_image,
        arkit_precomputed={},
        # QR loop closure is skipped entirely here (unlike refine_dataset_part_two):
        # Step 3's QR poses were estimated using seeded-FOV intrinsics, since no
        # calibration is recorded anywhere for this capture. Feeding an
        # intrinsics-dependent QR pose into BA as a hard constraint risks fighting the
        # rig's real, registry-anchored scale with an assumption we don't trust yet --
        # process_auki_qr re-estimates QR poses afterwards using self-calibrated intrinsics.
        filter_spikes=False,
        ba_options_overrides={
            "refine_focal_length": True,
            "refine_principal_point": False,
            "refine_extra_params": True,
        },
        refinement_config_overrides={
            "add_rel_constraints": False,
            "use_arkit_relposes": False,
        },
    )
    logger.info("Finished triangulation")
    reproj_error = triangulated.compute_mean_reprojection_error()
    logger.info(f'After triangulation, the mean reprojection error is {reproj_error}')

    remove_3d_outliers(triangulated, logger)
    check_rig_offsets_unchanged(build, data, triangulated, logger)

    triangulated.write(paths.sfm_dir)
    triangulated.export_PLY(paths.point_cloud)

    # Process QR codes
    process_auki_qr(triangulated, image_ids_per_qr, corners_per_qr, portal_sizes, paths, logger,
                     qr_origin_id=qr_origin_id)

    if remove_outputs:
        logger.info('Remove output directory')
        shutil.rmtree(paths.output)

    duration = datetime.now() - start_time
    logger.info(f"Local refinement completed in {duration}")
    logger.info('========================================================================')
    logger.info('')
    logger.info('========================================================================')


def refine_auki_session(
    session_path,
    output_path,
    session_id: Optional[str] = None,
    every_nth_image=1,
    remove_outputs=False,
    domain_id="",
    job_id="",
    log_level="INFO",
    pool_executor=None,
    qr_origin_id: Optional[str] = None,
):
    """
    Refine an Auki SDK multi-sensor rig capture session using Structure from Motion
    techniques. Auki analogue of refinement_util.refine_dataset: same output layout
    (sfm/ with the final reconstruction + portals.csv, colmap_rec/ with the initial
    unrefined reconstruction, a log file under the session's output folder) and the
    same Future/None return contract -- but a different pipeline flow. Frames come from
    an Auki registry + per-sensor logs (not an ARKit scan folder / Frames.mp4), and a
    real multi-sensor pycolmap.Rig is built (rigidly-mounted cameras sharing one Rig +
    trajectory, movable cameras each their own mini-rig) instead of production's
    existing one-trivial-rig-per-image ARKit pattern.

    Args:
        session_path: Path to the Auki capture folder (app root) -- the folder
            containing scan_report.json, registries/, and the session's own subfolder
            with sensorlogs/poselogs (e.g. /data/auki-sessions/auki_capture_complete)
        output_path: Path for output files
        session_id: Session id inside session_path to refine. Auto-resolved when the
            capture folder holds exactly one session; required if it holds more than one.
        every_nth_image: Process every nth image, per sensor
        remove_outputs: Whether to remove existing outputs
        domain_id: Domain identifier
        job_id: Job identifier
        log_level: Logging level
        pool_executor: ThreadPoolExecutor instance for parallel processing
        qr_origin_id: Which detected QR marker to re-origin the QR-anchored outputs
            (qr_anchor_poses.csv, point_cloud_qr_anchored.ply) at. Defaults to the
            marker with the lowest cross-detection deviation; pass a specific marker's
            short id if the AR client is known to scan a particular physical marker first.
    Returns:
        Future object if pool_executor is provided, otherwise None
    """
    start_time = datetime.now()
    session_path = Path(session_path)

    if session_id is None:
        session_id = resolve_auki_session_id(session_path)

    # Setup paths and logging
    paths = setup_auki_refinement_paths(session_path, session_id, output_path)

    # Setup Logging (same logger name as refine_dataset -- utils/triangulation.py's
    # run_triangulation grabs it by that hardcoded name, so its own log lines land in
    # this session's log file too)
    logger = setup_logger(
        name="refine_dataset",
        log_file=str(paths.log_path / "local_logs"),
        domain_id=domain_id,
        job_id=job_id,
        dataset_id=session_id,
        level=log_level
    )
    logger.info(f'Starting local refinement of Auki session {session_id}')

    # Load the Auki session's registry + sensor/pose logs, extract every camera frame
    data = load_auki_session(str(session_path), session_id, paths.images, logger=logger)
    data = _subsample_auki_session(data, every_nth_image, logger)

    # Build the pycolmap.Reconstruction (multi-sensor rig + trajectory frames)
    build = build_auki_rig_and_frames(data, logger=logger)
    logger.info(build.reconstruction.summary())
    if build.dropped_images:
        logger.warning(f"Dropped {build.dropped_images} image(s) with no pose sample within the matching window")

    # Save initial (registry-seeded) reconstruction
    build.reconstruction.write(paths.colmap_rec)
    build.reconstruction.write(paths.sfm_dir)

    # Detect QR codes for portal registration -- independent of the rig/BA pipeline below
    detections_per_qr, image_ids_per_qr, corners_per_qr, portal_sizes, _ = detect_qr_codes(data, build)
    logger.info(f"Unique QR marker(s) seen: {len(detections_per_qr)}")
    for short_id, dets in detections_per_qr.items():
        logger.info(f"  {short_id}: {len(dets)} detection(s) across the session")

    # Create the COLMAP database and process features and matching
    image_ids = create_colmap_database(build.reconstruction, paths.database, logger)
    extract_and_match_features(build, image_ids, paths, logger)

    if pool_executor:
        future = pool_executor.submit(
            refine_auki_session_part_two,
            paths,
            build,
            data,
            image_ids_per_qr,
            corners_per_qr,
            portal_sizes,
            logger,
            remove_outputs,
            start_time,
            qr_origin_id
        )
        return future
    else:
        refine_auki_session_part_two(
            paths,
            build,
            data,
            image_ids_per_qr,
            corners_per_qr,
            portal_sizes,
            logger,
            remove_outputs,
            start_time,
            qr_origin_id
        )
        return None
