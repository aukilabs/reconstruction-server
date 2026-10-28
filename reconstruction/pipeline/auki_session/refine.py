"""Local refinement of an Auki session: the counterpart of pipeline/local/refine.py.

Same output layout as a DMT scan (refined/local/<scan>/ with sfm/, colmap_rec/ and portals.csv),
but the frames come from the session's sensor logs and the reconstruction has a real
multi-sensor rig: the cameras fixed to the robot share one rig and trajectory, and each camera
that moved gets a rig of its own.
"""
from datetime import datetime
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

import numpy as np
import pycolmap

from hloc import extract_features, match_features
from hloc.triangulation import import_features
from hloc.utils.io import get_matches

from reconstruction.io.auki_session import load_auki_session, resolve_auki_session_id
from reconstruction.layout import JobLayout
from reconstruction.log import LOCAL_REFINEMENT, setup_logger
from reconstruction.pipeline.auki_session.qr import detect_qr_codes, process_auki_qr
from reconstruction.pipeline.auki_session.rig import build_auki_rig_and_frames
from reconstruction.pipeline.local.triangulation import run_triangulation


class AukiRefinementPaths(NamedTuple):
    """Like the DMT stage's RefinementPaths (sfm_dir: the final reconstruction and portals.csv;
    colmap_rec: the initial one; log_path: where local_logs goes), plus what this flow writes
    itself because it builds the database and match pairs directly."""
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
    qr_detections: Path
    qr_detection_crops: Path
    log_path: Path


def setup_auki_refinement_paths(layout: JobLayout, capture_path, session_id, scan) -> AukiRefinementPaths:
    """Create the output folders for a session refined under the name `scan`."""
    output = layout.refined_scan(scan)
    sfm_dir = layout.refined_sfm(scan)
    hloc_dir = output / 'hloc'
    paths = AukiRefinementPaths(
        session_root=Path(capture_path),
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
        qr_detections=sfm_dir / 'qr_detections.csv',
        qr_detection_crops=output / 'qr_detection_crops',
        log_path=output,
    )

    for path in [paths.output, paths.images, paths.sfm_dir, paths.colmap_rec, paths.hloc_dir,
                 paths.qr_detection_crops, paths.log_path]:
        path.mkdir(parents=True, exist_ok=True)

    return paths


def _subsample_auki_session(data, every_nth_image, logger):
    """Keep every nth frame of each sensor (frames are already on disk)."""
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
    """Write cameras, rigs, frames and images into a fresh COLMAP database. The DMT stage goes
    through hloc's create_db_from_model, which writes cameras and images only; this session's
    multi-sensor rig needs the rig and frame tables from the start."""
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
    """Match pairs for a multi-camera rig, instead of retrieval or single-camera sequence pairs:
    (a) strided temporal pairs per camera, the within-camera depth signal (the DMT stage's
    strides); (b) same-timestamp pairs between the main rig's cameras, which tie the rig together."""
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

    # (b) same-timestamp pairs between the main rig's cameras, which share one frame
    frames_with_images = {}
    for image in reconstruction.images.values():
        frames_with_images.setdefault(image.frame_id, []).append(image.image_id)
    for frame_id, image_ids_in_frame in frames_with_images.items():
        if reconstruction.frames[frame_id].rig_id != build.main_rig_id or len(image_ids_in_frame) < 2:
            continue
        for i, id_i in enumerate(image_ids_in_frame):
            for id_j in image_ids_in_frame[i + 1:]:
                pairs.add(tuple(sorted((id_i, id_j))))

    with open(paths.pairs, "w") as f:
        for id1, id2 in sorted(pairs):
            f.write(f"{name_by_id[id1]} {name_by_id[id2]}\n")

    logger.info(f"Generated {len(pairs)} match pairs (strided temporal + same-timestamp cross-camera).")


def import_matches(image_ids, database_path, pairs_path, matches_path):
    """hloc's import_matches with skip_geometric_verification, written through pycolmap.

    hloc inserts two_view_geometries rows with the SQL of COLMAP's older schema (10 columns),
    and the database create_colmap_database makes with pycolmap 4.2 has 12. As in hloc, each
    pair's matches are also stored as its verified (CALIBRATED) two-view geometry.
    """
    with open(pairs_path) as f:
        pairs = [line.split() for line in f]

    matched = set()
    with pycolmap.Database.open(database_path) as db, pycolmap.DatabaseTransaction(db):
        for name0, name1 in pairs:
            id0, id1 = image_ids[name0], image_ids[name1]
            if (id0, id1) in matched:
                continue
            matches = np.asarray(get_matches(matches_path, name0, name1)[0], dtype=np.uint32)
            db.write_matches(id0, id1, matches)
            geometry = pycolmap.TwoViewGeometry()
            geometry.config = pycolmap.TwoViewGeometryConfiguration.CALIBRATED
            geometry.inlier_matches = matches
            db.write_two_view_geometry(id0, id1, geometry)
            matched |= {(id0, id1), (id1, id0)}


def extract_and_match_features(build, image_ids, paths, logger):
    """ALIKED features and LightGlue matching with the DMT stage's settings, over
    generate_rig_match_pairs' pairs. The rig already defines the pairing, so there is no
    retrieval (EigenPlaces) step."""
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
    import_matches(image_ids, paths.database, paths.pairs, paths.matches)

    with pycolmap.Database.open(paths.database) as db:
        logger.info(
            f"{db.num_matches()} raw matches across {db.num_verified_image_pairs()} pairs "
            f"-- geometric verification skipped."
        )


def remove_3d_outliers(triangulated, logger, mad_multiplier=5):
    """Delete points farther from the scene centre than median + k * MAD of all distances, on
    top of the point filtering run_triangulation does every bundle adjustment round."""
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


def fixed_intrinsics_sensor_ids(intrinsics_source_per_sensor: Dict[str, str]) -> List[str]:
    """The sensors whose intrinsics bundle adjustment holds fixed whatever refine_intrinsics
    says: every one not seeded from the FOV guess, i.e. recorded with the session or passed
    in as external intrinsics. Those are measured, and self-calibrating them only made things
    worse: on session_20261027_165831 it pulled the D405 arm cameras' fy from 652 to 589-611 px
    (fx stayed near 652) and every QR deviation grew.
    """
    return sorted(sensor_id for sensor_id, source in intrinsics_source_per_sensor.items()
                  if source != "seeded_fov_guess")


def check_rig_offsets_unchanged(build, data, triangulated, logger):
    """Every rigid camera's sensor_from_rig must come out of bundle adjustment unchanged: it is
    never a Ceres parameter, only the frames' rig_from_world is. Raises if the bundle adjuster's
    rig handling regresses."""
    rig_id = build.main_rig_id
    changed = []
    for sensor_id in data.rigid_sensor_ids:
        if sensor_id == data.ref_sensor_id:
            continue
        sensor_t = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=build.camera_id_per_sensor[sensor_id])
        before = build.reconstruction.rigs[rig_id].sensor_from_rig(sensor_t)
        after = triangulated.rigs[rig_id].sensor_from_rig(sensor_t)
        unchanged = (
            np.allclose(before.translation, after.translation)
            and np.allclose(before.rotation.quat, after.rotation.quat)
        )
        logger.info(f"{sensor_id}: sensor_from_rig unchanged by BA = {unchanged}")
        if not unchanged:
            changed.append(sensor_id)
    if changed:
        raise RuntimeError(f"Bundle adjustment changed the rig offset (sensor_from_rig) of {changed}")


def refine_auki_session(
    layout: JobLayout,
    capture_path,
    scan: Optional[str] = None,
    session_id: Optional[str] = None,
    every_nth_image=1,
    domain_id="",
    job_id="",
    log_level="INFO",
    log_format="json",
    qr_size_m: float = 0.10,
    qr_origin_id: Optional[str] = None,
    qr_ignore_distortion: bool = True,
    refine_intrinsics: bool = True,
    external_intrinsics_per_sensor: Optional[dict] = None,
    regroup_static_movable: bool = True,
    static_translation_tol_m: float = 0.02,
    static_rotation_tol_deg: float = 1.0,
):
    """Refine one session of an Auki capture into layout.refined_scan(scan).

    Args:
        layout: JobLayout of the job
        capture_path: The capture folder (the SDK's app root): scan_report.json, registries/
            and one folder per session
        scan: Name of the output folder under refined/local; defaults to the session id. The
            runner passes the name it derives from the input artifact, so the refined scan is
            uploaded under that.
        session_id: Session to refine; found automatically if the capture has only one.
        every_nth_image: Keep every nth frame of each sensor.
        qr_size_m: Marker side length. The SDK records none, so every marker gets this one.
        qr_origin_id: The marker to re-origin qr_anchor_poses.csv and
            point_cloud_qr_anchored.ply at; defaults to the most consistent one.
        qr_ignore_distortion: See qr.reestimate_qr_camera_poses.
        refine_intrinsics: Self-calibrate, in bundle adjustment, the intrinsics and
            distortion of cameras seeded from the FOV guess. Recorded and external intrinsics
            are always held fixed (see fixed_intrinsics_sensor_ids); False holds every
            camera fixed. Self-calibration per segment was unstable for this rig's wide
            surround cameras (one camera's fitted vertical FOV varied by about 30 degrees
            between segments).
        external_intrinsics_per_sensor: {sensor_id: {"fx", "fy", "cx", "cy", "dist"}} to seed
            the cameras with, e.g. recorded intrinsics of the same device from another
            capture; held fixed like recorded ones.
        regroup_static_movable, static_translation_tol_m, static_rotation_tol_deg: see
            io.auki_session.load_auki_session.
    """
    start_time = datetime.now()
    capture_path = Path(capture_path)
    if session_id is None:
        session_id = resolve_auki_session_id(capture_path)
    scan = scan or session_id

    paths = setup_auki_refinement_paths(layout, capture_path, session_id, scan)

    # LOCAL_REFINEMENT, like the DMT stage: run_triangulation logs to it too.
    logger = setup_logger(
        name=LOCAL_REFINEMENT,
        log_file=str(paths.log_path / "local_logs"),
        domain_id=domain_id,
        job_id=job_id,
        dataset_id=scan,
        level=log_level,
        log_format=log_format,
    )
    logger.info(f'Starting local refinement of Auki session {session_id}')

    data = load_auki_session(
        str(capture_path), session_id, paths.images, logger=logger,
        regroup_static_movable=regroup_static_movable,
        static_translation_tol_m=static_translation_tol_m,
        static_rotation_tol_deg=static_rotation_tol_deg,
    )
    logger.info(f"Rig grouping: {len(data.rigid_sensor_ids)} sensor(s) on the main rig "
                f"{data.rigid_sensor_ids} (ref={data.ref_sensor_id}), "
                f"{len(data.movable_sensor_ids)} on per-sensor mini rig(s) {data.movable_sensor_ids}")
    data = _subsample_auki_session(data, every_nth_image, logger)

    build = build_auki_rig_and_frames(data, logger=logger,
                                      external_intrinsics_per_sensor=external_intrinsics_per_sensor)
    logger.info(build.reconstruction.summary())
    if build.dropped_images:
        logger.warning(f"Dropped {build.dropped_images} image(s) with no pose sample within the matching window")

    # The initial reconstruction, seeded from the pose logs
    build.reconstruction.write(paths.colmap_rec)
    build.reconstruction.write(paths.sfm_dir)

    detections_per_qr, image_ids_per_qr, corners_per_qr, portal_sizes = detect_qr_codes(data, build, qr_size_m)
    logger.info(f"Unique QR marker(s) seen: {len(detections_per_qr)}")
    for short_id, dets in detections_per_qr.items():
        logger.info(f"  {short_id}: {len(dets)} detection(s) across the session")

    image_ids = create_colmap_database(build.reconstruction, paths.database, logger)
    extract_and_match_features(build, image_ids, paths, logger)

    logger.info("Start triangulation")
    fixed_sensor_ids = fixed_intrinsics_sensor_ids(build.intrinsics_source_per_sensor)
    logger.info(f"refine_intrinsics={refine_intrinsics} "
                f"(intrinsics source per sensor: {build.intrinsics_source_per_sensor}); "
                f"held fixed: {fixed_sensor_ids if refine_intrinsics else 'all'}")
    triangulated = run_triangulation(
        paths.database,
        paths.images,
        build.reconstruction,
        timestamp_per_image=build.timestamp_per_image,
        arkit_precomputed={},
        # No QR loop closure (the DMT stage has one): the QR poses so far come from seeded
        # intrinsics, and holding them as constraints could fight the rig's real scale.
        # process_auki_qr re-estimates them with the refined intrinsics afterwards.
        filter_spikes=False,
        ba_options_overrides={
            # Seeding alone does not stop bundle adjustment refining away from the seed, so
            # refine_intrinsics=False also has to fix them here.
            "refine_focal_length": refine_intrinsics,
            "refine_principal_point": False,
            "refine_extra_params": refine_intrinsics,
        },
        constant_intrinsics_camera_ids=[build.camera_id_per_sensor[s] for s in fixed_sensor_ids],
        refinement_config_overrides={
            "add_rel_constraints": False,
            "use_arkit_relposes": False,
        },
    )
    logger.info("Finished triangulation")
    logger.info(f'After triangulation, the mean reprojection error is {triangulated.compute_mean_reprojection_error()}')

    remove_3d_outliers(triangulated, logger)
    check_rig_offsets_unchanged(build, data, triangulated, logger)

    triangulated.write(paths.sfm_dir)
    triangulated.export_PLY(paths.point_cloud)

    process_auki_qr(triangulated, image_ids_per_qr, corners_per_qr, portal_sizes, paths, logger,
                    qr_origin_id=qr_origin_id, qr_ignore_distortion=qr_ignore_distortion)

    logger.info(f"Local refinement completed in {datetime.now() - start_time}")
