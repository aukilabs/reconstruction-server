from pathlib import Path
import argparse
import os
from utils.data_utils import get_data_paths, mean_pose, save_manifest_json, setup_logger
from utils.point_cloud_utils import post_process_ply
from utils.scan_alignment import align_scans, merge_aligned_scans, refine_alignment, print_alignment_comparison, AlignedScans
from utils.io import read_portal_csv
from utils.merge_features import merge_features_h5
import logging
from typing import Dict
import pycolmap


def collect_portal_sizes(scan_ids, job_root_path, logger):
    portal_sizes = {}

    for scan_id in scan_ids:
        portals_csv_path = job_root_path / "refined" / "local" / scan_id / "sfm" / "portals.csv"
        if not portals_csv_path.exists():
            continue

        try:
            for portal in read_portal_csv(portals_csv_path):
                if portal.short_id not in portal_sizes:
                    portal_sizes[portal.short_id] = portal.size
        except Exception as e:
            logger.warning(f"Failed to read portal sizes from {portals_csv_path}: {e}")

    return portal_sizes


def get_mean_portal_poses(aligned_scans: AlignedScans, logger: logging.Logger) -> Dict[str, pycolmap.Rigid3d]:
    """
    Combines all portal detections across all aligned scans into one pose per portal.

    Args:
        portal_detections_per_scan: scan_id -> (qr_id -> poses[])
    Returns:
        Dict[str, pycolmap.Rigid3d]: qr_id -> mean pose
    """
    poses_by_qr = {}

    if not aligned_scans.aligned_portal_detections:
        logger.warning("[get_mean_portal_poses] WARNING: No portal detections in aligned_scans. Returning empty dict.")
        return {}

    for scan_portals in aligned_scans.aligned_portal_detections.values():
        for qr_id, poses in scan_portals.items():
            if qr_id not in poses_by_qr:
                poses_by_qr[qr_id] = []
            poses_by_qr[qr_id].extend(poses)

    return {
        qr_id: mean_pose(poses)
        for qr_id, poses in poses_by_qr.items()
        if poses
    }


def main(args):
    # Create and configure logger
    output_path = args.output_path
    os.makedirs(output_path, exist_ok=True)
    global_log_file = str(output_path) + "/global_logs"
    logger = setup_logger(
        name="global_refinement", 
        log_file=global_log_file,
        domain_id=args.domain_id, 
        job_id=args.job_id, 
        level=args.log_level,
        log_format=args.log_format
    )

    # Find all stitch paths
    truth_portal_poses, dataset_paths = get_data_paths(args.data_dir, "global_refinement")

    # Sort dataset paths by timestamp (indirectly, since folders are named by timestamp)
    # Starting with oldest scan keeps the origin portal consistent.
    dataset_paths.sort()
    
    # Perform stitching
    
    scan_ids = [dataset_path.name for dataset_path in dataset_paths]
    job_root_path = args.data_dir.parent
    aligned_scans = align_scans(
        scan_ids,
        job_root_path,
        logger=logger
    )
    refined_aligned_scans = refine_alignment(
        aligned_scans,
        job_root_path,
        logger=logger,
    )

    print_alignment_comparison(
        aligned_scans,
        refined_aligned_scans,
        logger=logger
    )
    
    combined_rec = merge_aligned_scans(
        refined_aligned_scans,
        job_root_path,
        logger=logger
    )

    refined_portal_poses = get_mean_portal_poses(refined_aligned_scans, logger)

    logger.debug("Aligned portal poses: ")
    aligned_portal_poses = get_mean_portal_poses(aligned_scans, logger)
    for qr_id in aligned_portal_poses.keys():
        basic_pose = aligned_portal_poses[qr_id]
        refined_pose = refined_portal_poses[qr_id]
        logger.debug(f"Basic pose for QR {qr_id}: R={basic_pose.rotation.matrix()}, t={basic_pose.translation}")
        logger.debug(f"Refined pose for QR {qr_id}: R={refined_pose.rotation.matrix()}, t={refined_pose.translation}")
    
    portal_sizes = collect_portal_sizes(refined_aligned_scans.scan_ids, job_root_path, logger)

    manifest_path = output_path / "refined_manifest.json"
    save_manifest_json(
        refined_portal_poses,
        manifest_path,
        job_root_path,
        job_status="refined",
        job_progress=100,
        portal_sizes=portal_sizes,
    )

    sfm_dir = output_path / "refined_sfm_combined"
    os.makedirs(sfm_dir, exist_ok=True)
    combined_rec.write(sfm_dir)

    # Merge per-scan feature files for localization use
    scan_feature_paths = [
        job_root_path / "refined" / "local" / scan_id / "sfm" / "features.h5"
        for scan_id in refined_aligned_scans.scan_ids
    ]
    merged_features_path = sfm_dir / "features.h5"
    merge_features_h5(scan_feature_paths, merged_features_path)
    
    logger.info(f"Exporting colmap points to PLY")
    obs = pycolmap.ObservationManager(combined_rec)
    filtered_count = obs.filter_all_points3D(max_reproj_error=4.0, min_tri_angle=2.0)
    logger.info(f"Filtered {filtered_count} points with large reprojection error or low triangulation angle.")
    logger.info(f"Filtered reconstruction for PLY export: {combined_rec}")
    ply_path = output_path / "RefinedPointCloud.ply"
    combined_rec.export_PLY(ply_path) # Outputs binary PLY in openCV coords. We convert it to OpenGL in the post_process_ply
    logger.info(f"PLY exported -> {ply_path}")

    post_process_ply(output_path, logger=logger)

    logger.info("Global refinement completed successfully")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=Path, 
                        default="./datasets")
    parser.add_argument("--output_path", type=Path, default="./refined/global")
    parser.add_argument("--use_refined_outputs", action='store_true', default=False, help="Use refined outputs")
    parser.add_argument("--add_3dpoints", action='store_true', default=False, help="Consider whole 3D points")
    parser.add_argument("--basic_stitch_only", action='store_true', default=False, help="Perform basic stitching only")
    parser.add_argument("--ply_downsample", type=float, default=None, help="Downsample the point cloud to given voxel size")
    parser.add_argument("--ply_remove_outliers", action='store_true', default=False, help="Remove outliers from the point cloud")
    parser.add_argument("--domain_id", type=str, default="")
    parser.add_argument("--job_id", type=str, default="")
    parser.add_argument("--log_level", type=str, default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )
    args = parser.parse_args()
    main(args)