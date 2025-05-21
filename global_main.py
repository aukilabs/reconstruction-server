from pathlib import Path
import argparse
import os
from utils.data_utils import get_data_paths, setup_logger
from utils.dataset_utils import stitching_helper
from utils.point_cloud_utils import filter_ply, downsample_ply_to_max_size, reduce_decimals_ply, draco_compress_ply

def post_process_ply(output_path, logger):
    ply_path = output_path / "RefinedPointCloud.ply"
    filter_ply(ply_path, ply_path, logger=logger)

    # Ensure ply fits in domain data
    logger.info("Downsampling ply if needed to be under 20 MB file size...")
    ply_path_reduced = output_path / "RefinedPointCloudReduced.ply"
    try:
        downsample_ply_to_max_size(ply_path, ply_path_reduced, 20000000, logger=logger)
    except Exception as e:
        logger.error(f"Failed to downsample PLY file: {str(e)}")

    logger.info("Draco compressing the PLY file...")
    try:
        # Must be float to do draco compression, but open3d outputs double precision.
        ply_path_float = output_path / "RefinedPointCloudFloat.ply"
        try:
            reduce_decimals_ply(ply_path, ply_path_float, 3, logger=logger)
        except Exception as e:
            logger.error(f"Failed to reduce decimals in PLY file: {str(e)}")

        draco_compress_ply(ply_path_float, output_path / "RefinedPointCloud.ply.drc", logger=logger)
    except Exception as e:
        logger.error(f"Failed to draco compress the PLY file: {str(e)}")


def main(args):
    # Create and configure logger
    output_path = args.data_dir.parent / "refined" / "global"
    os.makedirs(output_path, exist_ok=True)
    global_log_file = str(output_path) + "/global_logs"
    logger = setup_logger(
        name="global_refinement", 
        log_file=global_log_file,
        domain_id=args.domain_id, 
        job_id=args.job_id, 
        level=args.log_level
    )

    # Find all stitch paths
    truth_portal_poses, dataset_paths = get_data_paths(args.data_dir, "global_refinement")

    # Sort dataset paths by timestamp (indirectly, since folders are named by timestamp)
    # Starting with oldest scan keeps the origin portal consistent.
    dataset_paths.sort()
    
    # Perform stitching
    result = stitching_helper(
        dataset_paths=dataset_paths,
        group_folder=args.data_dir,
        truth_portal_poses=truth_portal_poses,
        use_refined_outputs=args.use_refined_outputs,
        with_3dpoints=args.add_3dpoints,
        basic_stitch_only=args.basic_stitch_only,
        logger_name="global_refinement",
        refix_scale=args.refix_scale
    )

    if result is None:
        logger.error("Stitching failed")
        return
    
    post_process_ply(output_path, logger=logger)

    logger.info("Global refinement completed successfully")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=Path, 
                        default="./datasets")
    parser.add_argument("--use_refined_outputs", action='store_true', default=False, help="Use refined outputs")
    parser.add_argument("--add_3dpoints", action='store_true', default=False, help="Consider whole 3D points")
    parser.add_argument("--basic_stitch_only", action='store_true', default=False, help="Perform basic stitching only")
    parser.add_argument("--ply_downsample", type=float, default=None, help="Downsample the point cloud to given voxel size")
    parser.add_argument("--ply_remove_outliers", action='store_true', default=False, help="Remove outliers from the point cloud")
    parser.add_argument("--domain_id", type=str, default="")
    parser.add_argument("--job_id", type=str, default="")
    parser.add_argument("--refix_scale", action='store_true', default=False, help="Refix scale")
    parser.add_argument("--log_level", type=str, default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )
    args = parser.parse_args()
    main(args)