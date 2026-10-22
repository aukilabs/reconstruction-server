from pathlib import Path
import argparse
import os
import sys
from utils.data_utils import add_file_handler, get_data_paths, setup_logger, write_fail_reason
from utils.dataset_utils import update_helper
from utils.point_cloud_utils import post_process_ply

def main(args):
    # Create and configure logger
    output_path = args.output_path
    os.makedirs(output_path, exist_ok=True)
    job_root_path = args.data_path.parent.parent
    update_log_file = str(output_path) + "/update_logs"
    logger = setup_logger(
        name="update_refinement", 
        log_file=update_log_file,
        domain_id=args.domain_id, 
        job_id=args.job_id, 
        level=args.log_level
    )
    # The runner uploads job_root/log.txt as the task log.
    add_file_handler(logger, job_root_path / "log.txt")

    try:
        update_refinement(args, job_root_path, logger)
    except Exception as e:
        logger.error(f"Update refinement failed with exception: {e}")
        write_fail_reason(job_root_path, e, logger)
        raise e


def update_refinement(args, job_root_path, logger):
    output_path = args.output_path

    # Find all stitch paths
    _, dataset_paths = get_data_paths(args.data_path, "update_refinement")

    # Sort dataset paths by timestamp (indirectly, since folders are named by timestamp)
    # Starting with oldest scan keeps the origin portal consistent.
    dataset_paths.sort()
    
    if dataset_paths:
        logger.info(f"Found {len(dataset_paths)} dataset paths for stitching:")
        for path in dataset_paths:
            logger.info(f" - {path}")
    else:
        logger.warning("No dataset paths found for stitching. Please check the data directory.")    

    # Perform Update Refinement
    result = update_helper(
        dataset_paths=dataset_paths,
        job_root_path=job_root_path,
        logger_name="update_refinement"
    )

    if not result:
        raise RuntimeError(f"Update refinement failed: no refined scans found in {args.data_path}")

    post_process_ply(output_path, logger=logger)

    logger.info("Update refinement completed successfully")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=Path, default="./local", help="Path to datasets directory")
    parser.add_argument("--output_path", type=Path, default="./merged", help="Path for output files")
    parser.add_argument("--domain_id", type=str, default="")
    parser.add_argument("--job_id", type=str, default="")
    parser.add_argument("--log_level", type=str, default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )
    args = parser.parse_args()
    main(args)