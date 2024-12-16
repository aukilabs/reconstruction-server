from pathlib import Path
import argparse
import os
from utils.data_utils import get_data_paths, setup_logger
from utils.dataset_utils import stitching_helper

def main(args):
    # Create and configure logger
    output_path = args.data_dir.parent / "refined" / "global"
    os.makedirs(output_path, exist_ok=True)
    global_log_file = str(output_path) + "/global_logs"
    logger = setup_logger(name="global_refinement", log_file=global_log_file,
                          domain_id=args.domain_id, job_id=args.job_id, level=args.log_level)

    # Find all stitch paths
    truth_portal_poses, dataset_paths = get_data_paths(args.data_dir, "global_refinement")

    (
    unadjusted_rec, unadjusted_qr_detections, unadjusted_mean_qr_poses,
    adjusted_rec, adjusted_qr_detections, adjusted_mean_qr_poses,
    detections_per_qr, image_ids_per_qr
    ) = stitching_helper(
        dataset_paths, 
        args.dataset_group, 
        args.data_dir, 
        truth_portal_poses, 
        args.all_observations, 
        args.all_poses, 
        args.use_refined_outputs, 
        args.add_3dpoints, 
        args.basic_stitch_only,
        logger_name="global_refinement"
    )
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=Path, 
                        default="./datasets")
    parser.add_argument("--dataset_group", type=str, default="my_domain")
    parser.add_argument("--all_observations", action='store_true', default=False)
    parser.add_argument("--all_poses", action='store_true', default=False)
    parser.add_argument("--use_refined_outputs", action='store_true', default=False)
    parser.add_argument("--add_3dpoints", action='store_true', default=False)
    parser.add_argument("--basic_stitch_only", action='store_true', default=False)
    parser.add_argument("--domain_id", type=str, default="")
    parser.add_argument("--job_id", type=str, default="")
    parser.add_argument("--log_level", type=str, default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )
    args = parser.parse_args()
    main(args)