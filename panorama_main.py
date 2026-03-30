from pathlib import Path
import argparse
from utils.refinement_util import refine_pano_data


def main(args, pool_executor=None):
    """
    Main function to run local refinement algorithm.

    Args:
        args: Namespace containing:
            dataset_path: Path to the input dataset
            output_path: Path for output files
            every_nth_frame: Process every nth frame (each frame is a rig, contains multiple images)
            remove_outputs: Whether to remove existing outputs
            domain_id: Domain identifier
            job_id: Job identifier
            log_level: Logging level
        pool_executor: Optional ThreadPoolExecutor instance for parallel processing
    """

    return refine_pano_data(
        args.dataset_path, 
        args.output_path,
        args.save_images_per_second,
        args.remove_outputs,
        args.domain_id,
        args.job_id,
        args.log_level,
        pool_executor=pool_executor
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset_path", type=Path, default="./datasets/pano_2024-06-26_10-26-52", help="Path to the input dataset"
    )
    parser.add_argument(
        "--output_path", type=Path, default="./outputs", help="Path for output files"
    )
    parser.add_argument(
        "--save_images_per_second", type=int, default=1, help="Save images per second"
    )
    parser.add_argument(
        "--remove_outputs", default=False, action='store_true', help="Remove existing outputs before processing"
    )
    parser.add_argument(
        "--domain_id", type=str, default="", help="Domain identifier"
    )
    parser.add_argument(
        "--job_id", type=str, default="", help="Job identifier"
    )
    parser.add_argument("--log_level", type=str, default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )
    args = parser.parse_args()

    main(args)
