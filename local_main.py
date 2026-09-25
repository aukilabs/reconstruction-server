from pathlib import Path
import argparse
from utils.mono_depth_mesh import DEFAULT_STRIDE, mono_depth_mesh_enabled
from utils.refinement_util import refine_dataset


def main(args, pool_executor=None):
    """
    Main function to run local refinement algorithm.

    Args:
        args: Namespace containing:
            dataset_path: Path to the input dataset
            output_path: Path for output files
            every_nth_image: Process every nth image
            remove_outputs: Whether to remove existing outputs
            domain_id: Domain identifier
            job_id: Job identifier
            log_level: Logging level
        pool_executor: Optional ThreadPoolExecutor instance for parallel processing
    """

    return refine_dataset(
        args.dataset_path, 
        args.output_path,
        args.every_nth_image,
        args.remove_outputs,
        args.domain_id,
        args.job_id,
        args.log_level,
        args.log_format,
        pool_executor=pool_executor,
        mono_depth_mesh=mono_depth_mesh_enabled(getattr(args, "mono_depth_mesh", False)),
        mono_depth_mesh_stride=args.mono_depth_mesh_stride,
        mono_depth_mesh_process_res=args.mono_depth_mesh_process_res,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset_path", type=Path, default="./datasets/dmt_scan_2024-06-26_10-26-52", help="Path to the input dataset"
    )
    parser.add_argument(
        "--output_path", type=Path, default="./outputs", help="Path for output files"
    )
    parser.add_argument(
        "--every_nth_image", type=int, default=1, help="Process every nth image"
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
    parser.add_argument('--log_format', choices=["text", "json"], default="json", help="Log output format (text or json)")
    parser.add_argument(
        "--mono_depth_mesh",
        action="store_true",
        help="Run colmap-monodepth dense mesh after local SfM (default off; or MONO_DEPTH_MESH=1)",
    )
    parser.add_argument(
        "--mono_depth_mesh_stride",
        type=int,
        default=DEFAULT_STRIDE,
        help="Frame stride for mono depth mesh inference (default: 3)",
    )
    parser.add_argument(
        "--mono_depth_mesh_process_res",
        type=int,
        default=504,
        help="DA3 process_res for mono depth mesh (default: 504)",
    )
    args = parser.parse_args()

    main(args)
