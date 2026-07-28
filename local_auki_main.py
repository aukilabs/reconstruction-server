from pathlib import Path
import argparse
from utils.auki_refinement_util import refine_auki_session


def main(args, pool_executor=None):
    """
    Main function to run local refinement algorithm on an Auki SDK capture session.

    Args:
        args: Namespace containing:
            dataset_path: Path to the Auki capture folder (app root)
            output_path: Path for output files
            session_id: Session id inside dataset_path to refine (auto-resolved if omitted)
            every_nth_image: Process every nth image
            remove_outputs: Whether to remove existing outputs
            domain_id: Domain identifier
            job_id: Job identifier
            log_level: Logging level
            qr_origin_id: Which detected QR marker to re-origin QR-anchored outputs at
                (auto-picks the lowest-deviation marker if omitted)
        pool_executor: Optional ThreadPoolExecutor instance for parallel processing
    """

    return refine_auki_session(
        args.dataset_path,
        args.output_path,
        args.session_id,
        args.every_nth_image,
        args.remove_outputs,
        args.domain_id,
        args.job_id,
        args.log_level,
        pool_executor=pool_executor,
        qr_origin_id=args.qr_origin_id,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset_path", type=Path, default="./datasets/auki_capture_complete", help="Path to the Auki capture folder (app root)"
    )
    parser.add_argument(
        "--output_path", type=Path, default="./outputs", help="Path for output files"
    )
    parser.add_argument(
        "--session_id", type=str, default=None, help="Session id inside dataset_path to refine (auto-resolved if omitted)"
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
    parser.add_argument(
        "--qr_origin_id", type=str, default=None,
        help="Which detected QR marker to re-origin QR-anchored outputs at (auto-picks the lowest-deviation marker if omitted)"
    )
    args = parser.parse_args()

    main(args)
