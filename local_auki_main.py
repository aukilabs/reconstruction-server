from pathlib import Path
import argparse
from utils.auki_refinement_util import refine_auki_session
from utils.data_utils import save_failed_manifest_json, setup_logger


def handle_refinement_error(error, job_root_path, logger):
    """Report a failed run the way main.py does, so the Rust runner surfaces the same
    diagnostics for this capability as for ARKit local refinement.

    fail_reason.txt is what runner-reconstruction-local-auki-sdk's python.rs reads to
    turn a non-zero exit into a useful DMS task error instead of a bare exit code.

    Args:
        error: The exception that occurred
        job_root_path: Path to the job root (the runner's workspace root)
        logger: Logger instance
    """
    logger.error(f"Refinement failed with exception: {error}")

    job_root_path = Path(job_root_path)
    fail_reason_path = job_root_path / "fail_reason.txt"
    try:
        fail_reason_path.write_text(str(error), encoding="utf-8")
        logger.info(f"Saved fail reason to: {fail_reason_path}")
    except Exception as write_err:
        logger.warning(f"Failed to write fail_reason.txt: {write_err}")

    manifest_out_path = job_root_path / "job_manifest.json"
    logger.error(f"Saving 'failed' manifest to: {manifest_out_path}")
    save_failed_manifest_json(manifest_out_path, job_root_path, str(error))


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
            output_name: Folder name under output_path for this session's outputs
                (defaults to session_id)
            job_root_path: Optional job root. When set (the compute-node runner always
                sets it), a job-level log is written to <job_root_path>/log.txt and a
                failure is additionally reported via fail_reason.txt +
                job_manifest.json -- same contract main.py implements for ARKit local
                refinement.
        pool_executor: Optional ThreadPoolExecutor instance for parallel processing
    """

    def run():
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
            output_name=getattr(args, "output_name", None),
        )

    job_root_path = getattr(args, "job_root_path", None)
    if job_root_path is None:
        return run()

    job_root_path = Path(job_root_path)
    job_root_path.mkdir(parents=True, exist_ok=True)
    logger = setup_logger(
        name='main',
        log_file=job_root_path / 'log.txt',
        domain_id=args.domain_id,
        job_id=args.job_id,
        level=args.log_level
    )
    logger.info("--------------------------------")
    logger.info("Running local refinement on an Auki SDK capture session")
    logger.info(f"Job root path: {job_root_path}")
    logger.info(f"Capture folder (app root): {args.dataset_path}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Session id: {args.session_id or '<auto>'}")
    logger.info(f"Output name: {getattr(args, 'output_name', None) or '<session id>'}")
    logger.info("--------------------------------")

    try:
        result = run()
    except Exception as e:
        handle_refinement_error(e, job_root_path, logger)
        raise

    logger.info("Done with local refinement of the Auki session")
    return result


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
        "--output_name", type=str, default=None,
        help="Folder name created under output_path for this session's outputs (defaults to the session id)"
    )
    parser.add_argument(
        "--job_root_path", type=Path, default=None,
        help="Job root path. When set, writes a job-level log.txt plus fail_reason.txt/job_manifest.json on failure"
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
