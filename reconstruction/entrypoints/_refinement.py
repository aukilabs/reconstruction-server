"""Plumbing shared by the local and global entry points: arguments, logging, failure handling."""
from pathlib import Path
import argparse
import re

from reconstruction.output.manifest import save_failed_manifest_json
from reconstruction.log import setup_logger
from reconstruction.output.failure import write_fail_reason

def infer_domain_and_job_from_job_root(job_root_path: Path) -> tuple[str, str] | None:
    """If job_root_path ends with <domain_uuid>/job_<uuid>, return (domain_id, job_id)."""

    if job_root_path is None:
        return None



    parts = job_root_path.parts
    if len(parts) < 2:
        return None

    _JOB_ROOT_JOB_SEGMENT_RE = re.compile(
        r"^job_[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )
    _DOMAIN_ID_RE = re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )

    domain_candidate, job_candidate = parts[-2], parts[-1]
    if not _DOMAIN_ID_RE.fullmatch(domain_candidate):
        return None
    if not _JOB_ROOT_JOB_SEGMENT_RE.fullmatch(job_candidate):
        return None

    return domain_candidate, job_candidate


def get_available_scans(datasets_path):
    """Get list of available scans in the datasets directory.

    Args:
        datasets_path: Path to datasets directory

    Returns:
        List of scan names
    """
    return [
        scan.name for scan in datasets_path.iterdir()
        if (scan.is_dir() or scan.suffix == ".zip")
    ]


def handle_refinement_error(error, args, logger):
    """Handle errors during refinement process.

    Args:
        error: The exception that occurred
        args: Command line arguments
        logger: Logger instance
    """
    logger.error(f"Refinement failed with exception: {error}")
    write_fail_reason(args.job_root_path, error, logger)

    manifest_out_path = args.job_root_path / "job_manifest.json"
    logger.error(f"Saving 'failed' manifest to: {manifest_out_path}")
    save_failed_manifest_json(manifest_out_path, args.job_root_path, str(error))


def run(args, refine):
    """Run one refinement stage with the job's logger and failure handling.

    Args:
        args: Command line arguments
        refine: Stage function, called as refine(args, logger)
    """
    logger = setup_logger(
        name='main',
        log_file=args.job_root_path / 'log.txt',
        domain_id=args.domain_id,
        job_id=args.job_id,
        level=args.log_level,
        log_format=args.log_format
    )

    # The runner currently derives scans from datasets to avoid stale client-provided scan lists.
    # Get available scans from datasets directory
    if not args.scans:
        logger.warning("--scans not provided, will use all available scans from datasets directory")
        args.scans = get_available_scans(args.job_root_path / "datasets")

    # Set default output path if not specified
    if not args.output_path:
        args.output_path = args.job_root_path / "refined"

    try:
        refine(args, logger)
    except Exception as e:
        handle_refinement_error(e, args, logger)
        raise e


def parse_args(stage, argv, prog=None):
    """Parse the arguments the local and global runners pass.

    Args:
        stage: "local" or "global"
        argv: Arguments after the subcommand
        prog: Program name for usage messages
    """
    parser = argparse.ArgumentParser(prog=prog, description=f"{stage.capitalize()} refinement")
    parser.add_argument("--domain_id", type=str, default=None, help="Domain ID for logging")
    parser.add_argument("--job_id", type=str, default=None, help="Job ID for logging")
    # The runners still pass --mode; it must match the subcommand.
    parser.add_argument("--mode", choices=[f"{stage}_refinement"], default=f"{stage}_refinement", help="Refinement mode")
    parser.add_argument("--job_root_path", type=Path, help="Path to the job root (parent of 'datasets' sub-folder with all scans inside)")
    parser.add_argument("--output_path", type=Path, help="Path for output")
    parser.add_argument("--local_refinement_workers", type=int, default=0,
        help="Number of workers for parallel processing of scans. 0 to run only on main thread."
    )
    parser.add_argument("--log_level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )
    parser.add_argument("--log_format", choices=["text", "json"], default="json", help="Log output format (text or json)")

    parser.add_argument("--scans", nargs="+", default=[], help="List of scans to process")
    args = parser.parse_args(argv)
    if args.job_root_path and not args.domain_id and not args.job_id:
        inferred_ids = infer_domain_and_job_from_job_root(args.job_root_path)
        if inferred_ids:
            args.domain_id, args.job_id = inferred_ids
            print("Inferred domain_id from job_root_path: ", args.domain_id)
            print("Inferred job_id from job_root_path: ", args.job_id)

    if not args.domain_id or not args.job_id:
        parser.error("domain_id and/or job_id were not supplied, and could not be inferred from job_root_path")
    return args
