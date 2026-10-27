"""Local refinement of an Auki SDK capture session into <output>/local/<scan>/sfm."""
from pathlib import Path
import argparse
import json

from reconstruction.entrypoints import _refinement
from reconstruction.pipeline.auki_session.refine import refine_auki_session


def local_auki_session_main(args, layout, logger):
    """Refine the session in args.dataset_path.

    Args:
        args: Command line arguments
        layout: JobLayout of the job
        logger: Logger instance
    """
    logger.info("--------------------------------")
    logger.info("Running local refinement on an Auki SDK capture session")
    logger.info(f"Job root path: {args.job_root_path}")
    logger.info(f"Capture folder: {args.dataset_path}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Session id: {args.session_id or '<auto>'}")
    logger.info(f"Output name: {args.output_name or '<session id>'}")
    logger.info("--------------------------------")

    external_intrinsics = None
    if args.external_intrinsics:
        external_intrinsics = json.loads(Path(args.external_intrinsics).read_text())

    refine_auki_session(
        layout,
        args.dataset_path,
        scan=args.output_name,
        session_id=args.session_id,
        every_nth_image=args.every_nth_image,
        domain_id=args.domain_id,
        job_id=args.job_id,
        log_level=args.log_level,
        log_format=args.log_format,
        qr_size_m=args.qr_size,
        qr_origin_id=args.qr_origin_id,
        qr_ignore_distortion=args.qr_ignore_distortion,
        refine_intrinsics=args.refine_intrinsics,
        external_intrinsics_per_sensor=external_intrinsics,
    )
    logger.info("Done with local refinement of the Auki session")


def parse_args(argv, prog=None):
    parser = argparse.ArgumentParser(prog=prog, description="Local refinement of an Auki SDK capture session")
    parser.add_argument("--domain_id", type=str, default=None, help="Domain ID for logging")
    parser.add_argument("--job_id", type=str, default=None, help="Job ID for logging")
    parser.add_argument("--job_root_path", type=Path, required=True, help="Path to the job root")
    parser.add_argument("--dataset_path", type=Path, required=True,
        help="The capture folder: scan_report.json, registries/ and one folder per session")
    parser.add_argument("--output_path", type=Path, help="Path for output (default: <job root>/refined)")
    parser.add_argument("--output_name", type=str, default=None,
        help="Name of the refined scan under <output>/local (default: the session id)")
    parser.add_argument("--session_id", type=str, default=None,
        help="Session to refine (default: the capture's only session)")
    parser.add_argument("--every_nth_image", type=int, default=1, help="Keep every nth frame of each sensor")
    parser.add_argument("--qr_size", type=float, default=0.10,
        help="QR marker side length in meters; the SDK records none (default: 0.10)")
    parser.add_argument("--qr_origin_id", type=str, default=None,
        help="Marker to re-origin the QR-anchored outputs at (default: the most consistent one)")
    parser.add_argument("--qr_ignore_distortion", action=argparse.BooleanOptionalAction, default=True,
        help="Drop camera distortion when re-estimating QR poses (default: on)")
    parser.add_argument("--refine_intrinsics", action=argparse.BooleanOptionalAction, default=True,
        help="Self-calibrate camera intrinsics in bundle adjustment (default: on)")
    parser.add_argument("--external_intrinsics", type=Path, default=None,
        help='JSON {sensor_id: {"fx", "fy", "cx", "cy", "dist"}} to seed the cameras with')
    parser.add_argument("--log_level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )
    parser.add_argument("--log_format", choices=["text", "json"], default="json", help="Log output format (text or json)")
    args = parser.parse_args(argv)

    if not args.domain_id and not args.job_id:
        inferred_ids = _refinement.infer_domain_and_job_from_job_root(args.job_root_path)
        if inferred_ids:
            args.domain_id, args.job_id = inferred_ids
    if not args.domain_id or not args.job_id:
        parser.error("domain_id and/or job_id were not supplied, and could not be inferred from job_root_path")
    return args


def main(args):
    _refinement.run(args, local_auki_session_main)
