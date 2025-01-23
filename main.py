from pathlib import Path
import argparse

from local_main import main as local_main
from global_main import main as global_main
from occlusion_box import main as occlusion_box_main
from utils.data_utils import save_failed_manifest_json, setup_logger
from utils.io import load_yaml, save_to_yaml


def occlusion_box_wrapper(path, output_dir, logger):
    """Run occlusion box extraction on the given point cloud.
    
    Args:
        path: Path to the point cloud file
        output_dir: Output directory for results
        logger: Logger instance
    """
    config = load_yaml('config/occlusion_box/default.yaml')
    config.update({
        'path': str(path),
        'output_dir': str(output_dir),
        'opengl': True,  # Point cloud already in OpenGL coordinates
        'display': False
    })

    logger.info(f"Running occlusion box with config contents:")
    for key, value in config.items():
        logger.info(f"{key}: {value}")

    save_to_yaml(config)
    logger.info("Starting occlusion box extraction...")
    occlusion_box_main(config)
    logger.info("Done with occlusion box extraction!")


def process_local_refinement(args, scan):
    """Process local refinement for a single scan.
    
    Args:
        args: Command line arguments
        scan: Name of the scan to process
    """
    local_args = argparse.Namespace(
        dataset_path=Path(args.job_root_path) / 'datasets' / scan,
        output_path=args.output_path,
        every_nth_image=1,
        remove_outputs=False,
        domain_id=args.domain_id,
        job_id=args.job_id,
        log_level=args.log_level
    )
    local_main(local_args)


def local_main_wrapper(args, logger):
    """Run local refinement on all scans.
    
    Args:
        args: Command line arguments
        logger: Logger instance
    """
    logger.info("--------------------------------")
    logger.info(f"Running local refinement on {len(args.scans)} scans")
    logger.info(f"Job root path: {args.job_root_path}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Scans: {args.scans}")
    logger.info("--------------------------------")

    for scan in args.scans:
        logger.info(f"Refining scan {scan}...")
        process_local_refinement(args, scan)
        logger.info(f"Done refining scan {scan}")


def global_main_wrapper(args, logger):
    """Run global refinement process.
    
    Args:
        args: Command line arguments
        logger: Logger instance
    """
    logger.info("--------------------------------")
    logger.info(f"Running global refinement with {len(args.scans)} scans")

    global_args = argparse.Namespace(
        data_dir=Path(args.job_root_path) / "datasets",
        use_refined_outputs=True,
        add_3dpoints=True,
        basic_stitch_only=True,
        domain_id=args.domain_id,
        job_id=args.job_id,
        log_level=args.log_level
    )
    global_main(global_args)
    logger.info("Done with global refinement")
    logger.info("--------------------------------")


def local_and_global_main_wrapper(args, logger):
    """Run both local and global refinement processes.
    
    Args:
        args: Command line arguments
        logger: Logger instance
    """
    local_args = argparse.Namespace(**vars(args))
    local_args.output_path = args.job_root_path / "refined" / "local"
    
    local_main_wrapper(local_args, logger)
    global_main_wrapper(args, logger)

    global_out_folder = args.job_root_path / "refined" / "global"

    ply_output_path = global_out_folder / "RefinedPointCloud.ply"
    if ply_output_path.exists():
        logger.info(f"Refined point cloud created! {ply_output_path}")
    else:
        logger.info(f"Point cloud wasn't created, expected at: {ply_output_path}")

    # TODO: needs some fixing and testing before re-enabling
    #occlusion_box_wrapper(ply_output_path, global_out_folder / "occlusion", logger)


def get_available_scans(datasets_path):
    """Get list of available scans in the datasets directory.
    
    Args:
        datasets_path: Path to datasets directory
        
    Returns:
        List of scan names
    """
    return [
        scan.name for scan in datasets_path.iterdir()
        if scan.is_dir() or scan.suffix == ".zip"
    ]


def process_refinement(args, logger):
    """Process refinement based on specified mode.
    
    Args:
        args: Command line arguments
        logger: Logger instance
    """
    # Set default output path if not specified
    if not args.output_path:
        args.output_path = args.job_root_path / "refined" / (
            "local" if args.mode == "local_refinement" else "global"
        )

    # Map refinement modes to their respective functions
    refinement_functions = {
        "local_refinement": local_main_wrapper,
        "global_refinement": global_main_wrapper,
        "local_and_global_refinement": local_and_global_main_wrapper
    }
    
    refinement_functions[args.mode](args, logger)


def handle_refinement_error(error, args, logger):
    """Handle errors during refinement process.
    
    Args:
        error: The exception that occurred
        args: Command line arguments
        logger: Logger instance
    """
    logger.error(f"Refinement failed with exception: {error}")
    manifest_out_path = args.output_path / "refined_manifest.json"
    logger.error(f"Saving 'failed' manifest to: {manifest_out_path}")
    save_failed_manifest_json(manifest_out_path, args.output_path, str(error))


def main(args):
    """Main entry point for refinement pipeline.
    
    Args:
        args: Command line arguments
    """
    logger = setup_logger(
        name='main', 
        log_file=args.job_root_path / 'log.txt', 
        domain_id=args.domain_id, 
        job_id=args.job_id, 
        level=args.log_level
    )

    # TODO: ignoring the scans parameter from go for now since it's incorrect (fix after redeploy)
    # Get available scans from datasets directory
    args.scans = get_available_scans(args.job_root_path / "datasets")

    try:
        process_refinement(args, logger)
    except Exception as e:
        handle_refinement_error(e, args, logger)
        raise e


def parse_args():
    parser = argparse.ArgumentParser(description="SfM refinement script")
    parser.add_argument("--domain_id", type=str, default="00000000-0000-0000-0000-000000000000", help="Domain ID for logging")
    parser.add_argument("--job_id", type=str, default="job_00000000-0000-0000-0000-000000000000", help="Job ID for logging")
    parser.add_argument("--mode", choices=["local_refinement", "global_refinement", "local_and_global_refinement"], help="Refinement mode")
    parser.add_argument("--job_root_path", type=Path, help="Path to the job root (parent of 'datasets' sub-folder with all scans inside)")
    parser.add_argument("--output_path", type=Path, help="Path for output")
    parser.add_argument("--log_level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )

    parser.add_argument("--scans", nargs="+", default=[], help="List of scans to process")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args)
