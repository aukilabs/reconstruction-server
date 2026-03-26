from pathlib import Path
import argparse
from concurrent.futures import ProcessPoolExecutor

from local_main import main as local_main
from global_main import main as global_main
from topology_main import main as topology_main
from occlusion_box import main as occlusion_main
from utils.data_utils import save_failed_manifest_json, setup_logger
from utils.io import load_yaml, save_to_yaml


def occlusion_box_wrapper(pointcloud_path, output_dir, logger):
    """Run occlusion box extraction on the given point cloud.
    
    Args:
        path: Path to the point cloud file
        output_dir: Output directory for results
        logger: Logger instance
    """
    config = load_yaml('config/occlusion_box/default.yaml')
    config.update({
        'path': str(pointcloud_path),
        'output_dir': str(output_dir),
        'opengl': True,  # Point cloud already in OpenGL coordinates
        'display': False
    })

    logger.info(f"Running occlusion box with config contents:")
    for key, value in config.items():
        logger.info(f"{key}: {value}")

    save_to_yaml(config)
    logger.info("Starting occlusion box extraction...")
    occlusion_main(config, logger)
    logger.info("Done with occlusion box extraction!")


def process_local_refinement(args, scan, worker_pool=None):
    """Process local refinement for a single scan.
    
    Args:
        args: Command line arguments
        scan: Name of the scan to process
    """
    local_args = argparse.Namespace(
        dataset_path=Path(args.job_root_path) / 'datasets' / scan,
        output_path=Path(args.output_path) / "local",
        every_nth_image=1,
        remove_outputs=False,
        domain_id=args.domain_id,
        job_id=args.job_id,
        log_level=args.log_level
    )
    return local_main(local_args, worker_pool)


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

    def process_all(pool_executor=None):
        futures = []
        for scan in args.scans:
            logger.info(f"Refining scan {scan}...")
            future = process_local_refinement(args, scan, pool_executor)
            if future:
                logger.info(f"Finished part 1 of local refinement for scan {scan}. Part 2 queued to worker pool.")
                futures.append(future)
                future.add_done_callback(lambda f: logger.info(
                    f"Finished refining scan {scan}" if not f.exception()
                    else f"Failed to refine scan {scan}: {f.exception()}"
                ))
            else:
                logger.info(f"Done refining scan {scan}")
            
            # Abort early if any refinement thread throws an exception
            if futures:
                for f in futures:
                    if f.done() and f.exception():
                        for f2 in futures:
                            if not f2.done():
                                f2.cancel()
                        f.result() # raises the exception (with callstack)

        # Wait for all threads. Does nothing if running without pool.
        for f in futures:
            f.result() # waits, and raises any exception from the worker (with full call stack)

    if args.local_refinement_workers and args.local_refinement_workers >= 1:
        with ProcessPoolExecutor(max_workers=args.local_refinement_workers) as pool_executor:
            process_all(pool_executor)
            pool_executor.shutdown(wait=True)
    else:
        process_all()


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
        output_path=Path(args.output_path) / "global",
        use_refined_outputs=True,
        add_3dpoints=True,
        basic_stitch_only=False,
        ply_downsample=0.03,
        ply_remove_outliers=True,
        domain_id=args.domain_id,
        job_id=args.job_id,
        log_level=args.log_level
    )
    global_main(global_args)
    logger.info("Done with global refinement")
    logger.info("--------------------------------")

    logger.info("Start extracting topology...")

    # TODO: needs some fixing and testing before re-enabling
    #occlusion_box_wrapper(ply_output_path, global_out_folder / "occlusion", logger)

    topology_args = argparse.Namespace(
        input_path=global_args.output_path / "RefinedPointCloud.ply",
        output_dir=global_args.output_path / "topology",
        floor_height=0.0,
        floor_height_threshold=0.35,
        voxel_size=0.05
    )

    topology_main(topology_args, logger)


def local_and_global_main_wrapper(args, logger):
    """Run both local and global refinement processes.
    
    Args:
        args: Command line arguments
        logger: Logger instance
    """
    local_args = argparse.Namespace(**vars(args))
    local_main_wrapper(local_args, logger)
    global_main_wrapper(args, logger)


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


def process_refinement(args, logger):
    """Process refinement based on specified mode.
    
    Args:
        args: Command line arguments
        logger: Logger instance
    """
    # Set default output path if not specified
    if not args.output_path:
        args.output_path = args.job_root_path / "refined"

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
    
    # Write error to fail_reason.txt for the Rust runner to read
    fail_reason_path = args.job_root_path / "fail_reason.txt"
    try:
        fail_reason_path.write_text(str(error), encoding="utf-8")
        logger.info(f"Saved fail reason to: {fail_reason_path}")
    except Exception as write_err:
        logger.warning(f"Failed to write fail_reason.txt: {write_err}")
    
    manifest_out_path = args.job_root_path / "job_manifest.json"
    logger.error(f"Saving 'failed' manifest to: {manifest_out_path}")
    save_failed_manifest_json(manifest_out_path, args.job_root_path, str(error))


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

    # The runner currently derives scans from datasets to avoid stale client-provided scan lists.
    # Get available scans from datasets directory
    if not args.scans:
        logger.warning("--scans not provided, will use all available scans from datasets directory")
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
    parser.add_argument("--local_refinement_workers", type=int, default=0,
        help="Number of workers for parallel processing of scans. 0 to run only on main thread."
    )
    parser.add_argument("--log_level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )

    parser.add_argument("--scans", nargs="+", default=[], help="List of scans to process")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args)
