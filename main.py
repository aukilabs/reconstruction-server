from pathlib import Path
import sys
import argparse

from local_main import main as local_main
from global_main import main as global_main
from utils.data_utils import save_failed_manifest_json, setup_logger

def local_main_wrapper(args, logger):
    scans = args.scans
    job_root_path = args.job_root_path
    output_path = args.output_path
    logger.info("--------------------------------")
    logger.info(f"Running local refinement on {len(scans)} scans")
    logger.info(f"Job root path: {job_root_path}")
    logger.info(f"Output path: {output_path}")
    logger.info(f"Scans: {scans}")
    logger.info("--------------------------------")

    for scan in scans:
        logger.info(f"Refining scan {scan}...")
        local_args = argparse.Namespace(
            dataset_path=Path(job_root_path) / 'datasets' / scan,
            output_path=args.output_path,
            every_nth_image=2,
            remove_outputs=False
        )
        local_main(local_args)
        logger.info(f"Done refining scan {scan}")


def global_main_wrapper(args, logger):
    scans = args.scans
    logger.info("--------------------------------")
    logger.info(f"Running global refinement with {len(scans)} scans")

    global_args = argparse.Namespace(
        data_dir=Path(args.job_root_path) / "datasets",
        dataset_group=None,
        all_observations=True,
        all_poses=True,
        use_refined_outputs=True,
        add_3dpoints=True,
        basic_stitch_only=True
    )
    global_main(global_args)
    logger.info("Done with global refinement")
    logger.info("--------------------------------")

def local_and_global_main_wrapper(args, logger):
    local_args = argparse.Namespace(**vars(args))
    local_args.output_path = args.job_root_path / "refined" / "local"
    local_main_wrapper(local_args, logger)
    global_main_wrapper(args, logger)

    """
    # output stitched point cloud
    stitch_args = argparse.Namespace(
        data_dir=Path(args.job_root_path) / "datasets",
        dataset_group=None,
        all_observations=True,
        all_poses=True,
        use_refined_outputs=True,
        add_3dpoints=True,
        basic_stitch_only=True
    )
    global_main(stitch_args)
    """
    
    ply_output_path = Path(args.job_root_path) / "refined" / "global" / "RefinedPointCloud.ply"
    if ply_output_path.exists():
        logger.info(f"Refined point cloud created! {ply_output_path}")
    else:
        logger.info(f"Point cloud wasn't created, expected at: {ply_output_path}")

def main(args, logger):
    args.job_root_path = Path(args.job_root_path)
    args.output_path = Path(args.output_path)

    logger = setup_logger('main', args.job_root_path / 'log.txt')

    # TODO: ignoring the scans parameter from go for now since it's incorrect (fix after redeploy)
    args.scans = []
    for scan in Path(args.job_root_path / "datasets").iterdir():
        if scan.is_dir() or scan.suffix == ".zip":
            args.scans.append(scan.name)

    try:
        if args.mode == "local_refinement":
            if not args.output_path:
                args.output_path = args.job_root_path / "refined" / "local"
            local_main_wrapper(args, logger)
        elif args.mode == "global_refinement":
            if not args.output_path:
                args.output_path = args.job_root_path / "refined" / "global"
            global_main_wrapper(args, logger)
        elif args.mode == "local_and_global_refinement":
            if not args.output_path:
                args.output_path = args.job_root_path / "refined" / "global"
            local_and_global_main_wrapper(args, logger)
    except Exception as e:
        logger.error(f"Refinement failed with exception: {e}")
        manifest_out_path =  args.output_path / "refined_manifest.json"
        logger.error(f"Saving 'failed' manifest to: {manifest_out_path}")
        save_failed_manifest_json(manifest_out_path, str(e))
        raise e

def parse_args():
    parser = argparse.ArgumentParser(description="SfM refinement script")
    parser.add_argument("mode", choices=["local_refinement", "global_refinement", "local_and_global_refinement"], help="Refinement mode")
    parser.add_argument("job_root_path", type=Path, help="Path to the job root (parent of 'datasets' sub-folder with all scans inside)")
    parser.add_argument("output_path", type=Path, help="Path for output")
    parser.add_argument("scans", nargs="+", help="List of scans to process")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    main(args)
