from pathlib import Path
import argparse

from utils.data_utils import get_data_paths
from utils.dataset_utils import stitching_helper


def main(args):
    truth_portal_poses, dataset_zip_paths = get_data_paths(args.data_dir)

    (
    unadjusted_rec, unadjusted_qr_detections, unadjusted_mean_qr_poses,
    adjusted_rec, adjusted_qr_detections, adjusted_mean_qr_poses,
    detections_per_qr, image_ids_per_qr
    ) = stitching_helper(
        dataset_zip_paths, 
        args.dataset_group, 
        args.data_dir, 
        truth_portal_poses, 
        args.all_observations, 
        args.all_poses, 
        args.use_refined_outputs, 
        args.add_3dpoints, 
        args.basic_stitch_only
    )
    
    print("All done!")



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

    args = parser.parse_args()
    main(args)