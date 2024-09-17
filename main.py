from pathlib import Path
import argparse

from utils.refinement_util import refine_dataset


def main(args):
    """
    Main function to run local refinement algorithm.
    """

    refined_rec, unrefined_rec = refine_dataset(
        args.dataset_path, 
        args.output_path,
        args.every_nth_image,
        args.remove_outputs
    )
    
    print(f"Finished local refinment on {args.dataset_path}!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset_path", type=Path, default="./datasets/dmt_scan_2024-06-26_10-26-52"
    )
    parser.add_argument(
        "--output_path", type=Path, default="./outputs"
    )
    parser.add_argument(
        "--every_nth_image", type=int, default=2
    )
    parser.add_argument(
        "--remove_outputs", default=False, action='store_true'
    )
    args = parser.parse_args()

    main(args)
