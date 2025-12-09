from pathlib import Path
import argparse
import shutil
from subprocess import run

from sympy.matrices import dense
from utils.densification import densify_rec_points, densify_reconstruction
import pycolmap
LICHTFELD_BIN = "./lichtfeld"

def train_splat(dataset_dir: Path, output_dir: Path):
    cmd = [
        LICHTFELD_BIN,
        "-d", str(dataset_dir),
        "-o", str(output_dir),
        "--config", "lichtfelt_optimization_params.json",
        "--gut",
        "-r", "4"
    ]
    print(f"Running command: {' '.join(cmd)}")
    run(cmd)

def preprocess(colmap_dir: Path, processed_dir: Path):
    rec = pycolmap.Reconstruction()
    rec.read(str(colmap_dir))
    print(f"Loaded colmap reconstruction: {rec.summary()}")
    print(rec.summary())
    densified_rec = densify_rec_points(rec)
    densified_rec.write(str(processed_dir))
    print(f"Densified colmap reconstruction written to: {processed_dir}")
    print(densified_rec.summary())

def main(job_root: Path, scan_id: str):

    frames_dir = job_root / "datasets" / scan_id / "Frames"
    if not frames_dir.exists():
        raise FileNotFoundError(f"Frames directory not found: {frames_dir}")
    
    colmap_dir = job_root / "refined" / "local" / scan_id / "sfm"
    dense_dir = job_root / "refined" / "local" / scan_id / "dense"

    processed_dir = job_root / "refined" / "local" / scan_id / "processed"
    if not processed_dir.exists():
        processed_dir.mkdir(parents=True, exist_ok=True)
    
    output_dir = job_root / "refined" / "local" / scan_id / "splat"
    shutil.rmtree(dense_dir, ignore_errors=True)
    shutil.rmtree(output_dir, ignore_errors=True)
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    if not dense_dir.exists():
        dense_dir.mkdir(parents=True, exist_ok=True)
    #images_dst = dense_dir / "images"
    #if images_dst.exists():
    #    shutil.rmtree(images_dst, ignore_errors=True)
    #images_dst.mkdir(parents=True, exist_ok=True)

    preprocess(colmap_dir, processed_dir)
    pycolmap.undistort_images(
        output_path=str(dense_dir),
        input_path=str(processed_dir),
        image_path=str(frames_dir)
    )
    #densify_reconstruction(job_root, colmap_dir, dense_dir)
    #train_splat(dense_dir, output_dir)

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--job-root", type=Path, required=True)
    args.add_argument("--scan-id", type=str, default=None)

    args = args.parse_args()

    if args.scan_id is None:
        scan_ids = [f.name for f in (args.job_root / "datasets").iterdir() if f.is_dir()]
        print(f"--scan-ids not provided, processing all {len(scan_ids)} scans in the dataset directory")
        for scan_id in scan_ids:
            print(f"Processing scan {scan_id}...")
            main(args.job_root, scan_id)
    else:
        print(f"Processing scan {args.scan_id}...")
        main(args.job_root, args.scan_id)
