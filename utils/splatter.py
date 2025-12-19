from pathlib import Path
import argparse
import shutil
from subprocess import run

from utils.densification import densify_rec_points, densify_reconstruction
import pycolmap
LICHTFELD_BIN = "D:/gaussian-splatting-cuda/build/LichtFeld-Studio.exe"

def format_path(path: Path):
    return "D:\\rec-server-new\\" + str(path).replace('/', '\\')

def train_splat(colmap_dir: Path, output_dir: Path, images_dir: Path = None):
    
    cmd = [
        LICHTFELD_BIN,
        "-d", format_path(colmap_dir),
        "-o", format_path(output_dir),
        "-r", "2",
        "--images", format_path(images_dir) if images_dir else str(colmap_dir / "images"),
        "--config", f"D:\gaussian-splatting-cuda\parameter\DMT_mcmc_optimization_params.json",
        "--headless"
    ]
    #print(f"Running command: {' '.join(cmd)}")
    #run(cmd)
    return cmd

def preprocess(colmap_dir: Path, processed_dir: Path):
    rec = pycolmap.Reconstruction()
    rec.read(str(colmap_dir))
    print(f"Loaded colmap reconstruction: {rec.summary()}")
    print(rec.summary())
    densified_rec = densify_rec_points(rec)
    densified_rec.write(str(processed_dir))
    print(f"Densified colmap reconstruction written to: {processed_dir}")
    print(densified_rec.summary())

def scan_main(job_root: Path, scan_id: str):
    print(f"Processing scan {scan_id}...")

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
    
    train_cmd = train_splat(dense_dir, output_dir, dense_dir / "images")
    with open(str(job_root / "train_splats.bat"), "a") as f:
        f.write(" ".join(train_cmd) + "\n")

def main(job_root: Path, scan_id: str = None):

    cmd_file = job_root / "train_splats.bat"
    if cmd_file.exists():
        print("Removing old existing train_splats.bat file from job root dir")
        cmd_file.unlink()
    with open(str(cmd_file), "w") as f:
        f.write("")

    if scan_id is None:
        scan_ids = [f.name for f in (job_root / "datasets").iterdir() if f.is_dir()]
        print(f"--scan-ids not provided, processing all {len(scan_ids)} scans in the dataset directory")
        for scan_id in scan_ids:
            scan_main(job_root, scan_id)
    else:
        scan_main(job_root, scan_id)

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--job-root", type=Path, required=True)
    args.add_argument("--scan-id", type=str, default=None)

    args = args.parse_args()

