#!/usr/bin/env python3
"""Holdout validation for single-image localization.

Takes a completed local refinement output (reconstruction + features.h5),
holds out a fraction of images, and localizes them against the remainder.
Reports pose error statistics.

Usage:
    python tests/test_localize_holdout.py \
        --scan_sfm_dir refined/local/<scan_id>/sfm \
        --image_dir datasets/<scan_id>/Frames/ \
        --holdout_fraction 0.2 \
        --output_dir tests/localize_holdout_results/

Test protocol -- run three times:
    1. Easy:      --noise_m 0.0   --noise_deg 0.0   (pipeline sanity)
    2. Realistic: --noise_m 0.5   --noise_deg 5.0   (ARKit-level drift)
    3. Stress:    --noise_m 2.0   --noise_deg 15.0   (graceful degradation)
"""

from pathlib import Path
import argparse
import json
import logging
import sys
import numpy as np
import pycolmap
from copy import deepcopy
from scipy.spatial.transform import Rotation as ScipyRotation

# Add project root so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))

from localize_image import SingleImageLocalizer

logger = logging.getLogger("holdout_test")


def pose_error(
    est_cam_from_world: pycolmap.Rigid3d,
    gt_cam_from_world: pycolmap.Rigid3d,
):
    """Compute position error (m) and rotation error (deg) between two poses."""
    est_world_from_cam = est_cam_from_world.inverse()
    gt_world_from_cam = gt_cam_from_world.inverse()

    # Position error
    pos_err = np.linalg.norm(
        est_world_from_cam.translation - gt_world_from_cam.translation
    )

    # Rotation error: angle of relative rotation
    R_rel = (
        est_cam_from_world.rotation.matrix()
        @ gt_cam_from_world.rotation.matrix().T
    )
    cos_angle = np.clip((np.trace(R_rel) - 1.0) / 2.0, -1.0, 1.0)
    rot_err_deg = np.degrees(np.arccos(cos_angle))

    return pos_err, rot_err_deg


def build_holdout_reconstruction(
    full_rec: pycolmap.Reconstruction,
    holdout_image_ids: set,
) -> pycolmap.Reconstruction:
    """Create a copy of the reconstruction with held-out images deregistered.

    Removes the held-out images and any 3D points that lose sufficient track
    support from the retained images.
    """
    db_rec = deepcopy(full_rec)

    # Deregister held-out images
    for img_id in holdout_image_ids:
        if img_id in db_rec.images:
            db_rec.deregister_image(img_id)

    # Clean up 3D points that lost track support
    point_ids_to_delete = []
    for p3d_id, p3d in db_rec.points3D.items():
        surviving = [
            elem for elem in p3d.track.elements
            if elem.image_id not in holdout_image_ids
        ]
        if len(surviving) < 2:
            point_ids_to_delete.append(p3d_id)

    for p3d_id in point_ids_to_delete:
        db_rec.delete_point3D(p3d_id)

    logger.info(
        f"Holdout reconstruction: {db_rec.num_reg_images()} images "
        f"(removed {len(holdout_image_ids)}), "
        f"{len(db_rec.points3D)} 3D points "
        f"(removed {len(point_ids_to_delete)})"
    )
    return db_rec


def select_holdout_images(
    rec: pycolmap.Reconstruction,
    fraction: float = 0.2,
) -> set:
    """Select held-out image IDs.

    Takes every Nth image (where N ~ 1/fraction) to ensure spatial spread.
    Never holds out the first or last two images (they anchor the BA gauge).
    """
    sorted_ids = sorted(rec.images.keys())

    # Don't hold out first/last two (BA gauge anchors)
    if len(sorted_ids) <= 4:
        logger.warning("Reconstruction too small for holdout test (<= 4 images)")
        return set()
    candidate_ids = sorted_ids[2:-2]

    step = max(1, int(1.0 / fraction))
    holdout = set(candidate_ids[::step])

    logger.info(
        f"Selected {len(holdout)} holdout images out of {len(sorted_ids)} "
        f"(step={step}, fraction={fraction:.0%})"
    )
    return holdout


def add_noise_to_pose(
    cam_from_world: pycolmap.Rigid3d,
    noise_m: float,
    noise_deg: float,
    rng: np.random.RandomState,
) -> pycolmap.Rigid3d:
    """Add Gaussian noise to a camera pose."""
    world_from_cam = cam_from_world.inverse()

    # Position noise
    noisy_t = world_from_cam.translation + rng.randn(3) * noise_m

    # Rotation noise
    noisy_R = world_from_cam.rotation.matrix()
    if noise_deg > 0:
        angle = rng.randn() * np.radians(noise_deg)
        axis = rng.randn(3)
        axis_norm = np.linalg.norm(axis)
        if axis_norm > 1e-8:
            axis /= axis_norm
            noise_rot = ScipyRotation.from_rotvec(axis * angle).as_matrix()
            noisy_R = noise_rot @ noisy_R

    world_from_cam_noisy = pycolmap.Rigid3d(
        pycolmap.Rotation3d(noisy_R),
        noisy_t,
    )
    return world_from_cam_noisy.inverse()


def run_holdout_test(
    scan_sfm_dir: Path,
    image_dir: Path,
    holdout_fraction: float = 0.2,
    output_dir: Path = None,
    add_noise_m: float = 0.0,
    add_noise_deg: float = 0.0,
):
    """Run the holdout localization test.

    Args:
        scan_sfm_dir: Path to the local refinement sfm dir
                      (contains cameras.bin, images.bin, points3D.bin, features.h5)
        image_dir: Path to the scan's Frames/ directory.
        holdout_fraction: Fraction of images to hold out.
        output_dir: Where to save results.
        add_noise_m: Gaussian position noise (metres) for approximate pose.
        add_noise_deg: Gaussian rotation noise (degrees) for approximate pose.
    """
    features_h5 = scan_sfm_dir / "features.h5"
    assert scan_sfm_dir.exists(), f"SFM dir not found: {scan_sfm_dir}"
    assert features_h5.exists(), f"Features not found: {features_h5}"
    assert image_dir.exists(), f"Image dir not found: {image_dir}"

    # Load the full reconstruction (this is our "ground truth")
    logger.info(f"Loading full reconstruction from {scan_sfm_dir}")
    full_rec = pycolmap.Reconstruction(scan_sfm_dir)
    logger.info(
        f"Full reconstruction: {full_rec.num_reg_images()} images, "
        f"{len(full_rec.points3D)} 3D points"
    )

    # Select holdout images
    holdout_ids = select_holdout_images(full_rec, holdout_fraction)
    if not holdout_ids:
        logger.error("No holdout images selected. Aborting.")
        return None

    # Build database reconstruction (without holdout images)
    db_rec = build_holdout_reconstruction(full_rec, holdout_ids)

    # Build localizer against the database reconstruction
    # features.h5 still contains all images (including holdout),
    # but the localizer only matches against images in the reconstruction.
    localizer = SingleImageLocalizer(
        reconstruction=db_rec,
        image_dir=image_dir,
        features_h5=features_h5,
    )

    # Localize each held-out image
    rng = np.random.RandomState(42)
    results = []

    for img_id in sorted(holdout_ids):
        image = full_rec.images[img_id]
        gt_cam_from_world = image.cam_from_world()
        camera = full_rec.cameras[image.camera_id]
        image_path = image_dir / image.name

        if not image_path.exists():
            logger.warning(f"Image not found: {image_path}, skipping")
            continue

        # Build approximate pose (optionally with noise)
        if add_noise_m > 0 or add_noise_deg > 0:
            approx_cfw = add_noise_to_pose(
                gt_cam_from_world, add_noise_m, add_noise_deg, rng
            )
        else:
            approx_cfw = deepcopy(gt_cam_from_world)

        # Localize
        result = localizer.localize(
            image_path=image_path,
            camera=camera,
            approximate_cam_from_world=approx_cfw,
            query_name=image.name,
        )

        entry = {
            "image_id": int(img_id),
            "image_name": image.name,
            "success": result.success,
            "num_inliers": result.num_inliers,
            "num_matches": result.num_matches,
            "num_2d3d": result.num_2d3d_correspondences,
        }

        if result.success:
            pos_err, rot_err = pose_error(
                result.refined_cam_from_world, gt_cam_from_world
            )
            entry["pos_error_m"] = float(pos_err)
            entry["rot_error_deg"] = float(rot_err)
            logger.info(
                f"  {image.name}: pos_err={pos_err*100:.1f}cm, "
                f"rot_err={rot_err:.2f} deg, "
                f"inliers={result.num_inliers}"
            )
        else:
            entry["pos_error_m"] = None
            entry["rot_error_deg"] = None
            logger.warning(f"  {image.name}: FAILED")

        results.append(entry)

    # --- Summary statistics ---
    successes = [r for r in results if r["success"]]
    failures = [r for r in results if not r["success"]]

    pos_errors = [r["pos_error_m"] * 100 for r in successes]  # cm
    rot_errors = [r["rot_error_deg"] for r in successes]

    summary = {
        "total_queries": len(results),
        "successes": len(successes),
        "failures": len(failures),
        "success_rate": len(successes) / len(results) if results else 0,
        "noise_m": add_noise_m,
        "noise_deg": add_noise_deg,
    }

    if pos_errors:
        summary["pos_error_cm"] = {
            "median": float(np.median(pos_errors)),
            "mean": float(np.mean(pos_errors)),
            "p90": float(np.percentile(pos_errors, 90)),
            "max": float(np.max(pos_errors)),
        }
        summary["rot_error_deg"] = {
            "median": float(np.median(rot_errors)),
            "mean": float(np.mean(rot_errors)),
            "p90": float(np.percentile(rot_errors, 90)),
            "max": float(np.max(rot_errors)),
        }

    print("\n" + "=" * 60)
    print("HOLDOUT LOCALIZATION RESULTS")
    print("=" * 60)
    print(f"Queries: {summary['total_queries']}")
    print(
        f"Success: {summary['successes']} / {summary['total_queries']} "
        f"({summary['success_rate']:.0%})"
    )
    if pos_errors:
        pe = summary["pos_error_cm"]
        re = summary["rot_error_deg"]
        print(
            f"Position error (cm): median={pe['median']:.1f}, "
            f"mean={pe['mean']:.1f}, p90={pe['p90']:.1f}, max={pe['max']:.1f}"
        )
        print(
            f"Rotation error (deg):  median={re['median']:.2f}, "
            f"mean={re['mean']:.2f}, p90={re['p90']:.2f}, max={re['max']:.2f}"
        )
    print("=" * 60)

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        result_path = output_dir / "holdout_results.json"
        with open(result_path, "w") as f:
            json.dump({"summary": summary, "per_image": results}, f, indent=2)
        logger.info(f"Results saved to {result_path}")

    return summary


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Holdout localization test")
    parser.add_argument(
        "--scan_sfm_dir", type=Path, required=True,
        help="Path to refined/local/<scan_id>/sfm/",
    )
    parser.add_argument(
        "--image_dir", type=Path, required=True,
        help="Path to datasets/<scan_id>/Frames/",
    )
    parser.add_argument("--holdout_fraction", type=float, default=0.2)
    parser.add_argument(
        "--output_dir", type=Path,
        default=Path("tests/localize_holdout_results"),
    )
    parser.add_argument(
        "--noise_m", type=float, default=0.0,
        help="Gaussian position noise (metres) to add to approximate pose",
    )
    parser.add_argument(
        "--noise_deg", type=float, default=0.0,
        help="Gaussian rotation noise (degrees) to add to approximate pose",
    )
    args = parser.parse_args()

    run_holdout_test(
        scan_sfm_dir=args.scan_sfm_dir,
        image_dir=args.image_dir,
        holdout_fraction=args.holdout_fraction,
        output_dir=args.output_dir,
        add_noise_m=args.noise_m,
        add_noise_deg=args.noise_deg,
    )
