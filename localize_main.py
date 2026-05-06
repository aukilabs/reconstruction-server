"""Entry point for single-image localization against a prior reconstruction.

Called by the Rust runner with paths to:
  - A prior COLMAP reconstruction directory (global refined_sfm_combined)
  - A query image
  - Query camera intrinsics (fx, fy, cx, cy, w, h)
  - Approximate query pose (position + quaternion in OpenGL convention)

The localizer is cached at module level so the reconstruction and spatial index
are only loaded once when the same process handles multiple queries.
"""

from pathlib import Path
import argparse
import json
import logging
import numpy as np
import pycolmap

from localize_image import SingleImageLocalizer
from utils.data_utils import convert_pose_opengl_to_colmap, setup_logger


# Module-level cache for the localizer (persists across calls within the same process)
_cached_localizer: SingleImageLocalizer = None
_cached_reconstruction_dir: Path = None


def get_or_create_localizer(
    reconstruction_dir: Path,
    image_dir: Path,
    features_h5: Path,
    voxel_size: float = 0.20,
) -> SingleImageLocalizer:
    """Return a cached localizer or build a new one."""
    global _cached_localizer, _cached_reconstruction_dir

    if (
        _cached_localizer is not None
        and _cached_reconstruction_dir == reconstruction_dir
    ):
        return _cached_localizer

    _cached_localizer = SingleImageLocalizer.from_reconstruction_dir(
        reconstruction_dir=reconstruction_dir,
        image_dir=image_dir,
        features_h5=features_h5,
        voxel_size=voxel_size,
    )
    _cached_reconstruction_dir = reconstruction_dir
    return _cached_localizer


def main(args):
    logger = setup_logger(
        name="localize_image",
        log_file=str(args.output_path / "localize_logs"),
        domain_id=args.domain_id,
        job_id=args.job_id,
        level=args.log_level,
        log_format=args.log_format
    )

    logger.info(f"Localizing query image: {args.query_image}")
    logger.info(f"Reconstruction: {args.reconstruction_dir}")

    # Build or retrieve cached localizer
    localizer = get_or_create_localizer(
        reconstruction_dir=args.reconstruction_dir,
        image_dir=args.image_dir,
        features_h5=args.features_h5,
        voxel_size=args.voxel_size,
    )

    # Parse camera intrinsics
    intrinsics = json.loads(args.intrinsics)  # [fx, fy, cx, cy, w, h]
    fx, fy, cx, cy, w, h = intrinsics
    if fx == fy:
        camera = pycolmap.Camera(
            model="SIMPLE_PINHOLE", width=int(w), height=int(h),
            params=[fx, cx, cy],
        )
    else:
        camera = pycolmap.Camera(
            model="PINHOLE", width=int(w), height=int(h),
            params=[fx, fy, cx, cy],
        )

    # Parse approximate pose
    # Input format: position (x,y,z) + quaternion (w,x,y,z) in OpenGL convention
    # (same as ARKit poses in the scan data)
    pose_data = json.loads(args.approximate_pose)  # [px, py, pz, qw, qx, qy, qz]
    position_gl = np.array(pose_data[0:3])
    quat_gl = np.array(pose_data[3:7])
    position, rotation = convert_pose_opengl_to_colmap(position_gl, quat_gl)
    cam_to_world = pycolmap.Rigid3d(pycolmap.Rotation3d(rotation), position)
    approximate_cam_from_world = cam_to_world.inverse()

    # Run localization
    result = localizer.localize(
        image_path=Path(args.query_image),
        camera=camera,
        approximate_cam_from_world=approximate_cam_from_world,
    )

    # Write result
    output = {
        "success": result.success,
        "num_inliers": result.num_inliers,
        "num_matches": result.num_matches,
        "num_2d3d_correspondences": result.num_2d3d_correspondences,
    }

    if result.success:
        cfw = result.refined_cam_from_world
        wtc = cfw.inverse()
        output["refined_pose"] = {
            "position": wtc.translation.tolist(),
            "rotation_matrix": wtc.rotation.matrix().tolist(),
            "quaternion_wxyz": wtc.rotation.quat.tolist(),
        }
        logger.info(f"Localization succeeded: {result.num_inliers} inliers")
    else:
        logger.warning("Localization failed")

    output_file = args.output_path / "localization_result.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    logger.info(f"Result written to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Single-image localization")
    parser.add_argument(
        "--reconstruction_dir", type=Path, required=True,
        help="Path to COLMAP reconstruction (cameras.bin, images.bin, points3D.bin)",
    )
    parser.add_argument(
        "--image_dir", type=Path, required=True,
        help="Path to database images directory",
    )
    parser.add_argument(
        "--features_h5", type=Path, required=True,
        help="Path to precomputed features.h5 from triangulation",
    )
    parser.add_argument(
        "--query_image", type=Path, required=True,
        help="Path to the query RGB image",
    )
    parser.add_argument(
        "--intrinsics", type=str, required=True,
        help='Camera intrinsics as JSON: [fx, fy, cx, cy, w, h]',
    )
    parser.add_argument(
        "--approximate_pose", type=str, required=True,
        help='Approximate pose as JSON: [px, py, pz, qw, qx, qy, qz] (OpenGL convention)',
    )
    parser.add_argument("--output_path", type=Path, default=Path("./localize_output"))
    parser.add_argument("--voxel_size", type=float, default=0.20)
    parser.add_argument("--domain_id", type=str, default="")
    parser.add_argument("--job_id", type=str, default="")
    parser.add_argument(
        "--log_level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    args = parser.parse_args()
    args.output_path.mkdir(parents=True, exist_ok=True)
    main(args)
