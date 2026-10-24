"""Single-image localization against a prior COLMAP reconstruction.

Usage:
    # One-time setup (reusable across queries):
    loc = SingleImageLocalizer.from_reconstruction_dir(
        reconstruction_dir=Path("refined/global/refined_sfm_combined"),
        image_dir=Path("datasets/scan_id/Frames"),
        features_h5=Path("refined/global/refined_sfm_combined/features.h5"),
    )

    # Per-query:
    result = loc.localize(
        image_path=Path("query.jpg"),
        camera=pycolmap.Camera(...),
        approximate_cam_from_world=pycolmap.Rigid3d(...),
    )
    print(result.refined_cam_from_world)
"""

from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Tuple, Dict
import logging
import tempfile
import numpy as np
import pycolmap

from hloc import extract_features, match_features
from hloc.utils.io import get_keypoints, get_matches

from utils.pointcloud_covisibility import PointcloudCovisibilityIndex

logger = logging.getLogger(__name__)


def _build_feature_conf():
    """Build ALIKED feature config matching triangulation.py process_features_and_matching."""
    conf = deepcopy(extract_features.confs["aliked-n16"])
    conf["model"]["max_num_keypoints"] = 1024
    conf["model"]["detection_threshold"] = 0.3
    conf["model"]["nms_radius"] = 4
    conf["preprocessing"]["resize_max"] = 1024
    return conf


def _build_matcher_conf():
    """Build LightGlue matcher config matching triangulation.py."""
    conf = deepcopy(match_features.confs["aliked+lightglue"])
    conf["model"]["compile_network"] = True
    return conf


@dataclass
class LocalizationResult:
    """Result of a single-image localization query."""
    success: bool
    refined_cam_from_world: Optional[pycolmap.Rigid3d]
    num_inliers: int = 0
    num_matches: int = 0
    num_2d3d_correspondences: int = 0
    matched_db_image_ids: List[int] = field(default_factory=list)
    covisibility_scores: List[Tuple[int, int]] = field(default_factory=list)


class SingleImageLocalizer:
    """Localize a single image against a preloaded COLMAP reconstruction.

    Lifecycle:
        1. Create once (loads reconstruction, builds spatial index).
        2. Call localize() for each query frame.
        3. The index and reconstruction stay in memory for repeated queries.
    """

    def __init__(
        self,
        reconstruction: pycolmap.Reconstruction,
        image_dir: Path,
        features_h5: Path,
        voxel_size: float = 0.20,
        num_pairs: int = 20,
        max_depth: float = 15.0,
        ransac_max_error: float = 12.0,
    ):
        """
        Args:
            reconstruction: Loaded COLMAP reconstruction with 3D points and images.
            image_dir: Directory containing the database images (needed by hloc
                       if feature cache doesn't have them, though normally it does).
            features_h5: Path to the precomputed features .h5 file for database images
                         (from the original triangulation run / merged at global stitch).
            voxel_size: Voxel grid cell size in metres.
            num_pairs: Number of database images to match against.
            max_depth: Max frustum ray depth for voxel traversal.
            ransac_max_error: PnP RANSAC inlier threshold in pixels.
        """
        self.reconstruction = reconstruction
        self.image_dir = image_dir
        self.features_h5 = features_h5
        self.num_pairs = num_pairs
        self.max_depth = max_depth
        self.ransac_max_error = ransac_max_error

        # Build the spatial index
        self.index = PointcloudCovisibilityIndex(
            reconstruction, voxel_size=voxel_size
        )

        # Pre-build name<->id lookups for the database
        self.db_name_to_id: Dict[str, int] = {
            img.name: img_id for img_id, img in reconstruction.images.items()
        }
        self.db_id_to_name: Dict[int, str] = {
            img_id: img.name for img_id, img in reconstruction.images.items()
        }

        logger.info(
            f"SingleImageLocalizer ready: {len(reconstruction.images)} db images, "
            f"{len(reconstruction.points3D)} 3D points"
        )

    @classmethod
    def from_reconstruction_dir(
        cls,
        reconstruction_dir: Path,
        image_dir: Path,
        features_h5: Path,
        **kwargs,
    ) -> "SingleImageLocalizer":
        """Create from a COLMAP reconstruction directory on disk."""
        logger.info(f"Loading reconstruction from {reconstruction_dir}")
        reconstruction = pycolmap.Reconstruction(reconstruction_dir)
        return cls(
            reconstruction=reconstruction,
            image_dir=image_dir,
            features_h5=features_h5,
            **kwargs,
        )

    def localize(
        self,
        image_path: Path,
        camera: pycolmap.Camera,
        approximate_cam_from_world: pycolmap.Rigid3d,
        query_name: Optional[str] = None,
    ) -> LocalizationResult:
        """Localize a single query image.

        Args:
            image_path: Path to the query RGB image.
            camera: Query camera intrinsics (pycolmap.Camera).
            approximate_cam_from_world: Rough pose estimate (e.g. from ARKit).
            query_name: Optional name for the query image in h5 files.
                        Defaults to image_path.name.

        Returns:
            LocalizationResult with refined pose (or failure info).
        """
        if query_name is None:
            query_name = image_path.name

        # --- Step 1: Find best database image pairs via frustum covisibility ---
        covisibility_scores = self.index.find_covisible_images(
            approximate_cam_from_world,
            camera,
            num_images=self.num_pairs,
            max_depth=self.max_depth,
        )
        db_image_ids = [img_id for img_id, _score in covisibility_scores]

        if not db_image_ids:
            logger.warning("No database images found for query. Localization failed.")
            return LocalizationResult(success=False, refined_cam_from_world=None)

        db_image_names = [self.db_id_to_name[img_id] for img_id in db_image_ids]
        logger.info(f"Selected {len(db_image_ids)} database images for matching")

        # --- Step 2: Extract features on query image + match ---
        with tempfile.TemporaryDirectory(prefix="loc_query_") as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Symlink the query image so hloc can find it
            query_image_dir = tmp_path / "images"
            query_image_dir.mkdir()
            (query_image_dir / query_name).symlink_to(image_path.resolve())

            query_features_path = tmp_path / "query_features.h5"
            query_matches_path = tmp_path / "query_matches.h5"
            pairs_path = tmp_path / "pairs.txt"

            # Write pairs file: query matched against each DB image
            with open(pairs_path, "w") as f:
                for db_name in db_image_names:
                    f.write(f"{query_name} {db_name}\n")

            # Extract features on the query image
            feature_conf = _build_feature_conf()
            feature_conf["output"] = "query_features"
            extract_features.main(
                feature_conf,
                query_image_dir,
                feature_path=query_features_path,
                as_half=True,
                image_list=[query_name],
            )

            # --- Step 3: Match query against database images ---
            matcher_conf = _build_matcher_conf()
            match_features.main(
                matcher_conf,
                pairs_path,
                features=query_features_path,
                matches=query_matches_path,
                features_ref=self.features_h5,
            )

            # --- Step 4: Collect 2D<->3D correspondences and run PnP ---
            result = self._estimate_pose(
                query_name=query_name,
                query_camera=camera,
                db_image_ids=db_image_ids,
                query_features_path=query_features_path,
                query_matches_path=query_matches_path,
                covisibility_scores=covisibility_scores,
            )

        return result

    def _estimate_pose(
        self,
        query_name: str,
        query_camera: pycolmap.Camera,
        db_image_ids: List[int],
        query_features_path: Path,
        query_matches_path: Path,
        covisibility_scores: List[Tuple[int, int]],
    ) -> LocalizationResult:
        """Run PnP+RANSAC from feature matches against database 3D points.

        For each matched DB image, look up which of its 2D keypoints have
        associated 3D points, then accumulate 2D(query)<->3D correspondences.
        """
        kpq = get_keypoints(query_features_path, query_name)
        kpq += 0.5  # COLMAP coordinate convention

        # Accumulate 2D query keypoint idx -> 3D point ID
        kp_idx_to_3D: Dict[int, int] = {}
        total_matches = 0

        for db_id in db_image_ids:
            db_image = self.reconstruction.images[db_id]
            if db_image.num_points3D == 0:
                continue

            # Get 3D point IDs for each 2D observation in the DB image
            points3D_ids = np.array([
                p.point3D_id if p.has_point3D() else -1
                for p in db_image.points2D
            ])

            try:
                matched, _ = get_matches(
                    query_matches_path, query_name, db_image.name
                )
            except (KeyError, ValueError):
                continue

            if len(matched) == 0:
                continue

            # Filter to matches where the DB keypoint has a 3D point
            valid = points3D_ids[matched[:, 1]] != -1
            matched = matched[valid]
            total_matches += len(matched)

            for query_idx, db_idx in matched:
                p3d_id = points3D_ids[db_idx]
                # Keep first observation per query keypoint (avoid duplicates)
                if query_idx not in kp_idx_to_3D:
                    kp_idx_to_3D[query_idx] = p3d_id

        if len(kp_idx_to_3D) < 4:
            logger.warning(
                f"Only {len(kp_idx_to_3D)} 2D-3D correspondences found "
                f"(need >=4 for PnP). Localization failed."
            )
            return LocalizationResult(
                success=False,
                refined_cam_from_world=None,
                num_matches=total_matches,
                num_2d3d_correspondences=len(kp_idx_to_3D),
                matched_db_image_ids=db_image_ids,
                covisibility_scores=covisibility_scores,
            )

        # Build arrays for PnP
        query_kp_idxs = list(kp_idx_to_3D.keys())
        points2D = kpq[query_kp_idxs]
        points3D = np.array([
            self.reconstruction.points3D[kp_idx_to_3D[idx]].xyz
            for idx in query_kp_idxs
        ])

        logger.info(
            f"PnP input: {len(points2D)} 2D-3D correspondences "
            f"from {total_matches} raw matches"
        )

        # Run PnP + RANSAC + refinement
        ret = pycolmap.estimate_and_refine_absolute_pose(
            points2D,
            points3D,
            query_camera,
            estimation_options={
                "ransac": {"max_error": self.ransac_max_error}
            },
            refinement_options={},
        )

        if ret is None or "cam_from_world" not in ret:
            logger.warning("PnP+RANSAC failed to find a valid pose.")
            return LocalizationResult(
                success=False,
                refined_cam_from_world=None,
                num_matches=total_matches,
                num_2d3d_correspondences=len(kp_idx_to_3D),
                matched_db_image_ids=db_image_ids,
                covisibility_scores=covisibility_scores,
            )

        logger.info(
            f"Localization succeeded: {ret['num_inliers']} inliers "
            f"out of {len(points2D)} correspondences"
        )

        return LocalizationResult(
            success=True,
            refined_cam_from_world=ret["cam_from_world"],
            num_inliers=ret["num_inliers"],
            num_matches=total_matches,
            num_2d3d_correspondences=len(kp_idx_to_3D),
            matched_db_image_ids=db_image_ids,
            covisibility_scores=covisibility_scores,
        )
