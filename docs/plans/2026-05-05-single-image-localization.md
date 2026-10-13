# Single-Image Localization Against Prior Reconstruction — Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Add a capability to the reconstruction server that takes a single camera frame (RGB + intrinsics + approximate pose) and returns a refined pose, localized against an existing COLMAP reconstruction.

**Architecture:** Load a prior COLMAP reconstruction once and precompute a spatial index (voxel grid, 20 cm) that maps 3D points to the images that observe them. For each query, frustum-march through the voxel grid to find which existing 3D points the query camera would see, then rank the original images by how many of those visible points they observe — "pairs from pointcloud covisibility." Extract ALIKED features on the query image (same config as `triangulation.py`), match against the top-20 database images with LightGlue, and estimate the pose via PnP+RANSAC using the existing 3D points.

The localizer always targets the **globally stitched reconstruction** (`refined_sfm_combined/`) and its merged `features.h5`, even for single-scan domains. The pipeline always runs global refinement for consistency.

**Tech Stack:** Python, pycolmap, numpy, hloc (ALIKED+LightGlue, `extract_features`, `match_features`), existing `utils/triangulation.py` conventions.

---

## Overview of Tasks

| # | Task | Files |
|---|------|-------|
| 1 | `PointcloudCovisibilityIndex` — voxel grid + frustum query | `utils/pointcloud_covisibility.py` |
| 2 | `SingleImageLocalizer` — full pipeline (pairs → features → match → PnP) | `localize_image.py` |
| 3 | Unit tests for the covisibility index | `tests/test_localize_image.py` |
| 4 | CLI entry point for Rust runner | `localize_main.py` |
| 5 | Merge per-scan features.h5 at global stitch time | `utils/merge_features.py`, modify `global_main.py` |
| 6 | Real-data holdout validation test (3 noise levels) | `tests/test_localize_holdout.py` |
| 7 | Upload features.h5 from Rust runners | modify `refined.rs` (local), modify `output.rs` (global) |

**Pipeline per query:**
1. Frustum-march through voxel grid → top 20 database images (~1ms)
2. Extract ALIKED features on query image (same config as `process_features_and_matching`: `aliked-n16`, 1024 keypoints, threshold 0.3, NMS 4, resize_max 1024)
3. LightGlue matching against precomputed database features (via `features_ref` parameter)
4. Collect 2D(query) ↔ 3D correspondences via database tracks
5. `pycolmap.estimate_and_refine_absolute_pose()` → refined pose

---

## Task 1: Create `PointcloudCovisibilityIndex` — voxel grid + image visibility

**Objective:** Build a spatial index from a COLMAP reconstruction that maps voxel cells to the set of image IDs observing points within that cell. This is the "pairs from pointcloud covisibility" data structure.

**Files:**
- Create: `utils/pointcloud_covisibility.py`

**Implementation:**

```python
"""Spatial index for finding database images that observe the same 3D regions as a query camera."""

import numpy as np
import pycolmap
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class PointcloudCovisibilityIndex:
    """Voxel grid index over a COLMAP reconstruction's 3D points.
    
    For each voxel cell, stores:
      - The set of image IDs whose registered 2D observations project into that cell
      - The 3D point IDs contained in that cell
    
    This allows efficient "which database images see roughly the same scene as
    this query camera?" queries without requiring the query to already have
    2D↔3D correspondences (unlike hloc's pairs_from_covisibility which needs 
    a COLMAP model with the query registered).
    
    Unlike pairs_from_poses (which ranks by camera position proximity), this
    actually accounts for *what is in front of the camera*, not just where the
    camera is.
    """

    def __init__(
        self,
        reconstruction: pycolmap.Reconstruction,
        voxel_size: float = 0.20,
    ):
        """Build the index from an existing reconstruction.
        
        Args:
            reconstruction: Fully triangulated COLMAP reconstruction.
            voxel_size: Edge length in metres for voxel cells. 20 cm default
                        balances granularity vs. memory.
        """
        self.reconstruction = reconstruction
        self.voxel_size = voxel_size
        self.inv_voxel_size = 1.0 / voxel_size

        # voxel_key (i,j,k) -> set of image_ids
        self.voxel_to_image_ids: Dict[Tuple[int, int, int], set] = defaultdict(set)
        # voxel_key -> list of point3D_ids
        self.voxel_to_point3d_ids: Dict[Tuple[int, int, int], List[int]] = defaultdict(list)

        self._build_index()

    def _point_to_voxel(self, xyz: np.ndarray) -> Tuple[int, int, int]:
        """Map a 3D point to its voxel grid cell."""
        ijk = np.floor(xyz * self.inv_voxel_size).astype(np.int64)
        return (int(ijk[0]), int(ijk[1]), int(ijk[2]))

    def _build_index(self):
        """Populate the voxel grid from reconstruction.points3D and their tracks."""
        points3D = self.reconstruction.points3D
        n_points = len(points3D)
        logger.info(
            f"Building PointcloudCovisibilityIndex: {n_points} 3D points, "
            f"voxel_size={self.voxel_size:.2f}m"
        )

        for point3D_id, point3D in points3D.items():
            voxel_key = self._point_to_voxel(point3D.xyz)
            self.voxel_to_point3d_ids[voxel_key].append(point3D_id)

            # Each track element tells us which image observes this point
            for track_elem in point3D.track.elements:
                self.voxel_to_image_ids[voxel_key].add(track_elem.image_id)

        logger.info(
            f"Index built: {len(self.voxel_to_image_ids)} occupied voxels, "
            f"{len(self.reconstruction.images)} database images"
        )

    def query_visible_voxels(
        self,
        cam_from_world: pycolmap.Rigid3d,
        camera: pycolmap.Camera,
        max_depth: float = 15.0,
        depth_step: Optional[float] = None,
    ) -> List[Tuple[int, int, int]]:
        """Find voxels that fall inside the query camera's view frustum.
        
        Strategy: cast rays from the camera centre through a grid of pixels
        (subsampled for speed) and step along each ray, collecting occupied
        voxels. This is a conservative approximation — it may miss some edge
        voxels, but that's fine for pair selection (we only need a good ranking,
        not perfect recall).
        
        Args:
            cam_from_world: Query camera extrinsic (world → camera transform).
            camera: Query camera intrinsic model.
            max_depth: Maximum ray depth in metres.
            depth_step: Step size along each ray. Defaults to voxel_size.
            
        Returns:
            List of occupied voxel keys (i,j,k) visible from this camera.
        """
        if depth_step is None:
            depth_step = self.voxel_size

        world_from_cam = cam_from_world.inverse()
        cam_center = world_from_cam.translation
        R_world_from_cam = world_from_cam.rotation.matrix()

        w, h = camera.width, camera.height

        # Subsample pixel grid — ~16x16 rays is enough for pair ranking
        n_samples = 16
        us = np.linspace(0, w - 1, n_samples)
        vs = np.linspace(0, h - 1, n_samples)
        uu, vv = np.meshgrid(us, vs)
        pixels = np.stack([uu.ravel(), vv.ravel()], axis=-1)  # (N, 2)

        # Unproject pixels to camera-frame unit rays
        # cam_from_img gives us the ray direction in camera coordinates
        rays_cam = np.array([camera.cam_from_img(p) for p in pixels])  # (N, 2) -> normalized
        # Make 3D: (x, y, 1) in camera frame, then normalize
        rays_cam_3d = np.column_stack([rays_cam, np.ones(len(rays_cam))])
        rays_cam_3d /= np.linalg.norm(rays_cam_3d, axis=1, keepdims=True)

        # Rotate to world frame
        rays_world = (R_world_from_cam @ rays_cam_3d.T).T  # (N, 3)

        # March along each ray and collect occupied voxels
        visited = set()
        t_values = np.arange(depth_step, max_depth, depth_step)

        for ray_dir in rays_world:
            points_along_ray = cam_center[np.newaxis, :] + t_values[:, np.newaxis] * ray_dir[np.newaxis, :]
            voxel_ijk = np.floor(points_along_ray * self.inv_voxel_size).astype(np.int64)
            for ijk in voxel_ijk:
                key = (int(ijk[0]), int(ijk[1]), int(ijk[2]))
                if key in self.voxel_to_image_ids:
                    visited.add(key)

        return list(visited)

    def find_covisible_images(
        self,
        cam_from_world: pycolmap.Rigid3d,
        camera: pycolmap.Camera,
        num_images: int = 20,
        max_depth: float = 15.0,
    ) -> List[Tuple[int, int]]:
        """Find database images that observe the same 3D points visible from the query.
        
        Args:
            cam_from_world: Query camera's world-to-camera transform.
            camera: Query camera intrinsics.
            num_images: Number of top database images to return.
            max_depth: Maximum depth to search along frustum rays.
            
        Returns:
            List of (image_id, covisibility_count) tuples, sorted descending
            by count. At most num_images entries.
        """
        visible_voxels = self.query_visible_voxels(
            cam_from_world, camera, max_depth=max_depth
        )

        if not visible_voxels:
            logger.warning("No occupied voxels found in query frustum. Falling back to nearest cameras.")
            return self._fallback_nearest_cameras(cam_from_world, num_images)

        # Accumulate: for each database image, how many visible voxels does it observe?
        image_scores: Dict[int, int] = defaultdict(int)
        for voxel_key in visible_voxels:
            for image_id in self.voxel_to_image_ids[voxel_key]:
                image_scores[image_id] += 1

        # Sort by score descending, take top K
        ranked = sorted(image_scores.items(), key=lambda x: -x[1])
        result = ranked[:num_images]

        if result:
            logger.info(
                f"Pointcloud covisibility: {len(visible_voxels)} visible voxels, "
                f"top image score={result[0][1]}, returning {len(result)} images"
            )
        return result

    def _fallback_nearest_cameras(
        self, cam_from_world: pycolmap.Rigid3d, num_images: int
    ) -> List[Tuple[int, int]]:
        """Position-only fallback when frustum search finds nothing."""
        query_pos = cam_from_world.inverse().translation
        dists = []
        for image_id, image in self.reconstruction.images.items():
            db_pos = image.cam_from_world().inverse().translation
            d = np.linalg.norm(query_pos - db_pos)
            dists.append((image_id, d))
        dists.sort(key=lambda x: x[1])
        # Return with dummy score (negative distance so higher = closer)
        return [(img_id, 0) for img_id, _ in dists[:num_images]]
    
    def get_point3d_ids_in_voxels(
        self, voxel_keys: List[Tuple[int, int, int]]
    ) -> List[int]:
        """Get all 3D point IDs contained in the given voxels."""
        point_ids = []
        for key in voxel_keys:
            point_ids.extend(self.voxel_to_point3d_ids.get(key, []))
        return point_ids
```

**Verification:** Unit test that builds an index from a small synthetic reconstruction, queries with a known camera pose, and checks that the correct database images are returned.

**Commit:** `feat: add PointcloudCovisibilityIndex for frustum-based pair selection`

---

## Task 2: Create `localize_single_image()` — the main localization function

**Objective:** Wire up the full pipeline: load reconstruction → build index (or reuse) → find pairs → extract features → match → PnP.

**Files:**
- Create: `localize_image.py`

**Implementation:**

```python
"""Single-image localization against a prior COLMAP reconstruction.

Usage:
    # One-time setup (reusable across queries):
    loc = SingleImageLocalizer.from_reconstruction_dir(Path("path/to/colmap_rec"))
    
    # Per-query:
    result = loc.localize(
        image_path=Path("query.jpg"),
        camera=pycolmap.Camera(...),
        approximate_cam_from_world=pycolmap.Rigid3d(...),
    )
    print(result.refined_cam_from_world)
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple
import logging
import tempfile
import shutil
import numpy as np
import pycolmap

from hloc import extract_features, match_features
from hloc.utils.io import get_keypoints, get_matches

from utils.pointcloud_covisibility import PointcloudCovisibilityIndex

logger = logging.getLogger(__name__)


# ---- Feature config: same as triangulation.py process_features_and_matching ----
FEATURE_CONF = {
    **extract_features.confs["aliked-n16"],
    "model": {
        **extract_features.confs["aliked-n16"]["model"],
        "max_num_keypoints": 1024,
        "detection_threshold": 0.3,
        "nms_radius": 4,
    },
    "preprocessing": {
        **extract_features.confs["aliked-n16"]["preprocessing"],
        "resize_max": 1024,
    },
}

MATCHER_CONF = {
    **match_features.confs["aliked+lightglue"],
    "model": {
        **match_features.confs["aliked+lightglue"]["model"],
        "compile_network": True,
    },
}


@dataclass
class LocalizationResult:
    """Result of a single-image localization query."""
    success: bool
    refined_cam_from_world: Optional[pycolmap.Rigid3d]
    num_inliers: int = 0
    num_matches: int = 0
    num_2d3d_correspondences: int = 0
    matched_db_image_ids: List[int] = None
    covisibility_scores: List[Tuple[int, int]] = None

    def __post_init__(self):
        if self.matched_db_image_ids is None:
            self.matched_db_image_ids = []
        if self.covisibility_scores is None:
            self.covisibility_scores = []


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
            image_dir: Directory containing the database images (for feature extraction
                       if the feature cache doesn't have them yet).
            features_h5: Path to the precomputed features .h5 file for database images
                         (from the original triangulation run).
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

        # Pre-build name→id lookup for the database
        self.db_name_to_id = {
            img.name: img_id for img_id, img in reconstruction.images.items()
        }
        self.db_id_to_name = {
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

        # --- Step 2: Extract features on query image ---
        # Use a temporary directory for query-specific outputs
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
            feature_conf = dict(FEATURE_CONF)
            feature_conf["output"] = "query_features"
            extract_features.main(
                feature_conf,
                query_image_dir,
                tmp_path,
                feature_path=query_features_path,
                as_half=True,
                image_list=[query_name],
            )

            # --- Step 3: Match query against database images ---
            matcher_conf = dict(MATCHER_CONF)
            match_features.main(
                matcher_conf,
                pairs_path,
                features=query_features_path,
                features_ref=self.features_h5,
                matches=query_matches_path,
            )

            # --- Step 4: Collect 2D↔3D correspondences and run PnP ---
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
        
        This follows the same logic as hloc's localize_sfm.pose_from_cluster:
        for each matched DB image, look up which of its 2D keypoints have 
        associated 3D points, then accumulate 2D(query)↔3D correspondences.
        """
        kpq = get_keypoints(query_features_path, query_name)
        kpq += 0.5  # COLMAP coordinate convention

        # Accumulate 2D query keypoint idx → 3D point ID
        kp_idx_to_3D = {}
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
                matches, _ = get_matches(
                    query_matches_path, query_name, db_image.name
                )
            except KeyError:
                continue

            if len(matches) == 0:
                continue

            # Filter to matches where the DB keypoint has a 3D point
            valid = points3D_ids[matches[:, 1]] != -1
            matches = matches[valid]
            total_matches += len(matches)

            for query_idx, db_idx in matches:
                p3d_id = points3D_ids[db_idx]
                # Keep first observation per query keypoint (avoid duplicates)
                if query_idx not in kp_idx_to_3D:
                    kp_idx_to_3D[query_idx] = p3d_id

        if len(kp_idx_to_3D) < 4:
            logger.warning(
                f"Only {len(kp_idx_to_3D)} 2D-3D correspondences found "
                f"(need ≥4 for PnP). Localization failed."
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
```

**Commit:** `feat: add SingleImageLocalizer with full localization pipeline`

---

## Task 3: Write tests

**Objective:** Smoke test and integration test using a small synthetic or real COLMAP reconstruction.

**Files:**
- Create: `tests/test_localize_image.py`

**Implementation:**

```python
"""Tests for single-image localization pipeline."""

import numpy as np
import pycolmap
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from utils.pointcloud_covisibility import PointcloudCovisibilityIndex


def make_synthetic_reconstruction(n_images=5, n_points=200):
    """Create a minimal synthetic COLMAP reconstruction for testing.
    
    Generates cameras arranged in an arc looking at a cluster of 3D points.
    """
    rec = pycolmap.Reconstruction()

    # Single shared camera
    cam = pycolmap.Camera(
        model="PINHOLE",
        width=640,
        height=480,
        params=[500.0, 500.0, 320.0, 240.0],
        camera_id=1,
    )
    rec.add_camera(cam)

    # Generate 3D points in a unit cube centered at (0, 0, 3)
    rng = np.random.RandomState(42)
    for pid in range(1, n_points + 1):
        xyz = rng.uniform([-0.5, -0.5, 2.5], [0.5, 0.5, 3.5])
        p3d = pycolmap.Point3D()
        p3d.xyz = xyz
        p3d.color = np.array([128, 128, 128], dtype=np.uint8)
        rec.add_point3D(p3d.xyz, pycolmap.Track(), p3d.color)

    # Generate cameras on an arc
    for i in range(1, n_images + 1):
        angle = (i - 1) / (n_images - 1) * 0.5 - 0.25  # -0.25 to +0.25 rad
        tx = np.sin(angle) * 2
        tz = -np.cos(angle) * 2 + 3  # offset so cameras look at the point cloud

        cam_to_world = pycolmap.Rigid3d(
            pycolmap.Rotation3d(np.eye(3)),
            np.array([tx, 0.0, tz - 3])
        )
        cam_from_world = cam_to_world.inverse()

        rig = pycolmap.Rig()
        rig.rig_id = i
        sensor = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=1)
        rig.add_ref_sensor(sensor)
        rec.add_rig(rig)

        frame = pycolmap.Frame(
            rig_id=i,
            rig_from_world=cam_from_world,
            frame_id=i,
        )
        frame.add_data_id(pycolmap.data_t(sensor_id=sensor, id=i))
        rec.add_frame(frame)

        img = pycolmap.Image(f"image_{i:03d}.jpg", pycolmap.Point2DList([]), i, 1)
        img.frame_id = i
        rec.add_image(img)
        rec.register_frame(i)

    # Assign some 3D points to each image's track
    # (simplified: each image "sees" a subset of points)
    point3d_ids = list(rec.points3D.keys())
    for img_id, image in rec.images.items():
        # Each image sees ~40% of points
        seen = rng.choice(point3d_ids, size=int(0.4 * len(point3d_ids)), replace=False)
        for pid in seen:
            track = rec.points3D[pid].track
            track.add_element(pycolmap.TrackElement(img_id, 0))

    return rec


class TestPointcloudCovisibilityIndex:
    def test_build_index(self):
        rec = make_synthetic_reconstruction()
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.2)
        assert len(index.voxel_to_image_ids) > 0
        assert len(index.voxel_to_point3d_ids) > 0

    def test_find_covisible_images_returns_results(self):
        rec = make_synthetic_reconstruction()
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.2)

        # Query from a pose that should see the point cloud
        query_cam = pycolmap.Camera(
            model="PINHOLE", width=640, height=480,
            params=[500.0, 500.0, 320.0, 240.0],
        )
        query_pose = pycolmap.Rigid3d(
            pycolmap.Rotation3d(np.eye(3)),
            np.array([0.0, 0.0, 0.0])  # at origin, looking at +Z
        )

        results = index.find_covisible_images(query_pose, query_cam, num_images=3)
        assert len(results) > 0
        # Results should be (image_id, score) tuples
        for img_id, score in results:
            assert img_id in rec.images
            assert score >= 0

    def test_no_visibility_triggers_fallback(self):
        rec = make_synthetic_reconstruction()
        index = PointcloudCovisibilityIndex(rec, voxel_size=0.2)

        # Query from a pose pointing completely away
        query_cam = pycolmap.Camera(
            model="PINHOLE", width=640, height=480,
            params=[500.0, 500.0, 320.0, 240.0],
        )
        # Place camera far away, looking in -Z (away from points at Z=3)
        R = np.diag([1.0, -1.0, -1.0])  # 180° around X axis
        query_pose = pycolmap.Rigid3d(
            pycolmap.Rotation3d(R),
            np.array([0.0, 0.0, 100.0])
        )

        results = index.find_covisible_images(query_pose, query_cam, num_images=3)
        # Should still return results via fallback
        assert len(results) > 0
```

**Commit:** `test: add unit tests for PointcloudCovisibilityIndex`

---

## Task 4: Hook into the reconstruction server (endpoint integration)

**Objective:** Add an entry point callable from the Rust runner, similar to how `local_main.py` and `global_main.py` work.

**Files:**
- Create: `localize_main.py`

**Implementation:**

```python
"""Entry point for single-image localization against a prior reconstruction.

Called by the Rust runner with paths to:
  - A prior COLMAP reconstruction directory
  - A query image
  - Query camera intrinsics (fx, fy, cx, cy, w, h)
  - Approximate query pose (quaternion + translation)
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
        camera = pycolmap.Camera(model="SIMPLE_PINHOLE", width=int(w), height=int(h), params=[fx, cx, cy])
    else:
        camera = pycolmap.Camera(model="PINHOLE", width=int(w), height=int(h), params=[fx, fy, cx, cy])

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
    parser.add_argument("--reconstruction_dir", type=Path, required=True,
                        help="Path to COLMAP reconstruction (cameras.bin, images.bin, points3D.bin)")
    parser.add_argument("--image_dir", type=Path, required=True,
                        help="Path to database images directory")
    parser.add_argument("--features_h5", type=Path, required=True,
                        help="Path to precomputed features.h5 from triangulation")
    parser.add_argument("--query_image", type=Path, required=True,
                        help="Path to the query RGB image")
    parser.add_argument("--intrinsics", type=str, required=True,
                        help='Camera intrinsics as JSON: [fx, fy, cx, cy, w, h]')
    parser.add_argument("--approximate_pose", type=str, required=True,
                        help='Approximate pose as JSON: [px, py, pz, qw, qx, qy, qz] (OpenGL convention)')
    parser.add_argument("--output_path", type=Path, default=Path("./localize_output"))
    parser.add_argument("--voxel_size", type=float, default=0.20)
    parser.add_argument("--domain_id", type=str, default="")
    parser.add_argument("--job_id", type=str, default="")
    parser.add_argument("--log_level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    args = parser.parse_args()
    args.output_path.mkdir(parents=True, exist_ok=True)
    main(args)
```

**Commit:** `feat: add localize_main.py entry point for Rust runner integration`

---

## Task 5: Merge per-scan features.h5 at global stitch time

**Objective:** After `merge_aligned_scans()` produces the combined reconstruction, also merge the per-scan `features.h5` files into a single combined file so the localizer has one h5 covering all database images.

**Context:** Each local refinement run produces `refined/local/<scan_id>/sfm/features.h5`. Image names in these h5 files are the frame filenames (e.g. `dmt_scan_2024-06-26_10-26-52_000042.jpg`). Since scan IDs are unique timestamps, there are no name collisions across scans. We just need to copy all groups from each per-scan h5 into one combined h5.

**Files:**
- Modify: `global_main.py` (add merge call after `merge_aligned_scans`)
- Create: `utils/merge_features.py`

**Implementation:**

```python
# utils/merge_features.py
"""Merge per-scan features.h5 files into a single combined features file."""

import h5py
import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


def merge_features_h5(
    scan_feature_paths: List[Path],
    output_path: Path,
) -> Path:
    """Merge multiple hloc features.h5 files into one.
    
    Each scan's features.h5 contains groups keyed by image filename.
    Since scan IDs are unique timestamps baked into the filenames,
    there should be no collisions.
    
    Args:
        scan_feature_paths: Paths to per-scan features.h5 files.
        output_path: Where to write the merged features.h5.
        
    Returns:
        Path to the merged file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    total_images = 0
    with h5py.File(output_path, "w") as out_f:
        for feat_path in scan_feature_paths:
            if not feat_path.exists():
                logger.warning(f"Features file not found, skipping: {feat_path}")
                continue
            with h5py.File(feat_path, "r") as in_f:
                for image_name in in_f.keys():
                    if image_name in out_f:
                        logger.warning(
                            f"Duplicate image name '{image_name}' across scans, "
                            f"skipping from {feat_path}"
                        )
                        continue
                    in_f.copy(image_name, out_f)
                    total_images += 1

    logger.info(
        f"Merged features from {len(scan_feature_paths)} scans: "
        f"{total_images} images → {output_path}"
    )
    return output_path
```

**Addition to `global_main.py`** (after the `merge_aligned_scans` call around line 104):

```python
    # Merge per-scan feature files for localization use
    from utils.merge_features import merge_features_h5
    scan_feature_paths = [
        job_root_path / "refined" / "local" / scan_id / "sfm" / "features.h5"
        for scan_id in refined_aligned_scans.scan_ids
    ]
    merged_features_path = output_path / "refined_sfm_combined" / "features.h5"
    merge_features_h5(scan_feature_paths, merged_features_path)
```

**Commit:** `feat: merge per-scan features.h5 during global stitch for localization`

---

## Task 6: Real-data validation — holdout test on a DMT scan

**Objective:** Validate localization accuracy using a real DMT scan. Hold out a subset of frames from the reconstruction, then localize them as if they were new query images. Compare refined pose against the known ground-truth pose from the full reconstruction.

**Rationale:** This is the most honest test we can run before real-world deployment. The held-out images come from the same scan so the visual overlap is high — this is the *easy* case. Real-world queries will be harder (different lighting, time of day, wider baseline, noisier poses). But if it doesn't work on held-out frames from the same scan, it won't work on anything.

**Files:**
- Create: `tests/test_localize_holdout.py`

**Test design:**

```
Given a DMT scan dataset (images + ARposes.csv + CameraIntrinsics.csv):
  1. Run full local refinement → produces reconstruction + features.h5
  2. Hold out every 5th image (20% validation set)
  3. Rebuild a "database" reconstruction from the remaining 80%
     (just remove the held-out images from the reconstruction)
  4. For each held-out image:
     a. Use its ARKit pose as the "approximate pose"
     b. Run localize() against the 80% reconstruction
     c. Compare refined pose against its pose from the full reconstruction
  5. Report: median/mean/90th-percentile position error (cm) and rotation error (deg)
```

**What "ground truth" means here:** The full-reconstruction pose (after triangulation + BA) is the best pose we have. It's not absolute truth, but it's substantially better than the raw ARKit pose. The localization result should be close to this refined pose.

**Expected error budget:** For held-out frames from the same scan, position error should be < 5 cm median, rotation < 1°. If it's much worse, something is broken in the pipeline (pair selection, matching, or PnP).

**Implementation:**

```python
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
"""

from pathlib import Path
import argparse
import json
import logging
import sys
import numpy as np
import pycolmap
from copy import deepcopy

# Add project root to path
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
    # Clamp for numerical stability
    cos_angle = np.clip((np.trace(R_rel) - 1.0) / 2.0, -1.0, 1.0)
    rot_err_deg = np.degrees(np.arccos(cos_angle))

    return pos_err, rot_err_deg


def build_holdout_reconstruction(
    full_rec: pycolmap.Reconstruction,
    holdout_image_ids: set,
) -> pycolmap.Reconstruction:
    """Create a copy of the reconstruction with held-out images deregistered.
    
    Removes the held-out images and any 3D points that lose all their
    observations from the retained images. This simulates a reconstruction
    that was built without those frames.
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
    min_spacing: int = 2,
) -> set:
    """Select held-out image IDs.
    
    Takes every Nth image (where N = 1/fraction) to ensure spatial spread.
    Never holds out the first or last image (they anchor the BA gauge).
    """
    sorted_ids = sorted(rec.images.keys())
    
    # Don't hold out first/last two (BA gauge anchors)
    candidate_ids = sorted_ids[2:-2]
    
    step = max(1, int(1.0 / fraction))
    holdout = set(candidate_ids[::step])

    logger.info(
        f"Selected {len(holdout)} holdout images out of {len(sorted_ids)} "
        f"(step={step}, fraction={fraction:.0%})"
    )
    return holdout


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
        add_noise_m: Add Gaussian noise to approximate position (metres). 
                     0 = use exact reconstruction pose as approximate (easy mode).
                     0.5 = simulate ~50cm drift (realistic ARKit-level noise).
        add_noise_deg: Add Gaussian noise to approximate rotation (degrees).
    """
    features_h5 = scan_sfm_dir / "features.h5"
    assert scan_sfm_dir.exists(), f"SFM dir not found: {scan_sfm_dir}"
    assert features_h5.exists(), f"Features not found: {features_h5}"
    assert image_dir.exists(), f"Image dir not found: {image_dir}"

    # Load the full reconstruction (this is our "ground truth")
    logger.info(f"Loading full reconstruction from {scan_sfm_dir}")
    full_rec = pycolmap.Reconstruction(scan_sfm_dir)
    logger.info(f"Full reconstruction: {full_rec.num_reg_images()} images, "
                f"{len(full_rec.points3D)} 3D points")

    # Select holdout images
    holdout_ids = select_holdout_images(full_rec, holdout_fraction)

    # Build database reconstruction (without holdout images)
    db_rec = build_holdout_reconstruction(full_rec, holdout_ids)

    # Save the database reconstruction to a temp dir for the localizer
    import tempfile
    with tempfile.TemporaryDirectory(prefix="holdout_db_") as tmp_dir:
        db_sfm_dir = Path(tmp_dir) / "db_sfm"
        db_sfm_dir.mkdir()
        db_rec.write(db_sfm_dir)

        # Build localizer against the database reconstruction
        # features.h5 still contains all images (including holdout),
        # but the localizer only matches against images in the reconstruction
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
            approx_cfw = deepcopy(gt_cam_from_world)
            if add_noise_m > 0 or add_noise_deg > 0:
                # Add position noise
                world_from_cam = approx_cfw.inverse()
                noise_t = rng.randn(3) * add_noise_m
                world_from_cam_noisy = pycolmap.Rigid3d(
                    world_from_cam.rotation,
                    world_from_cam.translation + noise_t,
                )
                # Add rotation noise (small random rotation)
                if add_noise_deg > 0:
                    angle = rng.randn() * np.radians(add_noise_deg)
                    axis = rng.randn(3)
                    axis /= np.linalg.norm(axis)
                    from scipy.spatial.transform import Rotation as R
                    noise_rot = R.from_rotvec(axis * angle).as_matrix()
                    noisy_R = noise_rot @ world_from_cam_noisy.rotation.matrix()
                    world_from_cam_noisy = pycolmap.Rigid3d(
                        pycolmap.Rotation3d(noisy_R),
                        world_from_cam_noisy.translation,
                    )
                approx_cfw = world_from_cam_noisy.inverse()

            # Localize
            result = localizer.localize(
                image_path=image_path,
                camera=camera,
                approximate_cam_from_world=approx_cfw,
                query_name=image.name,
            )

            entry = {
                "image_id": img_id,
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
                entry["pos_error_m"] = pos_err
                entry["rot_error_deg"] = rot_err
                logger.info(
                    f"  {image.name}: pos_err={pos_err*100:.1f}cm, "
                    f"rot_err={rot_err:.2f}°, "
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
    print(f"Success: {summary['successes']} / {summary['total_queries']} "
          f"({summary['success_rate']:.0%})")
    if pos_errors:
        pe = summary["pos_error_cm"]
        re = summary["rot_error_deg"]
        print(f"Position error (cm): median={pe['median']:.1f}, "
              f"mean={pe['mean']:.1f}, p90={pe['p90']:.1f}, max={pe['max']:.1f}")
        print(f"Rotation error (°):  median={re['median']:.2f}, "
              f"mean={re['mean']:.2f}, p90={re['p90']:.2f}, max={re['max']:.2f}")
    print("=" * 60)

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(output_dir / "holdout_results.json", "w") as f:
            json.dump({"summary": summary, "per_image": results}, f, indent=2)
        logger.info(f"Results saved to {output_dir / 'holdout_results.json'}")

    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Holdout localization test")
    parser.add_argument("--scan_sfm_dir", type=Path, required=True,
                        help="Path to refined/local/<scan_id>/sfm/")
    parser.add_argument("--image_dir", type=Path, required=True,
                        help="Path to datasets/<scan_id>/Frames/")
    parser.add_argument("--holdout_fraction", type=float, default=0.2)
    parser.add_argument("--output_dir", type=Path, default=Path("tests/localize_holdout_results"))
    parser.add_argument("--noise_m", type=float, default=0.0,
                        help="Gaussian position noise (metres) to add to approximate pose")
    parser.add_argument("--noise_deg", type=float, default=0.0,
                        help="Gaussian rotation noise (degrees) to add to approximate pose")
    args = parser.parse_args()

    run_holdout_test(
        scan_sfm_dir=args.scan_sfm_dir,
        image_dir=args.image_dir,
        holdout_fraction=args.holdout_fraction,
        output_dir=args.output_dir,
        add_noise_m=args.noise_m,
        add_noise_deg=args.noise_deg,
    )
```

**Test protocol — run three times:**

| Run | `--noise_m` | `--noise_deg` | What it tests |
|-----|-------------|---------------|---------------|
| 1. Easy | 0.0 | 0.0 | Pipeline correctness. Approximate pose = ground truth. Should get <1cm, <0.1° error. If not, the matching/PnP is broken. |
| 2. Realistic | 0.5 | 5.0 | ARKit-level drift. ~50cm position noise, ~5° rotation noise. Checks that pair selection + PnP can recover from typical approximate poses. Target: <5cm median, <1° median. |
| 3. Stress | 2.0 | 15.0 | Large drift. Checks graceful degradation. Some failures expected. Success rate and error distribution reveal how far the frustum-based pair selection can tolerate. |

**Limitations of this test (what it does NOT cover):**
- Different lighting conditions (same scan = same lighting)
- Different viewpoints (holdout frames are from the same trajectory, real queries may come from novel positions)
- Motion blur, different camera (same device, same session)
- Temporal changes to the scene (moved furniture, people)

These limitations are real and matter for production. But the holdout test catches pipeline bugs (wrong coordinate conventions, broken matching, bad pair selection) and gives a lower bound on accuracy.

**Commit:** `test: add holdout localization test with noise levels`

---

## Task 7: Upload features.h5 from Rust runners

**Objective:** Both the local and global Rust runners need to include `features.h5` in their uploads so that the localization capability has access to precomputed database features.

**Context:**
- **Local runner** (`runner-reconstruction-local/src/refined.rs`): Zips the `sfm/` directory for each scan, but the zip filter only allows `.bin`, `.csv`, `.txt` extensions — `features.h5` is excluded.
- **Global runner** (`runner-reconstruction-global/src/output.rs`): Uploads individual files from `refined/global/refined_sfm_combined/` — lists only the `.bin` COLMAP files. After Task 5, `features.h5` will also exist there but won't be uploaded.

**Changes needed:**

### Local runner — `refined.rs`

Add `.h5` to the allowed zip extensions:

```rust
// Line 16, refined.rs
const ZIP_ALLOWED_EXTENSIONS: &[&str] = &[".bin", ".csv", ".txt", ".h5"];
```

That's it — the features.h5 file sits in the `sfm/` folder alongside the `.bin` files, so it'll get picked up by the existing `zip_directory` logic.

### Global runner — `output.rs`

Add the merged features.h5 as an optional output:

```rust
// Add to GLOBAL_OUTPUTS array (after the rigs.bin entry):
    OutputSpec {
        relative_path: "refined/global/refined_sfm_combined/features.h5",
        display_name: "colmap_features_h5",
        mandatory: false,
    },
```

And add the data type mapping:

```rust
// Add to data_type_for_display match:
        "colmap_features_h5" => "colmap_features_h5",
```

**Note:** `features.h5` is marked `mandatory: false` because existing reconstructions (before this feature) won't have it, and the global runner should still succeed for non-localization jobs.

**Commit:** `feat: upload features.h5 from local and global Rust runners`

---

## Resolved Design Questions

1. ✅ **`features_ref` is supported** on `auki-master`. Line 168 of `hloc/match_features.py`: `features_ref: Optional[Path] = None`. The `FeaturePairsDataset` reads `name0` from query h5 and `name1` from reference h5 — cross-file matching works.

2. ✅ **Feature cache for database images** — Solved by Task 5: merge per-scan `features.h5` into a combined file at global stitch time. For single-scan localization, the existing `sfm/features.h5` is used directly.

## Remaining Design Notes

1. **Coordinate convention for the API** — The plan uses OpenGL convention for the approximate pose input (matching ARKit/scan data convention used throughout the codebase — see `convert_pose_opengl_to_colmap` in `data_utils.py`). Confirm this is what the client will send.

2. **Performance considerations for production:**
   - The voxel frustum march uses 16×16 = 256 rays. For ~100k voxels this is plenty fast (~ms). Can tune up for accuracy.
   - Feature extraction on the query image (~50-100ms on GPU for ALIKED) can run in parallel with the voxel query (noted in the plan, not yet implemented — straightforward future optimization with `concurrent.futures`).
   - The `SingleImageLocalizer` caches the spatial index so reconstruction loading + indexing only happens once.

3. **Octree vs. flat voxel grid** — Started with a flat `defaultdict` voxel grid since it's simpler and fast enough for typical reconstruction sizes (100k-1M points → 10k-100k voxels). If reconstructions grow much larger, can swap to an octree without changing the API.
