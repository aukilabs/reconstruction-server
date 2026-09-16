"""Spatial index for finding database images that observe the same 3D regions as a query camera.

Builds a voxel grid over a COLMAP reconstruction's 3D points. For each voxel cell,
stores the set of image IDs whose registered 2D observations project into that cell.

This enables efficient "which database images see roughly the same scene as this query
camera?" queries *without* requiring the query to already have 2D-3D correspondences
(unlike hloc's pairs_from_covisibility which needs a COLMAP model with the query
registered).

Unlike pairs_from_poses (which ranks by camera position proximity), this accounts for
*what is in front of the camera*, not just where the camera is.
"""

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

    Build once per reconstruction, reuse across queries.
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
        num_ray_samples: int = 16,
    ) -> List[Tuple[int, int, int]]:
        """Find voxels that fall inside the query camera's view frustum.

        Strategy: cast rays from the camera centre through a grid of pixels
        (subsampled for speed) and step along each ray, collecting occupied
        voxels. This is a conservative approximation -- it may miss some edge
        voxels, but that's fine for pair selection (we only need a good ranking,
        not perfect recall).

        Args:
            cam_from_world: Query camera extrinsic (world -> camera transform).
            camera: Query camera intrinsic model.
            max_depth: Maximum ray depth in metres.
            depth_step: Step size along each ray. Defaults to voxel_size.
            num_ray_samples: Grid dimension for pixel subsampling (NxN rays).

        Returns:
            List of occupied voxel keys (i,j,k) visible from this camera.
        """
        if depth_step is None:
            depth_step = self.voxel_size

        world_from_cam = cam_from_world.inverse()
        cam_center = world_from_cam.translation
        R_world_from_cam = world_from_cam.rotation.matrix()

        w, h = camera.width, camera.height

        # Subsample pixel grid
        us = np.linspace(0, w - 1, num_ray_samples)
        vs = np.linspace(0, h - 1, num_ray_samples)
        uu, vv = np.meshgrid(us, vs)
        pixels = np.stack([uu.ravel(), vv.ravel()], axis=-1)  # (N, 2)

        # Unproject pixels to camera-frame unit rays
        rays_cam = np.array([camera.cam_from_img(p) for p in pixels])  # (N, 2)
        # Make 3D: (x, y, 1) in camera frame, then normalize
        rays_cam_3d = np.column_stack([rays_cam, np.ones(len(rays_cam))])
        norms = np.linalg.norm(rays_cam_3d, axis=1, keepdims=True)
        norms[norms < 1e-8] = 1.0
        rays_cam_3d /= norms

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
        # Return with dummy score 0 (fallback)
        return [(img_id, 0) for img_id, _ in dists[:num_images]]

    def get_point3d_ids_in_voxels(
        self, voxel_keys: List[Tuple[int, int, int]]
    ) -> List[int]:
        """Get all 3D point IDs contained in the given voxels."""
        point_ids = []
        for key in voxel_keys:
            point_ids.extend(self.voxel_to_point3d_ids.get(key, []))
        return point_ids
