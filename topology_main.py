from pathlib import Path
import open3d as o3d
from utils.io import Model
import argparse
from utils.data_utils import setup_logger
import numpy as np
import time
import logging
import sys
from colorsys import hsv_to_rgb


# Convention in this file:
# Right-handed coordinates (x,y,z) with positive y as global up

def segment_floor_points(pcd, floor_height, floor_height_threshold):
    points = np.asarray(pcd.points)
    floor = points[
        (points[:, 1] < floor_height + floor_height_threshold) & 
        (points[:, 1] > floor_height - floor_height_threshold)
    ]
    above_floor = points[points[:, 1] >= floor_height + floor_height_threshold]
    below_floor = points[points[:, 1] < floor_height - floor_height_threshold]

    return floor, above_floor, below_floor

from contextlib import contextmanager

@contextmanager
def timed_section_scope(name, logger):
    if logger is None:
        logger = setup_logger(name=name)
    time_start = time.time()
    logger.info(f"{name}...")
    yield
    logger.info(f"{name} DONE. Took {time.time() - time_start:.4f} seconds")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=Path, required=True)
    parser.add_argument('--output_dir', type=Path, required=True)
    parser.add_argument('--floor_height', type=float, default=0.0)
    parser.add_argument('--floor_height_threshold', type=float, default=0.2, help='Max height offset from [floor_height] to count as floor, in meters')
    parser.add_argument('--voxel_size', type=float, default=0.05, help='Voxel size for clustering, in meters')

    return parser.parse_args()


def main(args, logger=None):
    if logger is None:
        logger = logging.getLogger('topology')
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(sys.stdout))

    if args.input_path.suffix == '.ply':
        pcd = o3d.io.read_point_cloud(args.input_path)
    else:
        model = Model()
        model.read_model(path=args.input_path, ext='.bin', logger=logger)
        pcd = model.get_points(in_opengl=True)

    if not args.output_dir.exists():
        args.output_dir.mkdir(parents=True)

    logger.info(f"Loaded point cloud with {len(pcd.points)} points")

    with timed_section_scope("Floor segmentation", logger):
        floor, above_floor, below_floor = segment_floor_points(pcd, args.floor_height, args.floor_height_threshold)    
        logger.info(f"Segmented points into: {len(floor)} floor, {len(above_floor)} above floor, {len(below_floor)} below floor")

    logger.info(f"Discarding {len(below_floor) + len(floor)} points on floor or below.")
    pcd = o3d.geometry.PointCloud()
    pcd.points = o3d.utility.Vector3dVector(above_floor)

    with timed_section_scope("Voxel Downsampling", logger):
        old_count = len(pcd.points)
        pcd = pcd.voxel_down_sample(voxel_size=args.voxel_size)
        logger.info(f"Downsampled using voxel size {args.voxel_size}, from {old_count} to {len(pcd.points)} points")

    with timed_section_scope("Outlier Removal", logger):
        old_count = len(pcd.points)
        [pcd, _] = pcd.remove_statistical_outlier(nb_neighbors=10, std_ratio=2.5)
        logger.info(f"Removed {old_count - len(pcd.points)} outlier points")

    meshes = []
    with timed_section_scope("Clustering", logger):
        logger.info(f"Clustering 'above floor' points")

        labels = pcd.cluster_dbscan(eps=0.35, min_points=100)
        unique_labels = np.unique(labels)
        unique_labels = unique_labels[unique_labels != -1] # -1 are points which didn't end up in any cluster

        logger.info(f"Found {len(unique_labels)} clusters")

        total_points_in_any_cluster = 0
        floating_in_air_clusters = 0
        too_few_points_clusters = 0
        too_flat_clusters = 0
        for label in unique_labels:
            cluster_points = np.array(pcd.points)[labels == label]

            cluster_pcd = o3d.geometry.PointCloud()
            cluster_pcd.points = o3d.utility.Vector3dVector(cluster_points)

            invalid = False
            if len(cluster_points) < 100:
                invalid = True
                too_few_points_clusters += 1
                logger.debug(f"Skip cluster {label}: Too few points (count: {len(cluster_points)})")
            else:
                bounding_box = cluster_pcd.get_axis_aligned_bounding_box()
                bounding_box_min_y = bounding_box.get_min_bound()[1]
                if bounding_box_min_y > args.floor_height + args.floor_height_threshold + 0.5:
                    invalid = True
                    floating_in_air_clusters += 1
                    logger.debug(f"Skip cluster {label}: Floating in air (count: {len(cluster_points)}, lowest Y: {bounding_box_min_y:.3f})")
                else:
                    bounding_box_max_y = bounding_box.get_max_bound()[1]
                    if bounding_box_max_y - bounding_box_min_y < 0.2:
                        invalid = True
                        too_flat_clusters += 1
                        logger.debug(f"Skip cluster {label}: Too flat (count: {len(cluster_points)}, height: {bounding_box_max_y - bounding_box_min_y:.3f})")

            if invalid:
                continue

            logger.debug(f"Keeping cluster {label} with {len(cluster_points)} points")
            total_points_in_any_cluster += len(cluster_points)

            color = hsv_to_rgb(float((label * 3) % len(unique_labels)) / len(unique_labels), 0.75, 1.0)
            mesh = o3d.geometry.TriangleMesh.create_from_point_cloud_alpha_shape(cluster_pcd, alpha=0.25)

            mesh.paint_uniform_color(color)
            meshes.append(mesh)

    total_skipped_clusters = too_few_points_clusters + floating_in_air_clusters + too_flat_clusters
    logger.info(f"Skipped {total_skipped_clusters} clusters (too few points: {too_few_points_clusters}, floating in air: {floating_in_air_clusters}, too flat: {too_flat_clusters})")
    logger.info(f"Kept {len(unique_labels) - total_skipped_clusters} clusters with a total of {total_points_in_any_cluster} points")

    if meshes:
        logger.info(f"Merging {len(meshes)} cluster meshes into a single mesh")
        with timed_section_scope("Merging meshes", logger):
            merged_mesh = o3d.geometry.TriangleMesh()
            for mesh in meshes:
                merged_mesh += mesh

            o3d.io.write_triangle_mesh(args.output_dir / "topology.glb", merged_mesh)
            o3d.io.write_triangle_mesh(args.output_dir / "topology.obj", merged_mesh)
        
        with timed_section_scope("Generating lowpoly meshes", logger):
            full_tri_count = len(merged_mesh.triangles)
            downsampled = merged_mesh
            ratio = 1.0
            for i in range(2):
                downsampled = downsampled.filter_smooth_laplacian(number_of_iterations=1)
                ratio /= 3
                downsampled = downsampled.simplify_quadric_decimation(target_number_of_triangles=int(full_tri_count * ratio))
                o3d.io.write_triangle_mesh(args.output_dir / f"topology_downsampled_{ratio:.3f}.glb", downsampled)
                o3d.io.write_triangle_mesh(args.output_dir / f"topology_downsampled_{ratio:.3f}.obj", downsampled)

if __name__ == "__main__":
    args = parse_args()
    main(args)