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
from contextlib import contextmanager
import alphashape
from shapely.geometry import Polygon, GeometryCollection
import matplotlib.pyplot as plt

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

def extrude_outline_to_mesh(outline, min_y, max_y, color, logger):
    vertices = np.empty((len(outline) * 2, 3))
    for i in range(len(outline)):
        vertices[i * 2] = np.array([outline[i][0], min_y, outline[i][1]])
        vertices[i * 2 + 1] = np.array([outline[i][0], max_y, outline[i][1]])
    
    triangles = np.empty((len(outline) * 2, 3))
    for i in range(len(outline)):
        triangles[i * 2] = np.array([i * 2, i * 2 + 1, (i + 1) * 2])
        triangles[i * 2 + 1] = np.array([i * 2 + 1, (i + 1) * 2 + 1, (i + 1) * 2])

    mesh = o3d.geometry.TriangleMesh()
    mesh.vertices = o3d.utility.Vector3dVector(vertices)
    mesh.triangles = o3d.utility.Vector3iVector(triangles)
    mesh.paint_uniform_color(color)

    return mesh

def fit_cluster_mesh_3D(cluster_pcd, cluster_points, color, logger):
    # Three step approach for a tight fit without holes, around a noisy sparse point cloud
    # 1. Create low poly alpha mesh first to avoid holes and get good outward-facing normals
    # 2. Subdivide to more polygons
    # 3. "shrink wrap": snap each vertex onto the point cloud nearby.
    lowpoly = o3d.geometry.TriangleMesh.create_from_point_cloud_alpha_shape(cluster_pcd, alpha=0.8)
    lowpoly = lowpoly.filter_smooth_simple(number_of_iterations=1)
    lowpoly.compute_vertex_normals()
    
    highpoly = lowpoly.subdivide_midpoint(number_of_iterations=2)

    pointcloud_search_tree = o3d.geometry.KDTreeFlann(cluster_pcd) # For fast querying of nearby points many times
    snapped_count = 0
    snapped_distance_sum = 0.0
    not_snapped_count = 0
    for i in range(len(highpoly.vertices)):
        vertex = highpoly.vertices[i]

        # Snap the vertex onto point cloud (basically 'shink-wrapping' the alpha shape tigher to the point cloud without introducing holes)
        # IMPORTANT: Snap to the average of several nearby points to ignore outlier points better.
        _, neighbor_indices, _ = pointcloud_search_tree.search_hybrid_vector_3d(vertex, 0.3, 7)
        if len(neighbor_indices) < 5:
            # Try again with bigger radius
            _, neighbor_indices, _ = pointcloud_search_tree.search_hybrid_vector_3d(vertex, 0.5, 10)
            if len(neighbor_indices) == 0:
                not_snapped_count += 1
                continue

        neighbor_points = cluster_points[neighbor_indices]
        middle_point = np.mean(neighbor_points, axis=0)
        
        snapped_distance_sum += np.linalg.norm(vertex - middle_point) # Just for stats logging
        highpoly.vertices[i] = middle_point
        snapped_count += 1

    logger.info(f"Snapped {snapped_count} vertices onto point cloud. Average distance: {snapped_distance_sum / snapped_count:.4f}. {not_snapped_count} vertices not snapped.")

    cluster_mesh = highpoly
    cluster_mesh = cluster_mesh.filter_smooth_simple(number_of_iterations=2)
    cluster_mesh = cluster_mesh.simplify_quadric_decimation(target_number_of_triangles=len(cluster_mesh.triangles) // 4)
    cluster_mesh.compute_vertex_normals()
    cluster_mesh.paint_uniform_color(color)

    return cluster_mesh

def fit_cluster_outline(cluster_pcd, cluster_points, color, logger, debug=False):
    
    logger.info(f"Fitting top-down cluster outline to {len(cluster_points)} points")
    if len(cluster_points) < 100:
        logger.warning(f"Skip outline for cluster with few points (point count: {len(cluster_points)})")
        return None

    points2D = cluster_points[:, [0,2]] # Ignore Y which is up axis, to get a "topdown" outline, like a floor plan
    heights = cluster_points[:, 1]
    lower_cutoff = np.percentile(heights, 10)
    upper_cutoff = np.percentile(heights, 90)

    print(f"Skipping points lower than {lower_cutoff} and higher than {upper_cutoff}")
    old_count = len(points2D)
    too_low_indicies = heights < lower_cutoff
    too_high_indicies = heights > upper_cutoff
    points2D = points2D[~too_low_indicies & ~too_high_indicies]
    print(f"Ignoring {old_count - len(points2D)} points. ({len(points2D)} remaining)")

    if debug:
        fig, ax = plt.subplots(figsize=(10, 10))
        ax.scatter(points2D[:,0], points2D[:,1], marker='.', s=0.2)
        ax.set_aspect('equal')
        plt.savefig("cluster_points.png")
        plt.close()
        plt.show()
    
    alpha_shape = None
    try:
        alpha_shape = alphashape.alphashape(points2D, 2.0)
    except Exception as e:
        logger.warning(f"failed to extract alpha shape: {e}")
        return None

    # Check if the result is a Polygon, MultiPolygon, or GeometryCollection
    if isinstance(alpha_shape, Polygon):
        # If it's a single Polygon, extract the exterior
        envelop = alpha_shape.oriented_envelope
        intersection_area = alpha_shape.intersection(envelop).area
        if (intersection_area / envelop.area < 0.95):
            logger.debug("simplifying alpha shape")
            shape = alpha_shape.simplify(0.3, preserve_topology=True)
        else:
            logger.debug("rectangle fit is very close, using it")
            shape = envelop

        x, y = shape.exterior.xy
        exterior_coords = np.column_stack([x, y])
        x_raw, y_raw = alpha_shape.exterior.xy
        raw_exterior_coords = np.column_stack([x_raw, y_raw])
        
    elif isinstance(alpha_shape, GeometryCollection):
        logger.debug("Unsupported alpha shape type: " + type(alpha_shape))
        for geom in alpha_shape.geoms:
            logger.debug("- child type: " + type(geom))
        return None
    else:
        logger.debug("Unsupported alpha shape type: " + type(alpha_shape))
        return None

    if debug:
        fig, ax = plt.subplots(figsize=(10, 10))
        ax.scatter(points2D[:,0], points2D[:,1], marker='.', s=0.2)
        ax.plot(exterior_coords[:,0], exterior_coords[:,1], marker='x')
        ax.set_aspect('equal')
        plt.savefig("cluster_outline.png")
        plt.close()
        plt.show()
        
        fig, ax = plt.subplots(figsize=(10, 10))
        ax.scatter(points2D[:,0], points2D[:,1], marker='.', s=0.2)
        ax.plot(raw_exterior_coords[:,0], raw_exterior_coords[:,1], marker='x')
        ax.set_aspect('equal')
        plt.savefig("cluster_outline_raw.png")
        plt.close()
        plt.show()
        
    return exterior_coords, raw_exterior_coords, lower_cutoff, upper_cutoff

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
        logger.setLevel(logging.DEBUG)
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

    meshes = []
    outlines = []
    with timed_section_scope("Clustering", logger):
        logger.info(f"Clustering 'above floor' points")

        labels = pcd.cluster_dbscan(eps=0.25, min_points=10)
        unique_labels = np.unique(labels)
        unique_labels = unique_labels[unique_labels != -1] # -1 are points which didn't end up in any cluster

        logger.info(f"Found {len(unique_labels)} clusters")

        for label in unique_labels:
            cluster_points = np.array(pcd.points)[labels == label]
            logger.info(f"Cluster {label} has {len(cluster_points)} points")

            cluster_pcd = o3d.geometry.PointCloud()
            cluster_pcd.points = o3d.utility.Vector3dVector(cluster_points)

            invalid = False
            if len(cluster_points) < 100:
                invalid = True
                logger.info(f"Skip cluster {label}: Too few points")
            else:
                bounding_box = cluster_pcd.get_oriented_bounding_box()
                bounding_box_min_y = bounding_box.get_min_bound()[1]
                if bounding_box_min_y > args.floor_height + 0.5:
                    invalid = True
                    logger.info(f"Skip cluster {label}: Floating in air")
                else:
                    bounding_box_max_y = bounding_box.get_max_bound()[1]
                    if bounding_box_max_y - bounding_box_min_y < 0.2:
                        invalid = True
                        logger.info(f"Skip cluster {label}: Too flat")

            if invalid:
                continue

            color = hsv_to_rgb(((label * 3) % len(unique_labels)) / len(unique_labels), 0.75, 1.0)
            cluster_mesh = fit_cluster_mesh_3D(cluster_pcd, cluster_points, color, logger)
            meshes.append(cluster_mesh)

            cluster_outline = fit_cluster_outline(cluster_pcd, cluster_points, color, logger, debug=True)
            if cluster_outline is not None:
                cluster_outline, raw_cluster_outline, lower_cutoff, upper_cutoff = cluster_outline
                #outline_mesh = extrude_outline_to_mesh(cluster_outline, bounding_box_min_y, bounding_box_max_y, color, logger)
                #meshes.append(outline_mesh)
                outlines.append(cluster_outline)

            #break

    if outlines:
        fig, ax = plt.subplots(figsize=(10, 10))
        for outline in outlines:
            ax.plot(outline[:,0], outline[:,1], marker='x')
        ax.set_aspect('equal')
        plt.savefig("merged_outlines.png")
        plt.close()
        plt.show()

    if meshes:
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