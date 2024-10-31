import itertools
from typing import Dict, List
import numpy as np
import pycolmap
import pyceres
from pathlib import Path
import open3d as o3d
from sklearn.linear_model import LinearRegression
from sklearn.cluster import DBSCAN
from collections import defaultdict
import random
import matplotlib.pyplot as plt
import alphashape
from shapely.geometry import Polygon
from scipy.spatial import ConvexHull
from scipy.spatial._qhull import QhullError

from utils.data_utils import get_world_space_qr_codes
from utils.bundle_adjuster import PyBundleAdjuster

from src.cost_functions import RelativeTransformationSim3CostFunction


def dmt_global_stitching(detections_per_qr,
                         image_ids_per_qr,
                         timestamp_per_image,
                         arkit_precomputed,
                         reconstruction,
                         ba_options,
                         ba_config):

    refinement_config = {
        'add_rel_constraints': True,
        'use_arkit_relposes': False,
        'use_arkit_centerdist': False,
        #'min_point3d_track_length': 20, # Only add cost terms for 3D points with long tracks
    }

    bundle_adjuster = PyBundleAdjuster(ba_options, ba_config, refinement_config)
    bundle_adjuster.set_up_problem(
        reconstruction,
        ba_options.create_loss_function(),
        timestamp_per_image,
        detections_per_qr,
        image_ids_per_qr,
        arkit_precomputed,
        verbose=False
    )

    solver_options = bundle_adjuster.set_up_solver_options(
        bundle_adjuster.problem, ba_options.solver_options
    )
    solver_options.linear_solver_type = pyceres.LinearSolverType.SPARSE_SCHUR
    solver_options.minimizer_progress_to_stdout = True
    solver_options.logging_type = pyceres.LoggingType.PER_MINIMIZER_ITERATION

    initial_loss_breakdown, initial_loss_breakdown_per_image_id = bundle_adjuster.evaluate_loss_breakdown()
    print("------------")
    print("INITIAL LOSS BREAKDOWN:")
    print("\n".join(f"{category}: {loss}" for category, loss in initial_loss_breakdown.items()))
    print("------------")

    summary = pyceres.SolverSummary()
    pyceres.solve(solver_options, bundle_adjuster.problem, summary)
    final_loss_breakdown, final_loss_breakdown_per_image_id = bundle_adjuster.evaluate_loss_breakdown()

    print("------------")
    print("INITIAL LOSS BREAKDOWN:")
    print("\n".join(f"{category}: {loss}" for category, loss in initial_loss_breakdown.items()))
    print("------------")
    print("FINAL LOSS BREAKDOWN:")
    print("\n".join(f"{category}: {loss}" for category, loss in final_loss_breakdown.items()))
    print("------------")

    if not summary.IsSolutionUsable():
        print("\n".join(summary.FullReport().split(",")))
        print("Solution not usable!")
        raise Exception("Solver failed! No usable solution found.")

    print("Solved")
    loss_details = (
        initial_loss_breakdown,
        initial_loss_breakdown_per_image_id,
        final_loss_breakdown,
        final_loss_breakdown_per_image_id
    )

    return (summary, loss_details)


def run_stitching(detections_per_qr,
                  image_ids_per_qr,
                  timestamp_per_image,
                  arkit_precomputed,
                  combined_rec,
                  sorted_image_ids,
                  global_ba=True):

    if global_ba:
        # Run bundle adjustment
        ba_options = pycolmap.BundleAdjustmentOptions()
        ba_options.refine_focal_length = False
        ba_options.refine_extra_params = False
        ba_options.refine_principal_point = False
        ba_options.solver_options.max_num_iterations = 150
        ba_options.solver_options.num_threads = 16

        # Configure bundle adjustment
        ba_config = pycolmap.BundleAdjustmentConfig()

        for image_id in sorted_image_ids:
            ba_config.add_image(image_id)

        for point_id in combined_rec.point3D_ids():
            ba_config.add_variable_point(point_id)

        # Fix 7-DOFs of the bundle adjustment problem
        # ba_config.set_constant_cam_pose(sorted_image_ids[0])
        # ba_config.set_constant_cam_positions(sorted_image_ids[1], [0])

        for image_id in combined_rec.images:
            if image_id not in arkit_precomputed:
                ba_config.set_constant_cam_pose(image_id)

        print("Start Global Bundle Adjustment")
        summary, loss_details = dmt_global_stitching(detections_per_qr,
                                                     image_ids_per_qr,
                                                     timestamp_per_image,
                                                     arkit_precomputed,
                                                     combined_rec,
                                                     ba_options,
                                                     ba_config)

        if summary is not None:
            print("\n".join(summary.FullReport().split(",")))

    combined_out_dir = Path("/tmp/stitched_rec")
    combined_out_dir.mkdir(exist_ok=True, parents=True)
    combined_rec.write(combined_out_dir)

    combined_detections = get_world_space_qr_codes(combined_rec, detections_per_qr, image_ids_per_qr)
    #save_qr_poses_csv(combined_detections, combined_out_dir / "refined_portal_poses.csv")
    
    print("\n-------------\n")

    return combined_rec, combined_detections


def filter_reconstruction(reconstruction, normalize_points=False):
    reconstruction.filter_all_points3D(4.0, 1.5)
    reconstruction.filter_observations_with_negative_depth()
    if normalize_points:
        reconstruction.normalize(5.0, 0.1, 0.9, True)
    return reconstruction


def align_reconstruction_chunks(
        reconstruction: pycolmap.Reconstruction,
        chunks_image_ids: List[List[int]],
        detections_per_qr: Dict[str, List[pycolmap.Rigid3d]],
        image_ids_per_qr: Dict[str, List[int]],
        with_scale: bool = True
    ):

    t_local_chunk_quat = [pycolmap.Rigid3d().rotation.quat for _ in range(len(chunks_image_ids))]
    t_local_chunk_translation = [pycolmap.Rigid3d().translation for _ in range(len(chunks_image_ids))]
    image_id_to_chunk_id = {image_id : chunk_id for chunk_id, image_ids in enumerate(chunks_image_ids) for image_id in image_ids}
    problem = pyceres.Problem()

    qr_ids_per_chunk = [set() for _ in range(len(chunks_image_ids))]
    connected_chunks = [set() for _ in range(len(chunks_image_ids))]
    for qr_id, cam_space_detections in detections_per_qr.items():
        assert qr_id in image_ids_per_qr and len(image_ids_per_qr[qr_id]) == len(cam_space_detections)
        image_ids = image_ids_per_qr[qr_id]

        for (image_id_ref, t_refcam_qr), (image_id_tgt, t_tgtcam_qr) in set(itertools.combinations(zip(image_ids, cam_space_detections), 2)):
            assert image_id_ref != image_id_tgt

            chunk_id_ref, chunk_id_tgt = image_id_to_chunk_id[image_id_ref], image_id_to_chunk_id[image_id_tgt]

            if chunk_id_ref == chunk_id_tgt:
                continue

            t_refworld_qr = reconstruction.image(image_id_ref).cam_from_world.inverse() * t_refcam_qr
            t_tgtworld_qr = reconstruction.image(image_id_tgt).cam_from_world.inverse() * t_tgtcam_qr

            cost = RelativeTransformationSim3CostFunction(t_refworld_qr.rotation.quat,
                                                          t_refworld_qr.translation,
                                                          t_tgtworld_qr.rotation.quat,
                                                          t_tgtworld_qr.translation, np.eye(6))

            params = [
                t_local_chunk_quat[chunk_id_tgt],
                t_local_chunk_translation[chunk_id_tgt],
                t_local_chunk_quat[chunk_id_ref],
                t_local_chunk_translation[chunk_id_ref]
            ]

            problem.add_residual_block(cost, None, params)
            qr_ids_per_chunk[chunk_id_ref].add(qr_id)
            qr_ids_per_chunk[chunk_id_tgt].add(qr_id)
            connected_chunks[chunk_id_ref].add(chunk_id_tgt)
            connected_chunks[chunk_id_tgt].add(chunk_id_ref)

    if with_scale:
        for chunk_idx in range(len(chunks_image_ids)):
            if len(qr_ids_per_chunk[chunk_idx]) < 2:
                chunks_to_fix_scale = list(connected_chunks[chunk_idx]) + [chunk_idx]
                print(f'Chunk {chunk_idx} has less than 2 correspondences, fixing scale for chunks {chunks_to_fix_scale}.')
                for chunk_fix_idx in chunks_to_fix_scale:
                    quat = t_local_chunk_quat[chunk_fix_idx]
                    if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
                        problem.set_manifold(quat, pyceres.QuaternionManifold())
    else:
        for quat in t_local_chunk_quat:
            if problem.has_parameter_block(quat) and not problem.is_parameter_block_constant(quat):
                problem.set_manifold(quat, pyceres.QuaternionManifold())

    solver_options = pyceres.SolverOptions()
    solver_options.linear_solver_type = pyceres.LinearSolverType.SPARSE_SCHUR
    solver_options.minimizer_progress_to_stdout = True
    solver_options.function_tolerance = 0.0
    solver_options.gradient_tolerance = 0.0
    solver_options.max_num_iterations = 500
    solver_options.logging_type = pyceres.LoggingType.PER_MINIMIZER_ITERATION

    summary = pyceres.SolverSummary()
    pyceres.solve(solver_options, problem, summary)
    print(summary.FullReport())

    t_local_chunks = [pycolmap.Sim3d(pycolmap.Rotation3d(quat).norm()**2, pycolmap.Rotation3d(quat), translation) for quat, translation in zip(t_local_chunk_quat, t_local_chunk_translation)]
    for t_local_chunk in t_local_chunks:
        t_local_chunk.rotation.normalize()

    print('Refined Sim3 transforms:')
    for chunk_idx, t_local_chunk in enumerate(t_local_chunks):
        print(f'Chunk {chunk_idx} ({len(chunks_image_ids[chunk_idx]):5,d} images): {t_local_chunk}')

    for image_id in reconstruction.images.keys():
        chunk_id = image_id_to_chunk_id[image_id]
        reconstruction.images[image_id].cam_from_world = pycolmap.Sim3d.transform_camera_world(t_local_chunks[chunk_id], reconstruction.images[image_id].cam_from_world)

    for point3D_id, point3D in reconstruction.points3D.items():
        if len(point3D.track.elements) == 0:
            continue

        chunk_id = image_id_to_chunk_id[point3D.track.elements[0].image_id] # assumes 3D points are never seen from multiple chunks
        reconstruction.points3D[point3D_id].xyz = t_local_chunks[chunk_id] * point3D.xyz

    for qr_id, cam_space_detections in detections_per_qr.items():
        for det_idx, (image_id, _) in enumerate(zip(image_ids_per_qr[qr_id], cam_space_detections)):
            chunk_id = image_id_to_chunk_id[image_id]
            detections_per_qr[qr_id][det_idx].translation *= t_local_chunks[chunk_id].scale

    return


def voxelise(pcd, voxel_size):
    voxel_grid = o3d.geometry.VoxelGrid.create_from_point_cloud(pcd, voxel_size=voxel_size)

    # Extract voxel centroids as a point cloud
    voxel_centroids = []
    for voxel in voxel_grid.get_voxels():
        voxel_centroids.append(voxel.grid_index * voxel_size + voxel_grid.origin)  # Transform grid index to world coordinates

    voxel_pcd = o3d.geometry.PointCloud()
    voxel_pcd.points = o3d.utility.Vector3dVector(np.array(voxel_centroids))
    return voxel_pcd


def floor_removal(pcd, hard_offset_threshold):
    _, pcd_only_floor = z_axis_hard_offset(pcd, hard_offset_threshold)
    # Extract x, y, z data from the point cloud
    X, Z = extract_xyz_from_point_cloud(pcd_only_floor)
    # Fit linear regression to find the slope and intercept
    a, b, c = fit_linear_regression(X, Z)
    return z_axis_slope_offset(pcd, a, b, c)


def z_axis_hard_offset(pcd, floor_height_threshold):
    pcd_np = np.asarray(pcd.points)
    # Keep points above the threshold (floor height)
    non_floor_indices = np.where(pcd_np[:, 2] < floor_height_threshold)[0] 
    pcd_without_floor = pcd.select_by_index(non_floor_indices)
    floor_indices = np.where(pcd_np[:, 2] > floor_height_threshold)[0]
    pcd_only_floor = pcd.select_by_index(floor_indices)
    return pcd_without_floor, pcd_only_floor


def z_axis_slope_offset(pcd, a, b, c):
    pcd_np = np.asarray(pcd.points)
    non_floor_indices = np.where(pcd_np[:, 2] < calculate_z_from_xy(a, b, c, pcd_np[:, 0], pcd_np[:, 1]))[0]  # Keep points above the threshold (floor height)
    pcd_without_floor = pcd.select_by_index(non_floor_indices)
    floor_indices = np.where(pcd_np[:, 2] > calculate_z_from_xy(a, b, c, pcd_np[:, 0], pcd_np[:, 1]))[0]
    pcd_only_floor = pcd.select_by_index(floor_indices)
    return pcd_without_floor, pcd_only_floor


def calculate_z_from_xy(a, b, c, x, y):
    # Calculate z from the linear equation
    z = a * x + b * y + c
    return z


def extract_xyz_from_point_cloud(pcd):
    # Convert the Open3D point cloud to numpy array
    points = np.asarray(pcd.points)
    
    # Separate x, y, and z coordinates
    X = points[:, :2]  # X contains (x, y)
    Z = points[:, 2]   # Z contains z
    return X, Z


def fit_linear_regression(X, Z):
    # Fit a linear regression model z = a*x + b*y + c
    reg = LinearRegression()
    reg.fit(X, Z)
    # Get the coefficients (a, b) and intercept (c)
    a, b = reg.coef_
    c = reg.intercept_
    c *=1.5
    return a, b, c


def group_points_by_xy(pcd, voxel_size=0.01):
    points = np.asarray(pcd.points)

    # Dictionary to hold (x, y) -> list of z values
    xy_to_heights = defaultdict(list)

    for point in points:
        x, y, z = point
        # Discretize (x, y) coordinates to ensure uniqueness based on voxel_size
        xy_key = (round(x / voxel_size) * voxel_size, round(y / voxel_size) * voxel_size)
        xy_to_heights[xy_key].append(z)

    # Convert dictionary to a list of (x, y, z_list) tuples
    grouped_points = [(xy_key[0], xy_key[1], heights) for xy_key, heights in xy_to_heights.items()]
    
    return grouped_points


def dbscan_clustering_2d(pcd, eps=0.1, min_samples=10):
    clustering = DBSCAN(eps=eps, min_samples=min_samples).fit(pcd)
    labels = clustering.labels_
    return labels


def assign_cluster_colors(pcd, labels):
    random.seed(69)
    if labels.max() > 0:
        cmap = plt.get_cmap('tab20')
        # Generate a List of colors from tab20
        color_list = [cmap(random.randint(1, 20))[:3] for _ in range(labels.max()+1)]
        colors = []
        for i in labels:
            colors.append(color_list[i])
    else:
        # Handle case with no clusters (only noise)
        colors = np.array([[0, 0, 0]] * len(labels))  # Default gray color for all points if no clusters
    pcd.colors = o3d.utility.Vector3dVector(colors)
    return pcd


def find_best_fit_alphashape(points, alpha=0.5):
    x_arr = points[:, 0]
    y_arr = points[:, 1]
    if len(np.unique(x_arr)) < 2 or len(np.unique(y_arr)) < 2:
        return False, None
    try:
        alpha_shape = alphashape.alphashape(points, alpha)
    except:
        print(f"failed to extract alpha shape")
        return False, None
    # Check if the result is a Polygon, MultiPolygon, or GeometryCollection
    if isinstance(alpha_shape, Polygon):
        # If it's a single Polygon, extract the exterior
        x, y = alpha_shape.exterior.xy
        exterior_coords = np.column_stack([x, y])
    else:
        # For other cases (like GeometryCollection), handle them appropriately
        return False, None
    return True, exterior_coords


def find_best_fit_convexhull(points):
    x_arr = points[:, 0]
    y_arr = points[:, 1]
    if len(np.unique(x_arr)) < 2 or len(np.unique(y_arr)) < 2:
        return False, None
    try:
        # Compute the convex hull of the centroids
        hull = ConvexHull(points)
    except QhullError:
        print(f"Convex hull error for cluster. Using all points as boundary.")
        return False, None
    quad_points = points[hull.vertices]
    return True, quad_points


def draw_box_from_poly(quad_points, min_z, max_z):
    # Assign height (z-value) to the 2D points for the top and bottom faces
    bottom_face = np.hstack((quad_points, np.full((quad_points.shape[0], 1), min_z)))  # z=0 for bottom face
    top_face = np.hstack((quad_points, np.full((quad_points.shape[0], 1), max_z)))  # z=height for top face
    # Combine the points into a single array for visualization
    box_points = np.vstack((bottom_face, top_face)) 

    # # Create an Open3D PointCloud object for visualization
    point_cloud = o3d.geometry.PointCloud()
    point_cloud.points = o3d.utility.Vector3dVector(box_points)

    # Define the lines to connect points and form the box
    lines = []
    for i in range(len(quad_points)):
        if i == len(quad_points)-1:
            lines.append([i, 0])
        else:
            lines.append([i, i+1])

    for i in range(len(quad_points)):
        j = i+len(quad_points)
        if i == len(quad_points)-1:
            lines.append([j, len(quad_points)])
        else:
            lines.append([j, j+1])
    
    for i in range(len(quad_points)):
        lines.append([i, i+len(quad_points)])

    # Create LineSet object to draw the lines connecting the points
    line_set = o3d.geometry.LineSet()
    line_set.points = o3d.utility.Vector3dVector(box_points)
    line_set.lines = o3d.utility.Vector2iVector(lines)

    # Set colors for the lines (RGB)
    colors = [[0, 0, 0] for _ in range(len(lines))]  # Green lines for the box
    line_set.colors = o3d.utility.Vector3dVector(colors)
    p_colors = [[1, 0, 0] for _ in range(len(box_points))]
    point_cloud.colors = o3d.utility.Vector3dVector(p_colors)
    return point_cloud, line_set