import numpy as np

import open3d as o3d
from sklearn.linear_model import LinearRegression
from sklearn.cluster import DBSCAN
from collections import defaultdict
import random
import matplotlib.pyplot as plt
import alphashape
import trimesh
import uuid
from pathlib import Path

from shapely.geometry import Polygon, GeometryCollection
from scipy.spatial import ConvexHull
from scipy.spatial._qhull import QhullError
from scipy.spatial import Delaunay

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


def draw_box_from_poly(quad_points, min_z, max_z, alpha=0.5):
    # Assign height (z-value) to the 2D points for the top and bottom faces
    bottom_face = np.hstack((quad_points, np.full((quad_points.shape[0], 1), min_z)))  # z=min_z for bottom face
    top_face = np.hstack((quad_points, np.full((quad_points.shape[0], 1), max_z)))  # z=max_z for top face
    # Combine the points into a single array for visualization
    box_points = np.vstack((bottom_face, top_face)) 

    # Create an Open3D PointCloud object for visualization
    point_cloud = o3d.geometry.PointCloud()
    point_cloud.points = o3d.utility.Vector3dVector(box_points)
    p_colors = [[1, 0, 0] for _ in range(len(box_points))]
    point_cloud.colors = o3d.utility.Vector3dVector(p_colors)

    # Define the lines to connect points and form the box
    lines = []
    for i in range(len(quad_points)):
        # Bottom face edges
        lines.append([i, (i + 1) % len(quad_points)])
        # Top face edges
        lines.append([i + len(quad_points), (i + 1) % len(quad_points) + len(quad_points)])
        # Side edges connecting top and bottom faces
        lines.append([i, i + len(quad_points)])

    # Create LineSet object to draw the lines connecting the points
    line_set = o3d.geometry.LineSet()
    line_set.points = o3d.utility.Vector3dVector(box_points)
    line_set.lines = o3d.utility.Vector2iVector(lines)
    line_set.colors = o3d.utility.Vector3dVector([[0, 0, 0] for _ in lines])  # Black lines for edges

    # Mesh creation
    # Define the triangles for the top and bottom faces and sides
    triangles = []
    num_points = len(quad_points)

    # Perform 2D triangulation on the quad points
    tri = Delaunay(quad_points)
    triangles = []
    
    # poly = geometry.Polygon([[p] for p in quad_points])
    surfaces = []
    alpha_shape = alphashape.alphashape(quad_points, alpha)
    if isinstance(alpha_shape, Polygon):
        surfaces.append(alpha_shape)
    elif isinstance(alpha_shape, GeometryCollection):
        for geom in alpha_shape.geoms:
            surfaces.append(geom)

    # Bottom face triangles
    top_offset = len(quad_points)
    for simplex in tri.simplices:
        triangle = Polygon(quad_points[simplex])
        if any(surface.contains(triangle.centroid) for surface in surfaces):
            triangles.append([simplex[0], simplex[1], simplex[2]])
            triangles.append([simplex[0] + top_offset, simplex[1] + top_offset, simplex[2] + top_offset])

    # Side Triangles
    for i in range(num_points):
        triangles.append([i, (i + 1) % num_points, (i + 1) % num_points + num_points])
        triangles.append([i, (i + 1) % num_points + num_points, i + num_points])

    # Create the mesh
    mesh = o3d.geometry.TriangleMesh()
    mesh.vertices = o3d.utility.Vector3dVector(box_points)
    mesh.triangles = o3d.utility.Vector3iVector(triangles)
    mesh.compute_vertex_normals()  # Optionally compute normals for shading

    return point_cloud, line_set, mesh