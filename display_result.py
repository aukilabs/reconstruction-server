import argparse
import csv
import pycolmap
import os, sys
import numpy as np
from scipy.spatial.transform import Rotation
from utils.io import Model
import open3d as o3d

####################################################
# Create Open3D Objects
####################################################

def create_pose_with_thickness(translation, quaternion, axis_length=1.0, line_radius=0.02):
    """
    Create a geometry to display a pose with thick lines (using cylinders) for X, Y, Z axes.

    Parameters:
    - translation: (3,) list or np.array, [tx, ty, tz] for position.
    - quaternion: (4,) list or np.array, [qx, qy, qz, qw] representing the rotation.
    - axis_length: float, length of the axes.
    - line_radius: float, radius (thickness) of the axes lines.

    Returns:
    - geometries: list of Open3D geometries representing the pose.
    """
    def create_axis_cylinder(start, end, radius, color):
        """Create a cylinder representing an axis."""
        # Create a cylinder mesh
        cylinder = o3d.geometry.TriangleMesh.create_cylinder(radius=radius, height=axis_length)
        cylinder.paint_uniform_color(color)

        # Align the cylinder between start and end
        direction = np.array(end) - np.array(start)
        axis = direction / np.linalg.norm(direction)
        rot_matrix = calculate_rotation_matrix([0, 0, 1], axis)
        cylinder.rotate(rot_matrix, center=[0, 0, 0])

        # Translate to the midpoint between start and end
        midpoint = (np.array(start) + np.array(end)) / 2
        cylinder.translate(midpoint)
        return cylinder

    def calculate_rotation_matrix(source_axis, target_axis):
        """Calculate a rotation matrix to align source_axis with target_axis."""
        v = np.cross(source_axis, target_axis)
        c = np.dot(source_axis, target_axis)
        if np.linalg.norm(v) < 1e-6:  # Already aligned
            return np.eye(3)
        vx = np.array([
            [0, -v[2], v[1]],
            [v[2], 0, -v[0]],
            [-v[1], v[0], 0]
        ])
        rot_matrix = np.eye(3) + vx + np.dot(vx, vx) * (1 / (1 + c))
        return rot_matrix

    # Define axis directions (local coordinates)
    local_axes = np.array([
        [0, 0, 0],                 # Origin
        [axis_length, 0, 0],       # X-axis
        [0, axis_length, 0],       # Y-axis
        [0, 0, axis_length]        # Z-axis
    ])

    # Apply rotation using quaternion
    rotation = Rotation.from_quat(quaternion)  # quaternion = [qx, qy, qz, qw]
    rotated_axes = rotation.apply(local_axes)

    # Apply translation
    translated_axes = rotated_axes + np.array(translation)

    # Define axis colors: Red for X, Green for Y, Blue for Z
    colors = [
        [1, 0, 0],  # Red for X-axis
        [0, 1, 0],  # Green for Y-axis
        [0, 0, 1]   # Blue for Z-axis
    ]

    # Create thick axes using cylinders
    geometries = []
    for i, color in zip(range(1, 4), colors):  # Skip the origin at index 0
        start = translated_axes[0]  # Origin
        end = translated_axes[i]    # Axis endpoint
        cylinder = create_axis_cylinder(start, end, line_radius, color)
        geometries.append(cylinder)

    return geometries


def create_pose_lineset(translation, quaternion, axis_length=0.1):
    """
    Create a LineSet geometry to display a pose with orientation.

    Parameters:
    - translation: (3,) list or np.array, [tx, ty, tz] for position.
    - quaternion: (4,) list or np.array, [qx, qy, qz, qw] representing the rotation.
    - axis_length: float, length of the axes.

    Returns:
    - line_set: Open3D.geometry.LineSet representing the pose.
    """
    # Define local axes in the coordinate frame (before rotation)
    local_axes = np.array([
        [0, 0, 0],  # Origin
        [axis_length, 0, 0],  # X-axis
        [0, axis_length, 0],  # Y-axis
        [0, 0, axis_length]   # Z-axis
    ])

    # Apply rotation using the quaternion
    rotation = Rotation.from_quat(quaternion)  # quaternion = [qx, qy, qz, qw]
    rotated_axes = rotation.apply(local_axes)

    # Apply translation
    translated_axes = rotated_axes + np.array(translation)

    # Define lines connecting origin to each axis end
    lines = [
        [0, 1],  # Origin to X-axis
        [0, 2],  # Origin to Y-axis
        [0, 3]   # Origin to Z-axis
    ]

    # Assign colors: Red for X, Green for Y, Blue for Z
    colors = [
        [1, 0, 0],  # Red for X-axis
        [0, 1, 0],  # Green for Y-axis
        [0, 0, 1]   # Blue for Z-axis
    ]

    # Create LineSet geometry
    line_set = o3d.geometry.LineSet()
    line_set.points = o3d.utility.Vector3dVector(translated_axes)  # Set points
    line_set.lines = o3d.utility.Vector2iVector(lines)            # Set lines
    line_set.colors = o3d.utility.Vector3dVector(colors)          # Set colors

    return line_set


def create_square_plane(translation, quaternion, size=1.0, thickness=0.01, color=[0.5, 0.5, 0.5]):
    """
    Create a square plane mesh on the XY plane with adjustable size, thickness, and pose.

    Parameters:
    - translation: (3,) list or np.array, [tx, ty, tz] for position.
    - quaternion: (4,) list or np.array, [qx, qy, qz, qw] representing the rotation.
    - size: float, the side length of the square.
    - thickness: float, thickness of the plane.
    - color: (3,) list, RGB color for the plane.

    Returns:
    - plane_mesh: Open3D.geometry.TriangleMesh representing the square plane.
    """
    # Create a box mesh to simulate a square plane with thickness
    plane_mesh = o3d.geometry.TriangleMesh.create_box(width=size, height=size, depth=thickness)
    plane_mesh.paint_uniform_color(color)

    # Shift the box so its center lies on the XY-plane
    plane_mesh.translate([-size / 2, -size / 2, -thickness / 2])

    # Apply rotation using quaternion
    rotation = Rotation.from_quat(quaternion)  # quaternion = [qx, qy, qz, qw]
    rotation_matrix = rotation.as_matrix()
    plane_mesh.rotate(rotation_matrix, center=[0, 0, 0])

    # Apply translation
    plane_mesh.translate(translation)

    return plane_mesh


def create_grid(size=1.0, divisions=10, plane="xy"):
    """
    Creates a grid on a specified plane (xy, yz, or xz).
    
    Args:
        size (float): Length of the grid.
        divisions (int): Number of divisions in the grid.
        plane (str): The plane where the grid is created ("xy", "yz", or "xz").
    
    Returns:
        open3d.geometry.LineSet: The grid as a LineSet object.
    """
    # Create grid points
    linspace = np.linspace(-size / 2, size / 2, divisions + 1)
    points = []
    lines = []

    if plane == "xy":
        # Grid on XY plane
        for i, x in enumerate(linspace):
            points.append([x, -size / 2, 0])
            points.append([x, size / 2, 0])
            lines.append([2 * i, 2 * i + 1])
        offset = len(points)
        for i, y in enumerate(linspace):
            points.append([-size / 2, y, 0])
            points.append([size / 2, y, 0])
            lines.append([offset + 2 * i, offset + 2 * i + 1])
    elif plane == "yz":
        # Grid on YZ plane
        for i, y in enumerate(linspace):
            points.append([0, y, -size / 2])
            points.append([0, y, size / 2])
            lines.append([2 * i, 2 * i + 1])
        offset = len(points)
        for i, z in enumerate(linspace):
            points.append([0, -size / 2, z])
            points.append([0, size / 2, z])
            lines.append([offset + 2 * i, offset + 2 * i + 1])
    elif plane == "xz":
        # Grid on XZ plane
        for i, x in enumerate(linspace):
            points.append([x, 0, -size / 2])
            points.append([x, 0, size / 2])
            lines.append([2 * i, 2 * i + 1])
        offset = len(points)
        for i, z in enumerate(linspace):
            points.append([-size / 2, 0, z])
            points.append([size / 2, 0, z])
            lines.append([offset + 2 * i, offset + 2 * i + 1])
    else:
        raise ValueError(f"Unknown plane '{plane}'. Choose from 'xy', 'yz', or 'xz'.")

    # Convert to LineSet
    points = np.array(points)
    lines = np.array(lines)
    line_set = o3d.geometry.LineSet()
    line_set.points = o3d.utility.Vector3dVector(points)
    line_set.lines = o3d.utility.Vector2iVector(lines)

    # Set all line colors to white
    colors = [[1.0, 1.0, 1.0] for _ in range(len(lines))]  # White color for all lines
    line_set.colors = o3d.utility.Vector3dVector(colors)
    
    return line_set
####################################################
# IO
####################################################
def read_portal(file):

    print(f'Loading QR detections from, {file}, ...')
    # Initialize the dictionary
    portal_detections = {}

    # Read and process the CSV file
    with open(file, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            timestamp = round(float(row[0]) * 1e9) # s to ns
            pose_values = [float(val) for val in row[2:9]] # px, py, pz, rx, ry, rz, rw
            pos = pose_values[:3]
            quat = pose_values[3:]

            qr_pose = pycolmap.Rigid3d(
                pycolmap.Rotation3d(np.array(quat)),
                np.array(pos)
            )

            portal_detections[timestamp] = {
                "pose": qr_pose,
                "short_id": row[1]
            }

    return portal_detections


####################################################
# Main Script
####################################################
def main(args):

    # Read Model
    model = Model()
    model.read_model(path=args.sfm_folder, ext='.bin')
    pcd = model.get_points(in_opengl=True)

    # Read Portals
    portal_detections = read_portal(args.portal_file)

    geo = []
    # Construct Origin Axis Visual
    geo.extend(create_pose_with_thickness([0, 0, 0], [0, 0, 0, 1], 0.5, 0.05))

    # Create Grid
    geo.append(create_grid(20.0, 20, plane="xz"))

    # Construct Portals Visual Object
    for _, portal in portal_detections.items():
        pose = portal["pose"]
        geo.append(create_pose_lineset(pose.translation, pose.rotation.quat))
        geo.append(create_square_plane(pose.translation, pose.rotation.quat, 0.2))

    # Create Open3d Visualizer
    vis1 = o3d.visualization.Visualizer()
    vis1.create_window(window_name='Domain Visual', width=960, height=540, left=0, top=0)
    vis1.add_geometry(pcd)
    for o in geo:
        vis1.add_geometry(o)
    vis1.poll_events()
    vis1.update_renderer()

    # Set Render Options
    render_option = vis1.get_render_option()
    render_option.point_size = 1.0  # Smaller value means smaller points (default is usually 5.0)
    render_option.background_color = [0, 0, 0] 

    while True:
        if not vis1.poll_events():
            break
        vis1.update_renderer()
    

def parse_arguments():
    parser = argparse.ArgumentParser(description="Display SFM and Portal results")
    parser.add_argument('--sfm-folder', type=str, help='Path to sfm result folder with .bin extensions', default='./refined/local/2024-12-16_12-57-48/sfm')
    parser.add_argument('--portal-file', type=str, help='Path to csv file with portal information', default='./datasets/2024-12-16_12-57-48/PortalDetections.csv')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    main(args)
