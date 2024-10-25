import open3d as o3d
import numpy as np
import argparse
import datetime
import time
import os
import yaml


from utils.io import Model, load_yaml, save_to_yaml
from utils.geometry_utils import (
    voxelise, 
    floor_removal, 
    group_points_by_xy, 
    dbscan_clustering_2d,
    assign_cluster_colors,
    find_best_fit_alphashape,
    find_best_fit_convexhull,
    draw_box_from_poly
)


SUPPORTED_OCCLUSION_METHOD = ["aabb", "alphashape", "convexhull"]

def parse_arguments():
    parser = argparse.ArgumentParser(description="Occlusion boxes extraction script")
    parser.add_argument('--config', type=str, help='Path to YAML config file', default='./config/occlusion_box/default.yaml')
    return parser.parse_args()


def setview_and_capture(vis, jfile, output_file):
    vis.set_view_status(jfile)
    # Update the visualization
    vis.poll_events()
    vis.update_renderer()
    vis.capture_screen_image(output_file)
    return


def main(config):
    # Date and time 
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    time0 = time.time()
    result = {}
    time_spent = {}
    num_points = {}
    
    # Model init
    model = Model()
    model.read_model(path=config['path'], ext='.bin')

    print("num_cameras:", len(model.cameras))
    print("num_images:", len(model.images))
    print("num_points3D:", len(model.points3D))

    # Load point cloud
    pcd = model.get_points()
    num_points['Original'] = len(pcd.points)

    # Step 0: fix coordinate
    if config['opengl']: # TODO: is this default?
        print("[Step 0] Fix Coordinate")
        r_l = [-np.pi / 2, 0, 0]
        rotation_fix = pcd.get_rotation_matrix_from_axis_angle(r_l)  # 90 degrees = π/2 radians
        pcd.rotate(rotation_fix, center=(0, 0, 0))  # Center of rotation at origin (0, 0, 0)

    # Step 1: Voxelizing
    pcd = pcd.voxel_down_sample(voxel_size = config['voxel_size'])
    time1 = time.time()
    time_spent["Voxelize"] = time1 - time0
    print("step1 time spent: ", time_spent["Voxelize"])
    num_points['Voxelized'] = len(pcd.points)

    # Step 2: Points Cleaning
    print("[Step 2] Point Cloud Cleaning")
    pcd, _ = pcd.remove_radius_outlier(nb_points=config['outlier_min_points'], radius=config['outlier_radius'])
    time2 = time.time()
    time_spent["Point Cloud Cleaning"] = time2 - time1
    print("step2 time spent: ", time_spent["Point Cloud Cleaning"])
    num_points['Outlier Cleaning'] = len(pcd.points)

    # Step 3: Floor Removal
    print("[Step 3] Floor Removal")
    pcd_without_floor, _ = floor_removal(pcd, config['height_threshold'])
    time3 = time.time()
    time_spent["Floor Removal"] = time3 - time2
    print("step3 time spent: ", time_spent["Floor Removal"])
    num_points['Removed Floor'] = len(pcd.points)

    # Step 4: DBSCAN for voxel clustering
    print("[Step 4] Clustering")
    if config['xy_plane_clustering']:
        grouped_points_without_floor = group_points_by_xy(pcd_without_floor, config['voxel_size'])
        xy_points = np.asarray([[i[0], i[1]] for i in grouped_points_without_floor])
        labels = dbscan_clustering_2d(xy_points, config['cluster_eps'], config['cluster_min_points'])
        # Recreate pcd
        actual_labels = []
        xyz_points = []
        for i, xy_group in enumerate(grouped_points_without_floor):
            x, y, z_list = xy_group
            for z in z_list: 
                xyz_points.append([x, y, z])
                actual_labels.append(labels[i])
        pcd_clusters =  o3d.geometry.PointCloud()
        pcd_clusters.points = o3d.utility.Vector3dVector(np.array(xyz_points))

        labels = np.array(labels)
        actual_labels = np.array(actual_labels)
        label_indices = []
        labels_without_zero = []
        for index, element in enumerate(actual_labels):
            if element > 0:
                label_indices.append(index)
                labels_without_zero.append(element)
    else:
        actual_labels = np.array(pcd_without_floor.cluster_dbscan(eps=config['cluster_eps'], min_points=config['cluster_min_points'], print_progress=True))
        pcd_clusters = pcd_without_floor

    print(f"Extracted number of clusters: {actual_labels.max() + 1}")
    pcd_clusters = assign_cluster_colors(pcd_clusters, actual_labels)
    time4 = time.time()
    time_spent['Clustering'] = time4 - time3
    print("step4 time spent: ", time_spent['Clustering'])

    # Step 5: Create bounding boxes for each cluster and set them to black
    print("[Step 5] Extract Occlusion Volume")
    geo = []

    if config['occlusion_method'] not in SUPPORTED_OCCLUSION_METHOD:
        print(f"{config['occlusion_method']} not supported, switching to default aabb")
        config['occlusion_method'] = "aabb"

    for i in range(actual_labels.max() + 1):
        if i < 1:
            continue
        cluster_indices = np.where(actual_labels == i)[0]
        if len(cluster_indices) >= 4:
            cluster = pcd_clusters.select_by_index(cluster_indices)
            cluster_np_points = np.array(cluster.points)

            if config['occlusion_method'] == 'aabb':
                # AABB
                bbox = cluster.get_axis_aligned_bounding_box()
                bbox.color = [0, 0, 0]  # Set bounding box color to black
                geo.append(bbox)

            elif config['occlusion_method'] == 'alphashape':
                # Alphashape
                success, qpoints = find_best_fit_alphashape(cluster_np_points[:, :2])
                if success:
                    occ_pcd, occ_box = draw_box_from_poly(qpoints, cluster_np_points[:, 2].min(), cluster_np_points[:, 2].max())
                    geo.append(occ_box)
                    geo.append(occ_pcd)
            elif config['occlusion_method'] == 'convexhull':
                 # Convexhull
                success, qpoints = find_best_fit_convexhull(cluster_np_points[:, :2])
                if success:
                    occ_pcd, occ_box = draw_box_from_poly(qpoints, cluster_np_points[:, 2].min(), cluster_np_points[:, 2].max())
                    geo.append(occ_box)
                    geo.append(occ_pcd)

    time5 = time.time()
    time_spent["Occlusion Volume"] = time5 - time4
    print("step5 time spent: ", time_spent['Occlusion Volume'])

    # Step 6: Visualize the clusters with black bounding boxes and axes
    vis1 = o3d.visualization.Visualizer()
    vis1.create_window(window_name='Occlusion', width=960, height=540, left=0, top=0)
    vis1.add_geometry(pcd_clusters)
    for o in geo:
        vis1.add_geometry(o)
    vis1.poll_events()
    vis1.update_renderer()

    if not os.path.exists(config['viewpoint_dir']):
        print(f"View Point Directory not exist: {config['viewpoint_dir']}")
    else:
        json_files = [file for file in os.listdir(config['viewpoint_dir']) if file.endswith('.json')]
        # Get full paths to the JSON files
        json_file_paths = [os.path.join(config['viewpoint_dir'], file) for file in json_files]

        if not os.path.exists(config['output_dir']):
            os.mkdir(config['output_dir'])

        for i, jfile in enumerate(json_file_paths):
            with open(jfile, 'r') as f:
                viewport_json = f.read()
            setview_and_capture(vis1, viewport_json, 
                                os.path.join(config['output_dir'],
                                             f"{config['occlusion_method']}_view{i}.jpg"
                                )
            )

    # Save Results
    result['Execution Time'] = formatted_datetime
    result['Time Spent'] = time_spent
    result['Number of Points'] = num_points
    with open(os.path.join(config['output_dir'], 'result.yaml'), 'w') as yaml_file:
        yaml.dump(result, yaml_file, default_flow_style=False)

    # If Display
    if config['display']:
        while True:
            if not vis1.poll_events():
                break
            vis1.update_renderer()

    vis1.destroy_window()


if __name__ == "__main__":

    args = parse_arguments()
    config = load_yaml(args.config)
    save_to_yaml(config)

    main(config)
