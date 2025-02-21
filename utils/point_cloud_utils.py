import open3d as o3d

def filter_ply(ply_path, filtered_ply_path, ply_remove_outliers=True, ply_downsample=True, logger=None):

    point_cloud = o3d.io.read_point_cloud(ply_path)
    filtered_point_cloud = filter_point_cloud(point_cloud, ply_remove_outliers, ply_downsample, logger)
    o3d.io.write_point_cloud(filtered_ply_path, filtered_point_cloud, write_ascii=True)

    if logger is not None:
        logger.info(
            f"Point cloud size reduced from {int(ply_path.stat().st_size / 1024)} KB "
            f"to {int(filtered_ply_path.stat().st_size / 1024)} KB"
        )
        logger.info(f"Filtered point cloud saved: {filtered_ply_path}")


def filter_point_cloud(point_cloud, ply_remove_outliers=True, ply_downsample=True, logger=None):
        
        input_point_count = len(point_cloud.points)

        if ply_remove_outliers:
            point_cloud, _ = point_cloud.remove_statistical_outlier(nb_neighbors=20, std_ratio=2.0)
            point_cloud, _ = point_cloud.remove_radius_outlier(nb_points=10, radius=0.20)

        if ply_downsample:
            point_cloud = point_cloud.voxel_down_sample(voxel_size=0.04)
            point_cloud = point_cloud.random_down_sample(sampling_ratio=0.7)

        if logger is not None:
            logger.info(f"Point cloud filtered from {input_point_count} to {len(point_cloud.points)} points.")

        return point_cloud
