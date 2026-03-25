import open3d as o3d
import os
import subprocess
import shutil
import numpy as np
import logging

# Reduce decimals making the ply file smaller.
# Also changes the vertex type to float instead of double, which is required by draco encoding.
def reduce_decimals_ply(ply_path, reduced_ply_path, decimals=2, logger=None):
    if logger is None:
        logger = logging.getLogger()

    parsing_header = True
    with open(reduced_ply_path, "w") as reduced_ply:
        with open(ply_path, "r") as original_ply:
            for line in original_ply:
                if parsing_header:
                    if line.strip() == "end_header":
                        parsing_header = False
                    elif line.strip() in ["property double x", "property double y", "property double z"]:
                        line = line.replace("double", "float")
                    reduced_ply.write(line)
                    continue
                
                # Parsing points
                values = line.split()
                if len(values) >= 3:
                    try:
                        x,y,z = [float(v) for v in values[:3]]
                    except ValueError:
                        logger.debug(f"Not a point: {line}. Skip to next line!")
                        continue

                    x = round(x, decimals)
                    y = round(y, decimals)
                    z = round(z, decimals)
                    
                    reduced_ply.write(f"{x} {y} {z} {' '.join(values[3:])}\n")

    if logger is not None:
        logger.info(f"Point cloud saved rounded to {decimals} decimals: {reduced_ply_path}")


def downsample_ply_to_max_size(ply_path, downsampled_ply_path, max_bytes, logger=None):

    if os.path.getsize(ply_path) < max_bytes:
        if logger is not None:
            logger.info(f"Point cloud is already smaller than {max_bytes} bytes. Copying to {downsampled_ply_path}")
        shutil.copy(ply_path, downsampled_ply_path)
        return

    # Make it sparser iteratively until it's small enough
    point_cloud = o3d.io.read_point_cloud(ply_path)
    size = os.path.getsize(ply_path)
    while size > max_bytes:
        point_cloud = point_cloud.random_down_sample(sampling_ratio=0.7)
        o3d.io.write_point_cloud(downsampled_ply_path, point_cloud, write_ascii=True)
        size = os.path.getsize(downsampled_ply_path)

    if logger is not None:
        logger.info(f"Point cloud downsampled to {os.path.getsize(downsampled_ply_path)} bytes: {downsampled_ply_path}")

def filter_ply(ply_path, filtered_ply_path, ply_remove_outliers=True, ply_downsample=True, convert_opencv_to_opengl=False, logger=None):

    point_cloud = o3d.io.read_point_cloud(ply_path)

    if convert_opencv_to_opengl:
        logger.info("Converting OpenCV to OpenGL coordinate system...")
        trans = np.zeros((4,4))
        trans[0,1] = 1
        trans[1,0] = 1
        trans[2,2] = -1
        trans[3,3] = 1
        point_cloud = point_cloud.transform(trans)
        logger.info("Done converting to OpenGL")

    old_size = ply_path.stat().st_size
    filtered_point_cloud = filter_point_cloud(point_cloud, ply_remove_outliers, ply_downsample, logger)
    o3d.io.write_point_cloud(filtered_ply_path, filtered_point_cloud, write_ascii=True)

    if logger is not None:
        logger.info(
            f"Point cloud size reduced from {int(old_size / 1024)} KB "
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


def draco_compress_ply(ply_path, draco_path, logger=None):
    # Command line tool (already compiled)
    # Quantizing with 13 bits, empirically selected for no visual difference.
    subprocess.run(["/src/draco/build/draco_encoder", "-i", ply_path, "-qp", "13", "-o", draco_path])

    if logger is not None:
        logger.info(f"Draco compressed point cloud saved: {draco_path}")
