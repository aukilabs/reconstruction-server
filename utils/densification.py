from hloc import extract_features, match_features, match_dense, reconstruction, pairs_from_sequential, triangulation, pairs_from_retrieval, pairs_from_poses
from hloc.triangulation import create_db_from_model, import_features, import_matches, run_triangulation
import pycolmap
from pathlib import Path
from copy import deepcopy
import shutil
from hloc.utils.database import COLMAPDatabase
import open3d as o3d
import numpy as np
import utils.pairs_from_voxelized_covisibility as pairs_from_voxelized_covisibility

def filter_reconstruction(reconstruction, image_names):
    rigs = reconstruction.rigs
    frames = reconstruction.frames
    imgs = reconstruction.images
    cams = reconstruction.cameras

    if image_names is not None:
        filtered_rigs = {}
        filtered_frames = {}
        filtered_imgs = {}
        filtered_cams = {}
        id = 1
        for i, image in imgs.items():
            if image.name in image_names:
                filtered_rigs[id] = rigs[image.frame.rig_id]
                filtered_frames[id] = frames[image.frame_id]
                filtered_imgs[id] = image
                filtered_cams[id] = cams[image.camera_id]
                id += 1
        
        rigs = filtered_rigs
        frames = filtered_frames
        imgs = filtered_imgs
        cams = filtered_cams

    filtered_rec = pycolmap.Reconstruction()
    new_id = 1
    for old_id, old_img in imgs.items():
        old_img = imgs[old_id]
        old_cam = cams[old_id]
        
        new_cam = pycolmap.Camera(
            model=old_cam.model,
            width=old_cam.width,
            height=old_cam.height,
            params=old_cam.params,
            camera_id=new_id
        )
        filtered_rec.add_camera(new_cam)

        new_rig = pycolmap.Rig()
        new_rig.rig_id = new_id

        sensor = pycolmap.sensor_t(type=pycolmap.SensorType.CAMERA, id=new_id)
        new_rig.add_ref_sensor(sensor)
        filtered_rec.add_rig(new_rig)

        new_frame = pycolmap.Frame(
            rig_id = new_id,
            rig_from_world = old_img.frame.rig_from_world,
            frame_id = new_id
        )
        new_frame.add_data_id(pycolmap.data_t(
            sensor_id=sensor,
            id=new_id
        ))
        filtered_rec.add_frame(new_frame)

        list_point_2d = [pycolmap.Point2D(pt2d.xy) for pt2d in old_img.points2D]
        new_img = pycolmap.Image(
            old_img.name,
            pycolmap.Point2DList(list_point_2d),
            new_id,
            new_id
        )
        new_img.frame_id = new_id

        filtered_rec.add_image(new_img)
        filtered_rec.register_frame(new_id)
        new_id += 1

    print(f"Filtered reconstruction from {len(reconstruction.images)} to {len(filtered_rec.images)} images")
    print("Filtered reconstruction summary: ", filtered_rec.summary())
    return filtered_rec

"""
Input colmap reconstruction and output dense reconstruction
Extracts and matches dense features, saves as colmap format.
"""
def densify_reconstruction(job_root_path, colmap_path, output_path):
    colmap_rec = pycolmap.Reconstruction()
    colmap_rec.read(str(colmap_path))

    print("Loaded colmap reconstruction")
    print(colmap_rec.summary())
    image_name_to_path = {}
    for dataset in (job_root_path / "datasets").iterdir():
        for image in (dataset / "Frames").iterdir():
            image_name_to_path[image.name] = image
    
    image_names = list(image_name_to_path.keys())
    image_names = sorted(image_names)
    print(f"Found {len(image_name_to_path)} images")

    use_every_nth_image = 2
    image_names = image_names[::use_every_nth_image]
    
    images_path = output_path / "images"
    skip_image_copying = images_path.exists() and len(list(images_path.iterdir())) == len(image_names)
    if skip_image_copying:
        for existing_image in list(images_path.iterdir()):
            if existing_image.name not in image_names:
                skip_image_copying = False
                break

    if skip_image_copying:
        print("Skipping image copying since all images already exist in combined images folder")
    else:
        if images_path.exists():
            shutil.rmtree(images_path) # Do a clean new copy of all images
        images_path.mkdir(parents=True, exist_ok=True)
        for image_name in image_names:
            image_path = image_name_to_path[image_name]
            shutil.copy(str(image_path), str(images_path / image_path.name))
        print(f"Using every {use_every_nth_image}th image, total {len(image_names)} images")
    
    """
    feature_conf = deepcopy(extract_features.confs["aliked-n16"])
    feature_conf["output"] = str(output_path / "aliked-n16-feats")
    feature_conf["model"]["max_keypoints"] = 2048
    feature_conf["model"]["detection_threshold"] = 0.3
    feature_conf["model"]["nms_radius"] = 4
    feature_conf["preprocessing"]["resize_max"] = 1024
    feats = extract_features.main(
        feature_conf, images_path,
        output_path, image_list=image_names
    )
    """

    #retrieval_conf = deepcopy(extract_features.confs["megaloc"])
    #retrieval_conf["output"] = str(output_path / "megaloc_features")
    #retrieval_feats = extract_features.main(
    #    retrieval_conf, images_path,
    #    output_path, image_list=image_names
    #)

    pairs_path = output_path / "pairs.txt"
    #pairs_from_retrieval.main(
    #    retrieval_feats,
    #    str(pairs_path),
    #    num_matched=5
    #)
    #pairs_from_poses.main(
    #    global_refinement_path / "refined_sfm_combined",
    #    str(pairs_path),
    #    num_matched=5
    #)
    pairs_from_voxelized_covisibility.main(
        colmap_path,
        str(pairs_path),
        num_matched=2,
        image_names=image_names
    )

    dense_conf = deepcopy(match_dense.confs["loftr_aachen"])
    feats, matches = match_dense.main(
        dense_conf, pairs_path, images_path, output_path,
        max_kps=4096
    )
    """
    match_conf = deepcopy(match_features.confs["aliked+lightglue"])
    match_conf["output"] = str(output_path / "aliked-n16-matches")
    match_conf["model"]["compile_network"] = True
    matches = match_features.main(
        match_conf, pairs_path, features=output_path / "aliked-n16-feats.h5",
        matches=output_path / "aliked-n16-matches"
    )
    """

    # Fewer images
    filtered_rec = filter_reconstruction(colmap_rec, image_names)
    filtered_rec_path = output_path / "filtered_global"
    filtered_rec_path.mkdir(parents=True, exist_ok=True)
    filtered_rec.write(str(filtered_rec_path))

    dense_path = output_path
    triangulation.main(
        dense_path, filtered_rec_path, images_path, pairs_path, feats, matches,
        skip_geometric_verification=True,
        estimate_two_view_geometries=False,
        verbose=True,
        mapper_options={
            "fix_existing_frames": True,
            "ba_refine_focal_length": True,
            "ba_refine_extra_params": False,
            "ba_global_max_num_iterations": 5,
            "triangulation": {
                "min_angle": 4.0
            }
        }
    )

    # Remove outlier points
    print("Removing outlier points...")
    dense_rec = pycolmap.Reconstruction()
    dense_rec.read(str(dense_path))
    old_count = len(dense_rec.points3D)
    pointcloud = colmap_to_o3d_pointcloud(dense_rec)
    pointcloud, _ = pointcloud.remove_radius_outlier(20, 0.5)
    set_colmap_points_from_pointcloud(dense_rec, pointcloud)
    dense_rec.write(str(dense_path))
    new_count = len(dense_rec.points3D)
    print(f"Removed {old_count - new_count} outlier points")

    return


    #database_path = filtered_rec_path / "colmap.db"
    #image_names_to_ids = create_db_from_model(filtered_rec, database_path)
    import_features(image_names_to_ids, database_path, dense_feats)
    import_matches(
        image_names_to_ids,
        database_path,
        pairs_path,
        dense_matches,
        skip_geometric_verification=True,
    )

    print("image_names_to_ids len: ", len(image_names_to_ids))
    print("image_names len: ", len(image_names))

    database = pycolmap.Database.open(str(database_path))
    database_cache = pycolmap.DatabaseCache.create(database, 10, True, set())
    mapper = pycolmap.IncrementalMapper(database_cache)
    dense_rec = pycolmap.Reconstruction()

    #for i, image in dense_rec.images.items():
    #    print("num points 2D: ", image.num_points2D())
    mapper.begin_reconstruction(dense_rec)

    tri_options = pycolmap.IncrementalTriangulatorOptions()
    #tri_options.re_min_ratio = 0.8
    #tri_options.re_max_angle_error = 8.0
    #tri_options.re_max_trials = 
    
    mapper.retriangulate(tri_options)

    """
    for image_id in image_names_to_ids.values():
        image = dense_rec.images[image_id]
        if image.name not in image_names:
            continue
        image.frame.rig_from_world = filtered_rec.frames[image.frame_id].rig_from_world
        dense_rec.register_frame(image.frame_id)

    for image_id in image_names_to_ids.values():
        image = dense_rec.images[image_id]
        if image.name not in image_names:
            continue
        
        #num_existing_points_2D = image.num_points2D()
        num_existing_points_3D = image.num_points3D
        try:
            mapper.triangulate_image(tri_options, image_id)
        except IndexError as e:
            print(f"Error triangulating image {image_id}: {e}")
            continue
        print(f'Image {image_id}: Previously had {image.num_points2D()} points 2D, {num_existing_points_3D} points 3D. Triangulated {image.num_points3D - num_existing_points_3D} new points 3D.')
    """
    mapper.complete_and_merge_tracks(tri_options)
    mapper.end_reconstruction(False)
    print("dense_rec len: ", len(dense_rec.images))
    dense_rec.write(str(output_path / "dense"))
    print("dense_rec written to: ", str(output_path / "dense"))
    print("dense_rec summary: ", dense_rec.summary())

def densify_point_cloud(pointcloud, debug_save_name=None, alpha_shape_radius=0.3, dense_voxel_size=0.05, outlier_radius=0.5, outlier_neighours=20):
    orig_pointcloud = pointcloud
    orig_colors = np.asarray(orig_pointcloud.colors)
    pointcloud, _ = pointcloud.remove_radius_outlier(outlier_neighours, outlier_radius)
    pointcloud = pointcloud.voxel_down_sample(0.05)
    mesh = o3d.geometry.TriangleMesh.create_from_point_cloud_alpha_shape(pointcloud, alpha_shape_radius)
    mesh.compute_triangle_normals()

    if debug_save_name is not None:
        o3d.io.write_triangle_mesh(debug_save_name + ".glb", mesh)

    vox = o3d.geometry.VoxelGrid.create_from_triangle_mesh(mesh, dense_voxel_size)
    ply_vox = o3d.geometry.PointCloud()
    search_tree = o3d.geometry.KDTreeFlann(orig_pointcloud)
    randomized_colors = 0
    for v in vox.get_voxels():
        point = vox.get_voxel_center_coordinate(v.grid_index)
        ply_vox.points.append(point)

        # Since the point->mesh->voxels removes colors,
        # we pick color from nearest point in the original point cloud.
        _, neighbor_indices, _ = search_tree.search_hybrid_vector_3d(point, 1.0, 1)
        if len(neighbor_indices) > 0:
            color = orig_colors[neighbor_indices[0]]
        else:
            randomized_colors += 1
            color = np.random.rand(3)
        ply_vox.colors.append(color)
    
    print(f"Densified point cloud, point count: {len(ply_vox.points)}, color count: {len(ply_vox.colors)} (randomized {randomized_colors} colors with no near neighbor)")
    if debug_save_name is not None:
        o3d.io.write_point_cloud(debug_save_name + ".ply", ply_vox)

    return ply_vox

def convertPoint(xyz, col):
    p2 = pycolmap.Point3D()
    p2.xyz = xyz
    p2.color = [int(col[0]*255), int(col[1]*255), int(col[2]*255)]
    return p2

def set_colmap_points_from_pointcloud(rec, pointcloud):
    # Delete old points
    for i in list(rec.point3D_ids())[::-1]:
        rec.delete_point3D(i)
    for i, (xyz, color) in enumerate(zip(pointcloud.points, pointcloud.colors)):
        rec.points3D[i] = convertPoint(xyz, color)

def colmap_to_o3d_pointcloud(rec):
    pointcloud = o3d.geometry.PointCloud()
    for id in rec.point3D_ids():
        pointcloud.points.append(rec.points3D[id].xyz)
        pointcloud.colors.append(rec.points3D[id].color / 255)
    return pointcloud


def densify_rec_points(rec, debug_save_name=None):
    original_pointcloud = colmap_to_o3d_pointcloud(rec)

    levels = [
        #(0.3, 0.09),
        (0.2, 0.07),
        #(0.1, 0.05),
    ]

    densified_pointcloud = o3d.geometry.PointCloud()

    densified_pointcloud.points.extend(original_pointcloud.points)
    densified_pointcloud.colors.extend(original_pointcloud.colors)

    for i, level in enumerate(levels):
        alpha_shape_radius, dense_voxel_size = level

        dense = densify_point_cloud(
            original_pointcloud,
            debug_save_name=debug_save_name + f"_level_{i}" if debug_save_name is not None else None,
            alpha_shape_radius=alpha_shape_radius,
            dense_voxel_size=dense_voxel_size
        )

        densified_pointcloud.points.extend(dense.points)
        densified_pointcloud.colors.extend(dense.colors)

    densified_pointcloud.voxel_down_sample(0.03)
    densified_pointcloud, _ = densified_pointcloud.remove_radius_outlier(30, 0.5)

    densified_rec = deepcopy(rec)
    set_colmap_points_from_pointcloud(densified_rec, densified_pointcloud)

    return densified_rec

def robin_hardcoded_test():
    #job_root_path = Path("/app/jobs/faa6d299-2989-461e-8c1e-aff4ceb9f645/job_909e7021-95ed-4cc2-ac47-0ca1fee49708")
    job_root_path = Path("/app/jobs/b57a2941-a323-4146-9870-90c53ec7f47a/job_bae87519-eb81-4444-842a-94c556c3b79a")
    global_refinement_path = job_root_path / "refined" / "global"
    output_path = job_root_path / "global_dense_voxpairs"
    output_path.mkdir(parents=True, exist_ok=True)

    colmap_path = global_refinement_path / "refined_sfm_combined"

    densify_reconstruction(job_root_path, global_refinement_path, output_path)
    return

    rec = pycolmap.Reconstruction()
    rec.read(str(global_refinement_path / "refined_sfm_combined"))
    print("Loaded global refinement:")
    print(rec.summary())
    densified_rec = densify_rec_points(rec, debug_save_name="RobinHomeDensified")

    densified_rec.write(str(output_path))