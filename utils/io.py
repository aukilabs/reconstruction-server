from typing import Dict, Optional, Tuple
import numpy as np
import logging
import os
import collections
import struct
import open3d 
import yaml
import trimesh
import uuid
import csv
from scipy.spatial.transform import Rotation as R
from utils.data_utils import (convert_pose_colmap_to_opengl)

CameraModel = collections.namedtuple(
    "CameraModel", ["model_id", "model_name", "num_params"]
)
Camera = collections.namedtuple(
    "Camera", ["id", "model", "width", "height", "params"]
)
BaseImage = collections.namedtuple(
    "Image", ["id", "qvec", "tvec", "camera_id", "name", "xys", "point3D_ids"]
)
Point3D = collections.namedtuple(
    "Point3D", ["id", "xyz", "rgb", "error", "image_ids", "point2D_idxs"]
)
Portal = collections.namedtuple(
    "Portal", ["id", "short_id", "qvec", "tvec", "image_id", "corners", "size"]
)

CAMERA_MODELS = {
    CameraModel(model_id=0, model_name="SIMPLE_PINHOLE", num_params=3),
    CameraModel(model_id=1, model_name="PINHOLE", num_params=4),
    CameraModel(model_id=2, model_name="SIMPLE_RADIAL", num_params=4),
    CameraModel(model_id=3, model_name="RADIAL", num_params=5),
    CameraModel(model_id=4, model_name="OPENCV", num_params=8),
    CameraModel(model_id=5, model_name="OPENCV_FISHEYE", num_params=8),
    CameraModel(model_id=6, model_name="FULL_OPENCV", num_params=12),
    CameraModel(model_id=7, model_name="FOV", num_params=5),
    CameraModel(model_id=8, model_name="SIMPLE_RADIAL_FISHEYE", num_params=4),
    CameraModel(model_id=9, model_name="RADIAL_FISHEYE", num_params=5),
    CameraModel(model_id=10, model_name="THIN_PRISM_FISHEYE", num_params=12),
}
CAMERA_MODEL_IDS = dict(
    [(camera_model.model_id, camera_model) for camera_model in CAMERA_MODELS]
)
CAMERA_MODEL_NAMES = dict(
    [(camera_model.model_name, camera_model) for camera_model in CAMERA_MODELS]
)


class Image(BaseImage):
    def qvec2rotmat(self):
        return qvec2rotmat(self.qvec)
    

def qvec2rotmat(qvec):
    return np.array(
        [
            [
                1 - 2 * qvec[2] ** 2 - 2 * qvec[3] ** 2,
                2 * qvec[1] * qvec[2] - 2 * qvec[0] * qvec[3],
                2 * qvec[3] * qvec[1] + 2 * qvec[0] * qvec[2],
            ],
            [
                2 * qvec[1] * qvec[2] + 2 * qvec[0] * qvec[3],
                1 - 2 * qvec[1] ** 2 - 2 * qvec[3] ** 2,
                2 * qvec[2] * qvec[3] - 2 * qvec[0] * qvec[1],
            ],
            [
                2 * qvec[3] * qvec[1] - 2 * qvec[0] * qvec[2],
                2 * qvec[2] * qvec[3] + 2 * qvec[0] * qvec[1],
                1 - 2 * qvec[1] ** 2 - 2 * qvec[2] ** 2,
            ],
        ]
    )
    

def read_next_bytes(fid, num_bytes, format_char_sequence, endian_character="<"):
    """Read and unpack the next bytes from a binary file.
    :param fid:
    :param num_bytes: Sum of combination of {2, 4, 8}, e.g. 2, 6, 16, 30, etc.
    :param format_char_sequence: List of {c, e, f, d, h, H, i, I, l, L, q, Q}.
    :param endian_character: Any of {@, =, <, >, !}
    :return: Tuple of read and unpacked values.
    """
    data = fid.read(num_bytes)
    return struct.unpack(endian_character + format_char_sequence, data)


def write_next_bytes(fid, data, format_char_sequence, endian_character="<"):
    """pack and write to a binary file.
    :param fid:
    :param data: data to send, if multiple elements are sent at the same time,
    they should be encapsuled either in a list or a tuple
    :param format_char_sequence: List of {c, e, f, d, h, H, i, I, l, L, q, Q}.
    should be the same length as the data list or tuple
    :param endian_character: Any of {@, =, <, >, !}
    """
    if isinstance(data, (list, tuple)):
        bytes = struct.pack(endian_character + format_char_sequence, *data)
    else:
        bytes = struct.pack(endian_character + format_char_sequence, data)
    fid.write(bytes)


def detect_model_format(path, ext, logger=None):
    if logger is None:
        logger = logging.getLogger()

    if (
        os.path.isfile(os.path.join(path, "cameras" + ext))
        and os.path.isfile(os.path.join(path, "images" + ext))
        and os.path.isfile(os.path.join(path, "points3D" + ext))
    ):
        logger.info("Detected model format: '%s'", ext)
        return True

    return False


def read_cameras_text(path):
    """
    see: src/colmap/scene/reconstruction.cc
        void Reconstruction::WriteCamerasText(const std::string& path)
        void Reconstruction::ReadCamerasText(const std::string& path)
    """
    cameras = {}
    with open(path, "r") as fid:
        while True:
            line = fid.readline()
            if not line:
                break
            line = line.strip()
            if len(line) > 0 and line[0] != "#":
                elems = line.split()
                camera_id = int(elems[0])
                model = elems[1]
                width = int(elems[2])
                height = int(elems[3])
                params = np.array(tuple(map(float, elems[4:])))
                cameras[camera_id] = Camera(
                    id=camera_id,
                    model=model,
                    width=width,
                    height=height,
                    params=params,
                )
    return cameras


def write_cameras_text(cameras, path):
    """
    see: src/base/reconstruction.cc
        void Reconstruction::WriteCamerasText(const std::string& path)
        void Reconstruction::ReadCamerasText(const std::string& path)
    """
    HEADER = (
        "# Camera list with one line of data per camera:\n"
        + "#   CAMERA_ID, MODEL, WIDTH, HEIGHT, PARAMS[]\n"
        + "# Number of cameras: {}\n".format(len(cameras))
    )
    with open(path, "w") as fid:
        fid.write(HEADER)
        for _, cam in cameras.items():
            to_write = [cam.id, cam.model, cam.width, cam.height, *cam.params]
            line = " ".join([str(elem) for elem in to_write])
            fid.write(line + "\n")


def read_images_text(path):
    """
    see: src/colmap/scene/reconstruction.cc
        void Reconstruction::ReadImagesText(const std::string& path)
        void Reconstruction::WriteImagesText(const std::string& path)
    """
    images = {}
    with open(path, "r") as fid:
        while True:
            line = fid.readline()
            if not line:
                break
            line = line.strip()
            if len(line) > 0 and line[0] != "#":
                elems = line.split()
                image_id = int(elems[0])
                qvec = np.array(tuple(map(float, elems[1:5])))
                tvec = np.array(tuple(map(float, elems[5:8])))
                camera_id = int(elems[8])
                image_name = elems[9]
                elems = fid.readline().split()
                xys = np.column_stack(
                    [
                        tuple(map(float, elems[0::3])),
                        tuple(map(float, elems[1::3])),
                    ]
                )
                point3D_ids = np.array(tuple(map(int, elems[2::3])))
                images[image_id] = Image(
                    id=image_id,
                    qvec=qvec,
                    tvec=tvec,
                    camera_id=camera_id,
                    name=image_name,
                    xys=xys,
                    point3D_ids=point3D_ids,
                )
    return images


def write_images_text(images, path):
    """
    see: src/base/reconstruction.cc
        void Reconstruction::ReadImagesText(const std::string& path)
        void Reconstruction::WriteImagesText(const std::string& path)
    """
    if len(images) == 0:
        mean_observations = 0
    else:
        mean_observations = sum(
            (len(img.point3D_ids) for _, img in images.items())
        ) / len(images)
    HEADER = (
        "# Image list with two lines of data per image:\n"
        + "#   IMAGE_ID, QW, QX, QY, QZ, TX, TY, TZ, CAMERA_ID, NAME\n"
        + "#   POINTS2D[] as (X, Y, POINT3D_ID)\n"
        + "# Number of images: {}, mean observations per image: {}\n".format(
            len(images), mean_observations
        )
    )

    with open(path, "w") as fid:
        fid.write(HEADER)
        for _, img in images.items():
            image_header = [img.id, *img.qvec, *img.tvec, img.camera_id, img.name]
            first_line = " ".join(map(str, image_header))
            fid.write(first_line + "\n")

            points_strings = []
            for xy, point3D_id in zip(img.xys, img.point3D_ids):
                points_strings.append(" ".join(map(str, [*xy, point3D_id])))
            fid.write(" ".join(points_strings) + "\n")


def read_points3D_text(path):
    """
    see: src/colmap/scene/reconstruction.cc
        void Reconstruction::ReadPoints3DText(const std::string& path)
        void Reconstruction::WritePoints3DText(const std::string& path)
    """
    points3D = {}
    with open(path, "r") as fid:
        while True:
            line = fid.readline()
            if not line:
                break
            line = line.strip()
            if len(line) > 0 and line[0] != "#":
                elems = line.split()
                point3D_id = int(elems[0])
                xyz = np.array(tuple(map(float, elems[1:4])))
                rgb = np.array(tuple(map(int, elems[4:7])))
                error = float(elems[7])
                image_ids = np.array(tuple(map(int, elems[8::2])))
                point2D_idxs = np.array(tuple(map(int, elems[9::2])))
                points3D[point3D_id] = Point3D(
                    id=point3D_id,
                    xyz=xyz,
                    rgb=rgb,
                    error=error,
                    image_ids=image_ids,
                    point2D_idxs=point2D_idxs,
                )
    return points3D


def write_points3D_text(points3D, path):
    """
    see: src/base/reconstruction.cc
        void Reconstruction::ReadPoints3DText(const std::string& path)
        void Reconstruction::WritePoints3DText(const std::string& path)
    """
    if len(points3D) == 0:
        mean_track_length = 0
    else:
        mean_track_length = sum(
            (len(pt.image_ids) for _, pt in points3D.items())
        ) / len(points3D)
    HEADER = (
        "# 3D point list with one line of data per point:\n"
        + "#   POINT3D_ID, X, Y, Z, R, G, B, ERROR, TRACK[] as (IMAGE_ID, POINT2D_IDX)\n"  # noqa: E501
        + "# Number of points: {}, mean track length: {}\n".format(
            len(points3D), mean_track_length
        )
    )

    with open(path, "w") as fid:
        fid.write(HEADER)
        for _, pt in points3D.items():
            point_header = [pt.id, *pt.xyz, *pt.rgb, pt.error]
            fid.write(" ".join(map(str, point_header)) + " ")
            track_strings = []
            for image_id, point2D in zip(pt.image_ids, pt.point2D_idxs):
                track_strings.append(" ".join(map(str, [image_id, point2D])))
            fid.write(" ".join(track_strings) + "\n")


def read_cameras_binary(path_to_model_file):
    """
    see: src/colmap/scene/reconstruction.cc
        void Reconstruction::WriteCamerasBinary(const std::string& path)
        void Reconstruction::ReadCamerasBinary(const std::string& path)
    """
    cameras = {}
    with open(path_to_model_file, "rb") as fid:
        num_cameras = read_next_bytes(fid, 8, "Q")[0]
        for _ in range(num_cameras):
            camera_properties = read_next_bytes(
                fid, num_bytes=24, format_char_sequence="iiQQ"
            )
            camera_id = camera_properties[0]
            model_id = camera_properties[1]
            model_name = CAMERA_MODEL_IDS[camera_properties[1]].model_name
            width = camera_properties[2]
            height = camera_properties[3]
            num_params = CAMERA_MODEL_IDS[model_id].num_params
            params = read_next_bytes(
                fid,
                num_bytes=8 * num_params,
                format_char_sequence="d" * num_params,
            )
            cameras[camera_id] = Camera(
                id=camera_id,
                model=model_name,
                width=width,
                height=height,
                params=np.array(params),
            )
        assert len(cameras) == num_cameras
    return cameras


def write_cameras_binary(cameras, path_to_model_file):
    """
    see: src/base/reconstruction.cc
        void Reconstruction::WriteCamerasBinary(const std::string& path)
        void Reconstruction::ReadCamerasBinary(const std::string& path)
    """
    with open(path_to_model_file, "wb") as fid:
        write_next_bytes(fid, len(cameras), "Q")
        for _, cam in cameras.items():
            model_id = CAMERA_MODEL_NAMES[cam.model].model_id
            camera_properties = [cam.id, model_id, cam.width, cam.height]
            write_next_bytes(fid, camera_properties, "iiQQ")
            for p in cam.params:
                write_next_bytes(fid, float(p), "d")
    return cameras


def read_images_binary(path_to_model_file):
    """
    see: src/colmap/scene/reconstruction.cc
        void Reconstruction::ReadImagesBinary(const std::string& path)
        void Reconstruction::WriteImagesBinary(const std::string& path)
    """
    images = {}
    with open(path_to_model_file, "rb") as fid:
        num_reg_images = read_next_bytes(fid, 8, "Q")[0]
        for _ in range(num_reg_images):
            binary_image_properties = read_next_bytes(
                fid, num_bytes=64, format_char_sequence="idddddddi"
            )
            image_id = binary_image_properties[0]
            qvec = np.array(binary_image_properties[1:5])
            tvec = np.array(binary_image_properties[5:8])
            camera_id = binary_image_properties[8]
            binary_image_name = b""
            current_char = read_next_bytes(fid, 1, "c")[0]
            while current_char != b"\x00":  # look for the ASCII 0 entry
                binary_image_name += current_char
                current_char = read_next_bytes(fid, 1, "c")[0]
            image_name = binary_image_name.decode("utf-8")
            num_points2D = read_next_bytes(
                fid, num_bytes=8, format_char_sequence="Q"
            )[0]
            x_y_id_s = read_next_bytes(
                fid,
                num_bytes=24 * num_points2D,
                format_char_sequence="ddq" * num_points2D,
            )
            xys = np.column_stack(
                [
                    tuple(map(float, x_y_id_s[0::3])),
                    tuple(map(float, x_y_id_s[1::3])),
                ]
            )
            point3D_ids = np.array(tuple(map(int, x_y_id_s[2::3])))
            images[image_id] = Image(
                id=image_id,
                qvec=qvec,
                tvec=tvec,
                camera_id=camera_id,
                name=image_name,
                xys=xys,
                point3D_ids=point3D_ids,
            )
    return images


def write_images_binary(images, path_to_model_file):
    """
    see: src/base/reconstruction.cc
        void Reconstruction::ReadImagesBinary(const std::string& path)
        void Reconstruction::WriteImagesBinary(const std::string& path)
    """
    with open(path_to_model_file, "wb") as fid:
        write_next_bytes(fid, len(images), "Q")
        for _, img in images.items():
            write_next_bytes(fid, img.id, "i")
            write_next_bytes(fid, img.qvec.tolist(), "dddd")
            write_next_bytes(fid, img.tvec.tolist(), "ddd")
            write_next_bytes(fid, img.camera_id, "i")
            for char in img.name:
                write_next_bytes(fid, char.encode("utf-8"), "c")
            write_next_bytes(fid, b"\x00", "c")
            write_next_bytes(fid, len(img.point3D_ids), "Q")
            for xy, p3d_id in zip(img.xys, img.point3D_ids):
                write_next_bytes(fid, [*xy, p3d_id], "ddq")


def read_points3D_binary(path_to_model_file):
    """
    see: src/colmap/scene/reconstruction.cc
        void Reconstruction::ReadPoints3DBinary(const std::string& path)
        void Reconstruction::WritePoints3DBinary(const std::string& path)
    """
    points3D = {}
    with open(path_to_model_file, "rb") as fid:
        num_points = read_next_bytes(fid, 8, "Q")[0]
        for _ in range(num_points):
            binary_point_line_properties = read_next_bytes(
                fid, num_bytes=43, format_char_sequence="QdddBBBd"
            )
            point3D_id = binary_point_line_properties[0]
            xyz = np.array(binary_point_line_properties[1:4])
            rgb = np.array(binary_point_line_properties[4:7])
            error = np.array(binary_point_line_properties[7])
            track_length = read_next_bytes(
                fid, num_bytes=8, format_char_sequence="Q"
            )[0]
            track_elems = read_next_bytes(
                fid,
                num_bytes=8 * track_length,
                format_char_sequence="ii" * track_length,
            )
            image_ids = np.array(tuple(map(int, track_elems[0::2])))
            point2D_idxs = np.array(tuple(map(int, track_elems[1::2])))
            points3D[point3D_id] = Point3D(
                id=point3D_id,
                xyz=xyz,
                rgb=rgb,
                error=error,
                image_ids=image_ids,
                point2D_idxs=point2D_idxs,
            )
    return points3D


def write_points3D_binary(points3D, path_to_model_file):
    """
    see: src/base/reconstruction.cc
        void Reconstruction::ReadPoints3DBinary(const std::string& path)
        void Reconstruction::WritePoints3DBinary(const std::string& path)
    """
    with open(path_to_model_file, "wb") as fid:
        write_next_bytes(fid, len(points3D), "Q")
        for _, pt in points3D.items():
            write_next_bytes(fid, pt.id, "Q")
            write_next_bytes(fid, pt.xyz.tolist(), "ddd")
            write_next_bytes(fid, pt.rgb.tolist(), "BBB")
            write_next_bytes(fid, pt.error, "d")
            track_length = pt.image_ids.shape[0]
            write_next_bytes(fid, track_length, "Q")
            for image_id, point2D_id in zip(pt.image_ids, pt.point2D_idxs):
                write_next_bytes(fid, [image_id, point2D_id], "ii")


def read_portal_csv(path_to_model_file):
    portals = {}
    with open(path_to_model_file, newline='')  as csvfile:
        csv_reader = csv.reader(csvfile)
        for i, row in enumerate(csv_reader):
            image_id = int(row[0])
            short_id = row[1]
            size = row[2]
            tvec = row[3:6]
            qvec = row[6:10]
            coordinates = [float(coord) for coord in row[10:]]

            portals[i] = Portal(
                id=i,
                short_id=short_id,
                qvec=np.array(qvec, dtype=np.float64),
                tvec=np.array(tvec, dtype=np.float64),
                image_id=image_id,
                size=float(size),
                corners=[(coordinates[i], coordinates[i + 1]) for i in range(0, len(coordinates), 2)]
            )
    return portals

def write_portal_csv(portals, csv_path):
    if len(portals) <=0:
        return
    with open(csv_path, mode='w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        for portal in portals.values():
            # image_id, portal_id, portal_size, px, py, pz, qx, qy, qz, qw
            row = [
                portal.image_id,
                portal.short_id,
                portal.size,
                portal.tvec[0], portal.tvec[1], portal.tvec[2],
                portal.qvec[0], portal.qvec[1], portal.qvec[2], portal.qvec[3]
            ]
            for point in portal.corners:
                row.extend(point)
            # Write the row to the CSV file
            csv_writer.writerow(row)
    return


def validate_model_consistency(cameras: Dict[int, 'Camera'], images: Dict[int, 'Image'], points3D: Dict[int, 'Point3D'], logger=None) -> bool:
    """Check camera/image/point3D cross-references for consistency.

    Returns True if valid, False if inconsistencies were detected.
    """
    if logger is None:
        logger = logging.getLogger()

    valid = True
    # Camera references in images
    for iid, img in images.items():
        if img.camera_id not in cameras:
            logger.error("Invalid model: image %d references missing camera id %d", iid, img.camera_id)
            valid = False

    # Image references in points3D
    for pid, p in points3D.items():
        for (iid, p2d_idx) in zip(p.image_ids, p.point2D_idxs):
            if iid not in images:
                logger.error("Invalid model: point3D %d references missing image id %d", pid, iid)
                valid = False
                continue
            img = images[iid]
            if p2d_idx < 0 or p2d_idx >= len(img.xys):
                logger.error("Invalid model: point3D %d references image %d point2D index %d out of bounds (len=%d)", pid, iid, p2d_idx, len(img.xys))
                valid = False

    # Inverse cross-check image point3D_ids vs point tracks
    for iid, img in images.items():
        if img.point3D_ids is None:
            continue
        if img.xys is not None and len(img.point3D_ids) != len(img.xys):
            logger.error("Invalid model: image %d has %d point3D_ids but %d xys", iid, len(img.point3D_ids), len(img.xys))
            valid = False

        for j, pid in enumerate(img.point3D_ids):
            if pid < 0:
                continue
            if pid not in points3D:
                logger.error("Invalid model: image %d observation %d references missing point3D id %d", iid, j, pid)
                valid = False
                continue
            p = points3D[pid]
            if not any(image_id == iid and point2D_idx == j for image_id, point2D_idx in zip(p.image_ids, p.point2D_idxs)):
                logger.error("Invalid model: image %d observation %d -> point3D %d has no matching track entry", iid, j, pid)
                valid = False

    # Optional: verify every point3D track is mirrored in image point3D_ids
    for pid, p in points3D.items():
        for (iid, p2d_idx) in zip(p.image_ids, p.point2D_idxs):
            if iid not in images:
                continue
            img = images[iid]
            if p2d_idx < 0 or p2d_idx >= len(img.point3D_ids):
                continue
            if img.point3D_ids[p2d_idx] != pid:
                logger.error("Invalid model: point3D %d track says image %d idx %d but image lists %d", pid, iid, p2d_idx, img.point3D_ids[p2d_idx])
                valid = False

    if valid:
        logger.info("Model consistency check passed: cameras=%d, images=%d, points3D=%d", len(cameras), len(images), len(points3D))
    else:
        logger.error("Model consistency check failed: cameras=%d, images=%d, points3D=%d", len(cameras), len(images), len(points3D))
    return valid


def read_model(path, ext="", logger=None, validate_consistency=True):
    if logger is None:
        logger = logging.getLogger()

    # try to detect the extension automatically
    if ext == "":
        if detect_model_format(path, ".bin", logger=logger):
            ext = ".bin"
        elif detect_model_format(path, ".txt", logger=logger):
            ext = ".txt"
        else:
            logger.error("Provide model format: '.bin' or '.txt'")
            return

    if ext == ".txt":
        cameras = read_cameras_text(os.path.join(path, "cameras" + ext))
        images = read_images_text(os.path.join(path, "images" + ext))
        points3D = read_points3D_text(os.path.join(path, "points3D") + ext)
    else:
        cameras = read_cameras_binary(os.path.join(path, "cameras" + ext))
        images = read_images_binary(os.path.join(path, "images" + ext))
        points3D = read_points3D_binary(os.path.join(path, "points3D") + ext)
    
    if validate_consistency:
        validate_model_consistency(cameras, images, points3D, logger=logger)
    
    return cameras, images, points3D


def write_model(cameras, images, points3D, path, ext=".bin"):
    if ext == ".txt":
        write_cameras_text(cameras, os.path.join(path, "cameras" + ext))
        write_images_text(images, os.path.join(path, "images" + ext))
        write_points3D_text(points3D, os.path.join(path, "points3D") + ext)
    else:
        write_cameras_binary(cameras, os.path.join(path, "cameras" + ext))
        write_images_binary(images, os.path.join(path, "images" + ext))
        write_points3D_binary(points3D, os.path.join(path, "points3D") + ext)
    return cameras, images, points3D


def apply_similarity_to_new_model(cams: Dict[int, 'Camera'], imgs: Dict[int, 'BaseImage'], pts: Dict[int, 'Point3D'],
                                  T_a: np.ndarray) -> Tuple[Dict[int, 'Camera'], Dict[int, 'BaseImage'], Dict[int, 'Point3D']]:
    """
    Transform the entire new model (cameras & points) into the reference frame 
    using a 4x4 similarity transformation matrix.
    """
    
    # Extract scale and pure rotation from the 4x4 matrix
    # T_a[:3, :3] contains s * R_a. We can find 's' by taking the norm of the first column.
    s = np.linalg.norm(T_a[:3, 0])
    R_a = T_a[:3, :3] / s

    # transform images: update rotations and translations via centers
    new_imgs = {}
    for iid, im in imgs.items():
        Rn = qvec2rotmat(im.qvec)
        tn = im.tvec
        Cn = cam_center_from_extrinsics(Rn, tn)
        
        # Apply 4x4 transformation to the camera center using homogeneous coordinates
        Cn_hom = np.append(Cn, 1.0)
        C_ref = (T_a @ Cn_hom)[:3]
        
        R_ref = Rn @ R_a.T
        t_ref = -R_ref @ C_ref

        rot_ref = R.from_matrix(R_ref)
        q_ref2 = rot_ref.as_quat()
        # SciPy returns [x, y, z, w], but COLMAP uses [w, x, y, z]
        q_ref2 = np.array([q_ref2[3], q_ref2[0], q_ref2[1], q_ref2[2]])
        new_imgs[iid] = BaseImage(iid, q_ref2, t_ref, im.camera_id, im.name, im.xys, im.point3D_ids)
        
    # transform points
    new_pts = {}
    for pid, p in pts.items():
        # Apply 4x4 transformation to 3D points
        p_hom = np.append(p.xyz, 1.0)
        X_ref = (T_a @ p_hom)[:3]
        new_pts[pid] = Point3D(pid, X_ref, p.rgb, p.error, p.image_ids, p.point2D_idxs)
        
    # cameras unchanged (intrinsics)
    return cams, new_imgs, new_pts


def merge_models(reference_model: Tuple[Dict[int,Camera], Dict[int,Image], Dict[int,Point3D]],
                 new_model: Tuple[Dict[int,Camera], Dict[int,Image], Dict[int,Point3D]],
                 new_name_prefix: Optional[str] = "new/") -> Tuple[Dict[int,Camera], Dict[int,Image], Dict[int,Point3D], Dict[int,int]]:
    cams_r, imgs_r, pts_r = reference_model
    cams_n, imgs_n, pts_n = new_model

    next_cam_id = (max(cams_r.keys()) + 1) if cams_r else 1
    next_img_id = (max(imgs_r.keys()) + 1) if imgs_r else 1
    next_pt_id  = (max(pts_r.keys())  + 1) if pts_r  else 1

    # 1) Cameras: append and map ids
    cam_map = {}
    for cid, c in cams_n.items():
        cam_map[cid] = next_cam_id
        cams_r[next_cam_id] = Camera(next_cam_id, c.model, c.width, c.height, list(c.params))
        next_cam_id += 1

    # 2) Images: append, avoid name clashes, remember image id remap
    existing_names = set([im.name for im in imgs_r.values()])
    img_map = {}
    for iid, im in imgs_n.items():
        new_name = im.name
        if new_name in existing_names:
            new_name = (new_name_prefix or "new/") + new_name
        img_map[iid] = next_img_id
        # Temporarily copy point3D_ids; we'll fix them after adding points (once we know pid remap)
        imgs_r[next_img_id] = BaseImage(
            next_img_id, im.qvec, im.tvec, cam_map[im.camera_id],
            new_name, im.xys.copy(), im.point3D_ids.copy()
        )
        existing_names.add(new_name)
        next_img_id += 1

    # 3) Points: append with new ids, rewrite tracks with remapped image ids
    oldpid_to_newpid: Dict[int, int] = {}
    for old_pid, p in pts_n.items():
        new_img_ids = []
        new_pt2ds = []
        for (old_iid, j) in zip(p.image_ids, p.point2D_idxs):
            if old_iid in img_map:
                new_img_ids.append(img_map[old_iid])
                new_pt2ds.append(j)

        if len(new_img_ids) >= 2:
            new_pid = next_pt_id
            oldpid_to_newpid[old_pid] = new_pid
            pts_r[new_pid] = Point3D(new_pid, p.xyz, p.rgb, p.error, np.array(new_img_ids), np.array(new_pt2ds))
            next_pt_id += 1
        # else: drop point (insufficient observations)

    # 4) Fix images' point3D_ids to reflect new point ids (or -1 if point was dropped)
    for new_iid, im_r in imgs_r.items():
        # Only adjust images that came from the new model (those in img_map values)
        if new_iid in img_map.values():
            # Find the corresponding old image (inverse lookup)
            # Build a small inverse map once to speed up for large sets
            pass

    # Build inverse image map once
    inv_img_map = {new_id: old_id for old_id, new_id in img_map.items()}

    for new_iid, im_r in imgs_r.items():
        if new_iid not in inv_img_map:
            continue  # this is an original reference image; its links stay unchanged
        old_iid = inv_img_map[new_iid]
        # Fetch the original new image to see original point3D_ids (old pids)
        im_old = imgs_n[old_iid]
        old_pids = im_old.point3D_ids
        if old_pids.size == 0:
            continue
        # Map old pids to new pids where available; else set to -1
        mapped = old_pids.copy()
        mask = mapped >= 0
        if mask.any():
            # vectorized map: for speed, use a dict lookup with fallback -1
            mapped_ids = []
            for pid in mapped[mask]:
                mapped_ids.append(oldpid_to_newpid.get(int(pid), -1))
            mapped[mask] = np.array(mapped_ids, dtype=np.int64)
        # namedtuple-based Image is immutable; replace with updated copy
        imgs_r[new_iid] = im_r._replace(point3D_ids=mapped)

    print(f"[merge] merged model (cams={len(cams_r)}, images={len(imgs_r)}, points={len(pts_r)})")
    
    return cams_r, imgs_r, pts_r, oldpid_to_newpid



# Load from COLMAP
class Model:
    def __init__(self):
        self.cameras = []
        self.images = []
        self.points3D = []
        self.portals=[]
        self.__vis=None
        self._path=None

    def read_model(self, path, ext="", logger=None):
        self.cameras, self.images, self.points3D = read_model(path, ext, logger)
        self.portals = read_portal_csv(os.path.join(path, "portals.csv"))
        self._path = path

    def write_model(self, path, ext=".bin"):
        write_model(self.cameras, self.images, self.points3D, path)
        write_portal_csv(self.portals, os.path.join(path, "portals.csv"))
        return


    def add_points(self, min_track_len=3, remove_statistical_outlier=True):
        pcd = open3d.geometry.PointCloud()

        xyz = []
        rgb = []
        for point3D in self.points3D.values():
            track_len = len(point3D.point2D_idxs)
            if track_len < min_track_len:
                continue
            xyz.append(point3D.xyz)
            rgb.append(point3D.rgb / 255)

        pcd.points = open3d.utility.Vector3dVector(xyz)
        pcd.colors = open3d.utility.Vector3dVector(rgb)

        # remove obvious outliers
        if remove_statistical_outlier:
            [pcd, _] = pcd.remove_statistical_outlier(
                nb_neighbors=20, std_ratio=2.0
            )

        # open3d.visualization.draw_geometries([pcd])
        self.__vis.add_geometry(pcd)
        self.__vis.poll_events()
        self.__vis.update_renderer()

    def add_cameras(self, scale=1):
        frames = []
        for img in self.images.values():
            # rotation
            R = qvec2rotmat(img.qvec)

            # translation
            t = img.tvec

            # invert
            t = -R.T @ t
            R = R.T

            # intrinsics
            cam = self.cameras[img.camera_id]

            if cam.model in ("SIMPLE_PINHOLE", "SIMPLE_RADIAL", "RADIAL"):
                fx = fy = cam.params[0]
                cx = cam.params[1]
                cy = cam.params[2]
            elif cam.model in (
                "PINHOLE",
                "OPENCV",
                "OPENCV_FISHEYE",
                "FULL_OPENCV",
            ):
                fx = cam.params[0]
                fy = cam.params[1]
                cx = cam.params[2]
                cy = cam.params[3]
            else:
                raise Exception("Camera model not supported")

            # intrinsics
            K = np.identity(3)
            K[0, 0] = fx
            K[1, 1] = fy
            K[0, 2] = cx
            K[1, 2] = cy

            # create axis, plane and pyramed geometries that will be drawn
            cam_model = draw_camera(K, R, t, cam.width, cam.height, scale)
            frames.extend(cam_model)

        # add geometries to visualizer
        for i in frames:
            self.__vis.add_geometry(i)
    
    def get_points(self, in_opengl=False, min_track_len=3, remove_statistical_outlier=True):
        pcd = open3d.geometry.PointCloud()

        xyz = []
        rgb = []
        for point3D in self.points3D.values():
            track_len = len(point3D.point2D_idxs)
            if track_len < min_track_len:
                continue
            xyz.append(point3D.xyz)
            rgb.append(point3D.rgb / 255)

        pcd.points = open3d.utility.Vector3dVector(xyz)
        pcd.colors = open3d.utility.Vector3dVector(rgb)

        # remove obvious outliers
        if remove_statistical_outlier:
            [pcd, _] = pcd.remove_statistical_outlier(
                nb_neighbors=20, std_ratio=2.0
            )

        # convert to opengl coordinate 
        if in_opengl:
            transformation_matrix = np.array([
                [0, 1, 0, 0],
                [1, 0, 0, 0],
                [0, 0, -1, 0],
                [0, 0, 0, 1]
            ])
            return pcd.transform(transformation_matrix)
        
        return pcd

    def get_portals(self, in_opengl=False):
        portals = []
        for portal in self.portals.values():
            tvec, qvec = portal.tvec, portal.qvec
            if in_opengl:
                tvec, qvec = convert_pose_colmap_to_opengl(tvec, qvec)
            portals.append(
                {
                    "short_id": portal.short_id, 
                    "tvec": tvec,
                    "qvec": qvec,
                    "image_id": portal.image_id, 
                    "size": portal.size, 
                    "corners": portal.corners
                }
            )
        return portals
    
    def get_path(self):
        return self._path

    def create_window(self):
        self.__vis = open3d.visualization.Visualizer()
        self.__vis.create_window()

    def show(self):
        self.__vis.poll_events()
        self.__vis.update_renderer()
        self.__vis.run()
        self.__vis.destroy_window()


def draw_camera(K, R, t, w, h, scale=1, color=[0.8, 0.2, 0.8]):
    """Create axis, plane and pyramed geometries in Open3D format.
    :param K: calibration matrix (camera intrinsics)
    :param R: rotation matrix
    :param t: translation
    :param w: image width
    :param h: image height
    :param scale: camera model scale
    :param color: color of the image plane and pyramid lines
    :return: camera model geometries (axis, plane and pyramid)
    """

    # intrinsics
    K = K.copy() / scale
    Kinv = np.linalg.inv(K)

    # 4x4 transformation
    T = np.column_stack((R, t))
    T = np.vstack((T, (0, 0, 0, 1)))

    # axis
    axis = open3d.geometry.TriangleMesh.create_coordinate_frame(
        size=0.5 * scale
    )
    axis.transform(T)

    # points in pixel
    points_pixel = [
        [0, 0, 0],
        [0, 0, 1],
        [w, 0, 1],
        [0, h, 1],
        [w, h, 1],
    ]

    # pixel to camera coordinate system
    points = [Kinv @ p for p in points_pixel]

    # image plane
    width = abs(points[1][0]) + abs(points[3][0])
    height = abs(points[1][1]) + abs(points[3][1])
    plane = open3d.geometry.TriangleMesh.create_box(width, height, depth=1e-6)
    plane.paint_uniform_color(color)
    plane.translate([points[1][0], points[1][1], scale])
    plane.transform(T)

    # pyramid
    points_in_world = [(R @ p + t) for p in points]
    lines = [
        [0, 1],
        [0, 2],
        [0, 3],
        [0, 4],
    ]
    colors = [color for i in range(len(lines))]
    line_set = open3d.geometry.LineSet(
        points=open3d.utility.Vector3dVector(points_in_world),
        lines=open3d.utility.Vector2iVector(lines),
    )
    line_set.colors = open3d.utility.Vector3dVector(colors)

    # return as list in Open3D format
    return [axis, plane, line_set]


def load_yaml(filepath):
    with open(filepath, 'r') as file:
        return yaml.safe_load(file)


def save_to_yaml(data):
    filename = os.path.join(data['output_dir'], 'config.yaml')
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as yaml_file:
        yaml.dump(data, yaml_file, default_flow_style=False)

def save_meshes_obj(meshes, filename):
    # Initialize lists to accumulate lines and vertex offset for OBJ indexing
    lines = []
    vertex_offset = 0
    lines.append(f'g ParentGroup')

    for mesh in meshes:
        trimesh_obj = trimesh.Trimesh(np.asarray(mesh.vertices), np.asarray(mesh.triangles))
        mesh_obj = trimesh.exchange.obj.export_obj(trimesh_obj)
            # Split lines and add a group name at the top
        mesh_lines = mesh_obj.splitlines()
        
        lines.append(f'o {str(uuid.uuid4())}:0')
        # Update vertex indices to account for previous vertices
        for i, line in enumerate(mesh_lines):
            if line.startswith('v '):
                lines.append(line)
            elif line.startswith('f '):  # Face definitions need updated indices
                parts = line.split()
                parts[1:] = [str(int(index) + vertex_offset) for index in parts[1:]]
                mesh_lines[i] = ' '.join(parts)
                lines.append(mesh_lines[i])
        
        # Append updated lines to main list and update vertex offset
        # lines.extend(mesh_lines)
        vertex_offset += len([line for line in mesh_lines if line.startswith('v ')])

    # Save the combined lines to an OBJ file
    with open(filename, 'w') as f:
        f.write('\n'.join(lines))
    return

def cam_center_from_extrinsics(R: np.ndarray, t: np.ndarray) -> np.ndarray:
    # world->cam: x_c = R X + t; center C satisfies R C + t = 0 => C = -R^T t
    return -R.T @ t