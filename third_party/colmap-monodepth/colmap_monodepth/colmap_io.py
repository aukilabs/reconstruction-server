"""COLMAP sparse model I/O and pose/intrinsic conversion for DA3 priors."""

from __future__ import annotations

import collections
import os
import struct
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np

CameraModel = collections.namedtuple("CameraModel", ["model_id", "model_name", "num_params"])
Camera = collections.namedtuple("Camera", ["id", "model", "width", "height", "params"])
BaseImage = collections.namedtuple(
    "Image", ["id", "qvec", "tvec", "camera_id", "name", "xys", "point3D_ids"]
)
Point3D = collections.namedtuple(
    "Point3D", ["id", "xyz", "rgb", "error", "image_ids", "point2D_idxs"]
)


class Image(BaseImage):
    def qvec2rotmat(self) -> np.ndarray:
        return qvec2rotmat(self.qvec)


CAMERA_MODELS = {
    CameraModel(0, "SIMPLE_PINHOLE", 3),
    CameraModel(1, "PINHOLE", 4),
    CameraModel(2, "SIMPLE_RADIAL", 4),
    CameraModel(3, "RADIAL", 5),
    CameraModel(4, "OPENCV", 8),
}
CAMERA_MODEL_IDS = {m.model_id: m for m in CAMERA_MODELS}
CAMERA_MODEL_NAMES = {m.model_name: m for m in CAMERA_MODELS}


def qvec2rotmat(qvec: np.ndarray) -> np.ndarray:
    """COLMAP quaternion (w, x, y, z) → rotation matrix."""
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


def rotmat2qvec(R: np.ndarray) -> np.ndarray:
    """Rotation matrix → COLMAP quaternion (w, x, y, z)."""
    Rxx, Ryx, Rzx, Rxy, Ryy, Rzy, Rxz, Ryz, Rzz = R.flat
    K = (
        np.array(
            [
                [Rxx - Ryy - Rzz, 0, 0, 0],
                [Ryx + Rxy, Ryy - Rxx - Rzz, 0, 0],
                [Rzx + Rxz, Rzy + Ryz, Rzz - Rxx - Ryy, 0],
                [Ryz - Rzy, Rzx - Rxz, Rxy - Ryx, Rxx + Ryy + Rzz],
            ]
        )
        / 3.0
    )
    eigvals, eigvecs = np.linalg.eigh(K)
    qvec = eigvecs[[3, 0, 1, 2], np.argmax(eigvals)]
    if qvec[0] < 0:
        qvec *= -1
    return qvec


def camera_to_K(camera: Camera) -> np.ndarray:
    """Build 3x3 intrinsics. Matches DA3 ColmapHandler conventions."""
    if camera.model == "PINHOLE":
        fx, fy, cx, cy = camera.params
    elif camera.model == "SIMPLE_PINHOLE":
        f, cx, cy = camera.params
        fx = fy = f
    else:
        fx = fy = float(camera.params[0]) if len(camera.params) > 0 else 1000.0
        cx = camera.width / 2.0
        cy = camera.height / 2.0
    return np.array([[fx, 0.0, cx], [0.0, fy, cy], [0.0, 0.0, 1.0]], dtype=np.float64)


def image_to_extrinsic(image: Image) -> np.ndarray:
    """World-to-camera 4x4 (OpenCV / COLMAP), as required by DA3 `extrinsics`."""
    extrinsic = np.eye(4, dtype=np.float64)
    extrinsic[:3, :3] = image.qvec2rotmat()
    extrinsic[:3, 3] = image.tvec
    return extrinsic


def _read_next_bytes(fid, num_bytes, fmt, endian="<"):
    data = fid.read(num_bytes)
    return struct.unpack(endian + fmt, data)


def read_cameras_text(path: str) -> Dict[int, Camera]:
    cameras = {}
    with open(path) as fid:
        for line in fid:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            elems = line.split()
            camera_id = int(elems[0])
            model = elems[1]
            width, height = int(elems[2]), int(elems[3])
            params = np.array(tuple(map(float, elems[4:])))
            cameras[camera_id] = Camera(camera_id, model, width, height, params)
    return cameras


def write_cameras_text(cameras: Dict[int, Camera], path: str) -> None:
    with open(path, "w") as fid:
        fid.write("# CAMERA_ID, MODEL, WIDTH, HEIGHT, PARAMS[]\n")
        for cam in cameras.values():
            params = " ".join(str(p) for p in cam.params)
            fid.write(f"{cam.id} {cam.model} {cam.width} {cam.height} {params}\n")


def read_images_text(path: str) -> Dict[int, Image]:
    """COLMAP images.txt: two lines per image (header + POINTS2D)."""
    images = {}
    with open(path) as fid:
        while True:
            line = fid.readline()
            if not line:
                break
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            elems = line.split()
            image_id = int(elems[0])
            qvec = np.array(tuple(map(float, elems[1:5])))
            tvec = np.array(tuple(map(float, elems[5:8])))
            camera_id = int(elems[8])
            name = elems[9]
            pts_line = fid.readline()
            xys = np.zeros((0, 2))
            point3D_ids = np.zeros((0,), dtype=np.int64)
            if pts_line is not None:
                pts = pts_line.split()
                if pts:
                    vals = list(map(float, pts))
                    xys = np.column_stack([vals[0::3], vals[1::3]])
                    point3D_ids = np.array(vals[2::3], dtype=np.int64)
            images[image_id] = Image(image_id, qvec, tvec, camera_id, name, xys, point3D_ids)
    return images


def write_images_text(images: Dict[int, Image], path: str) -> None:
    with open(path, "w") as fid:
        fid.write(
            "# IMAGE_ID, QW, QX, QY, QZ, TX, TY, TZ, CAMERA_ID, NAME\n"
            "# POINTS2D[] as (X, Y, POINT3D_ID)\n"
        )
        for img in images.values():
            q = " ".join(str(x) for x in img.qvec)
            t = " ".join(str(x) for x in img.tvec)
            fid.write(f"{img.id} {q} {t} {img.camera_id} {img.name}\n")
            pts = []
            for xy, pid in zip(img.xys, img.point3D_ids):
                pts.append(f"{xy[0]} {xy[1]} {int(pid)}")
            fid.write(" ".join(pts) + "\n")


def read_points3D_text(path: str) -> Dict[int, Point3D]:
    points3D = {}
    if not os.path.isfile(path):
        return points3D
    with open(path) as fid:
        for line in fid:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            elems = line.split()
            pid = int(elems[0])
            xyz = np.array(tuple(map(float, elems[1:4])))
            rgb = np.array(tuple(map(int, elems[4:7])))
            error = float(elems[7])
            track = elems[8:]
            image_ids = np.array(tuple(map(int, track[0::2]))) if track else np.zeros(0, dtype=int)
            point2D_idxs = np.array(tuple(map(int, track[1::2]))) if track else np.zeros(0, dtype=int)
            points3D[pid] = Point3D(pid, xyz, rgb, error, image_ids, point2D_idxs)
    return points3D


def write_points3D_text(points3D: Dict[int, Point3D], path: str) -> None:
    with open(path, "w") as fid:
        fid.write("# POINT3D_ID, X, Y, Z, R, G, B, ERROR, TRACK[] as (IMAGE_ID, POINT2D_IDX)\n")
        for pt in points3D.values():
            xyz = " ".join(str(x) for x in pt.xyz)
            rgb = " ".join(str(int(c)) for c in pt.rgb)
            track = []
            for iid, p2d in zip(pt.image_ids, pt.point2D_idxs):
                track.append(f"{int(iid)} {int(p2d)}")
            fid.write(f"{pt.id} {xyz} {rgb} {pt.error} {' '.join(track)}\n")


def read_cameras_binary(path: str) -> Dict[int, Camera]:
    cameras = {}
    with open(path, "rb") as fid:
        num_cameras = _read_next_bytes(fid, 8, "Q")[0]
        for _ in range(num_cameras):
            camera_id, model_id, width, height = _read_next_bytes(fid, 24, "iiQQ")
            model_name = CAMERA_MODEL_IDS[model_id].model_name
            num_params = CAMERA_MODEL_IDS[model_id].num_params
            params = np.array(_read_next_bytes(fid, 8 * num_params, "d" * num_params))
            cameras[camera_id] = Camera(camera_id, model_name, width, height, params)
    return cameras


def read_images_binary(path: str) -> Dict[int, Image]:
    images = {}
    with open(path, "rb") as fid:
        num_reg_images = _read_next_bytes(fid, 8, "Q")[0]
        for _ in range(num_reg_images):
            props = _read_next_bytes(fid, 64, "idddddddi")
            image_id = props[0]
            qvec = np.array(props[1:5])
            tvec = np.array(props[5:8])
            camera_id = props[8]
            name_bytes = b""
            while True:
                c = _read_next_bytes(fid, 1, "c")[0]
                if c == b"\x00":
                    break
                name_bytes += c
            name = name_bytes.decode("utf-8")
            num_points2D = _read_next_bytes(fid, 8, "Q")[0]
            x_y_id = _read_next_bytes(fid, 24 * num_points2D, "ddq" * num_points2D) if num_points2D else ()
            xys = np.column_stack([x_y_id[0::3], x_y_id[1::3]]) if num_points2D else np.zeros((0, 2))
            point3D_ids = np.array(x_y_id[2::3], dtype=np.int64) if num_points2D else np.zeros(0, dtype=np.int64)
            images[image_id] = Image(image_id, qvec, tvec, camera_id, name, xys, point3D_ids)
    return images


def read_points3D_binary(path: str) -> Dict[int, Point3D]:
    points3D = {}
    if not os.path.isfile(path):
        return points3D
    with open(path, "rb") as fid:
        num_points = _read_next_bytes(fid, 8, "Q")[0]
        for _ in range(num_points):
            props = _read_next_bytes(fid, 43, "QdddBBBd")
            pid = props[0]
            xyz = np.array(props[1:4])
            rgb = np.array(props[4:7])
            error = float(props[7])
            track_length = _read_next_bytes(fid, 8, "Q")[0]
            track = _read_next_bytes(fid, 8 * track_length, "ii" * track_length) if track_length else ()
            image_ids = np.array(track[0::2], dtype=int) if track_length else np.zeros(0, dtype=int)
            point2D_idxs = np.array(track[1::2], dtype=int) if track_length else np.zeros(0, dtype=int)
            points3D[pid] = Point3D(pid, xyz, rgb, error, image_ids, point2D_idxs)
    return points3D


def detect_model_format(path: str) -> str:
    if all(os.path.isfile(os.path.join(path, f"{n}.bin")) for n in ("cameras", "images", "points3D")):
        return ".bin"
    if all(os.path.isfile(os.path.join(path, f"{n}.txt")) for n in ("cameras", "images")):
        return ".txt"
    raise FileNotFoundError(
        f"No COLMAP model (cameras/images [.txt|.bin]) found under {path}"
    )


def read_model(path: str, ext: str = "") -> Tuple[Dict[int, Camera], Dict[int, Image], Dict[int, Point3D]]:
    if not ext:
        ext = detect_model_format(path)
    if ext == ".txt":
        cameras = read_cameras_text(os.path.join(path, "cameras.txt"))
        images = read_images_text(os.path.join(path, "images.txt"))
        points3D = read_points3D_text(os.path.join(path, "points3D.txt"))
    else:
        cameras = read_cameras_binary(os.path.join(path, "cameras.bin"))
        images = read_images_binary(os.path.join(path, "images.bin"))
        points3D = read_points3D_binary(os.path.join(path, "points3D.bin"))
    return cameras, images, points3D


def write_model(
    cameras: Dict[int, Camera],
    images: Dict[int, Image],
    points3D: Dict[int, Point3D],
    path: str,
    ext: str = ".txt",
) -> None:
    os.makedirs(path, exist_ok=True)
    if ext != ".txt":
        raise ValueError("This package writes COLMAP text models only (.txt).")
    write_cameras_text(cameras, os.path.join(path, "cameras.txt"))
    write_images_text(images, os.path.join(path, "images.txt"))
    write_points3D_text(points3D, os.path.join(path, "points3D.txt"))


def _has_colmap_cameras(path: Path) -> bool:
    return (path / "cameras.txt").is_file() or (path / "cameras.bin").is_file()


def resolve_sparse_dir(colmap_dir: str, sparse_subdir: str = "") -> Path:
    """Locate the COLMAP model directory.

    Tries, in order (when ``sparse_subdir`` is empty):
      1. ``<colmap_dir>/sparse`` if it contains cameras
      2. ``<colmap_dir>/sparse/0``
      3. ``<colmap_dir>`` itself (model files directly in the dataset root)
    """
    root = Path(colmap_dir)
    if sparse_subdir:
        sparse = root / "sparse" / sparse_subdir
        if not sparse.is_dir():
            raise FileNotFoundError(f"Sparse reconstruction directory not found: {sparse}")
        return sparse

    candidates = [root / "sparse", root / "sparse" / "0", root]
    for candidate in candidates:
        if candidate.is_dir() and _has_colmap_cameras(candidate):
            return candidate

    raise FileNotFoundError(
        "No COLMAP model found. Looked for cameras.txt/.bin under "
        f"{root / 'sparse'}, {root / 'sparse' / '0'}, and {root}"
    )


def load_colmap_scene(
    colmap_dir: str,
    sparse_subdir: str = "",
    images_subdir: str = "images",
) -> Tuple[List[str], np.ndarray, np.ndarray]:
    """Load COLMAP poses/intrinsics for DA3 pose conditioning.

    Returns
    -------
    image_paths : list[str]
    extrinsics : (N, 4, 4) float64 world-to-camera
    intrinsics : (N, 3, 3) float64
    """
    colmap_dir = str(colmap_dir)
    images_dir = Path(colmap_dir) / images_subdir
    if not images_dir.is_dir():
        raise FileNotFoundError(f"Images directory not found: {images_dir}")

    sparse_dir = resolve_sparse_dir(colmap_dir, sparse_subdir)
    cameras, images, _points3D = read_model(str(sparse_dir))

    image_paths: List[str] = []
    extrinsics: List[np.ndarray] = []
    intrinsics: List[np.ndarray] = []

    # Stable order by image id (COLMAP registration order)
    for _image_id, image_data in sorted(images.items(), key=lambda kv: kv[0]):
        image_path = images_dir / image_data.name
        if not image_path.is_file():
            # allow nested names already containing subdirs
            alt = Path(colmap_dir) / image_data.name
            if alt.is_file():
                image_path = alt
            else:
                continue
        camera = cameras[image_data.camera_id]
        image_paths.append(str(image_path))
        extrinsics.append(image_to_extrinsic(image_data))
        intrinsics.append(camera_to_K(camera))

    if not image_paths:
        raise FileNotFoundError(
            f"No images from sparse model found under {images_dir}"
        )

    return (
        image_paths,
        np.stack(extrinsics, axis=0),
        np.stack(intrinsics, axis=0),
    )
