"""portals.csv, portals.json and QR detection CSV: readers and writers."""
import pycolmap
import json
import numpy as np
import csv
import os
from typing import List
import collections
from reconstruction.geometry.frames import COLMAP_WORLD, OPENGL, convert_pose_convention


def load_portals_json(file_path):
    portal_poses = {}

    # Have to rotate 90 deg for now since the DMT Recorder captures rotation after doing a 90 deg in the app.
    # This function however loads raw poses from domain which are not rotated 90 deg.
    # This comes from the misconception where apps treat identity quaternions as "floor" while domains are "wall".
    # This should be changed in the DMT recorder to capture same as domain format correctly.
    rot90 = pycolmap.Rotation3d(np.array([np.pi/2, 0.0, 0.0]))

    with open(file_path) as f:
        json_data = json.load(f)
        for entry in json_data["poses"]:
            pos = np.array([entry["px"], entry["py"], entry["pz"]])
            quat = np.array([entry["rx"], entry["ry"], entry["rz"], entry["rw"]])

            # quat = (pycolmap.Rotation3d(quat) * rot90).quat # TODO: remove later (see above)

            pos, quat = convert_pose_convention(pos, quat, OPENGL, COLMAP_WORLD)

            portal_poses[entry["short_id"]] = pycolmap.Rigid3d(pycolmap.Rotation3d(quat), pos)

    return portal_poses


def load_qr_detections_csv(csv_path):
    detections_per_timestamp = {}
    with open(csv_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            timestamp = round(float(row[0]) * 1e9) # s to ns
            pose_values = [float(val) for val in row[2:9]] # px, py, pz, rx, ry, rz, rw
            pos = pose_values[:3]
            quat = pose_values[3:]

            pos, quat = convert_pose_convention(pos, quat, OPENGL, COLMAP_WORLD)

            rot3d = pycolmap.Rotation3d(np.array(quat))

            qr_pose = pycolmap.Rigid3d(
                rot3d,
                np.array(pos)
            )

            # Remarks: the coordinates recorded is reference to image bottom right
            # and the order is top-right, bottom-right, bottom-left, and top-left of the portal
            coordinates = [float(coord) for coord in row[9:]]

            detections_per_timestamp[timestamp] = {
                "pose": qr_pose,
                "short_id": row[1],
                "portal_corners": [(coordinates[i], coordinates[i + 1]) for i in range(0, len(coordinates), 2)]
            }

    return detections_per_timestamp


def save_portal_csv(poses_per_qr, csv_path, image_ids_per_qr, portal_sizes, corners_per_qr):
    with open(csv_path, mode='w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        for short_id, qr_poses in poses_per_qr.items():

            corresponding_image_ids = image_ids_per_qr[short_id]
            corresponding_corners = corners_per_qr[short_id]

            for image_id, qr_pose, qr_corners in zip(corresponding_image_ids, qr_poses, corresponding_corners):
                pos, quat = qr_pose.translation, qr_pose.rotation.quat
                corner_array = [coord for coords in qr_corners for coord in coords]
                # Create a row for the CSV
                # Format
                # image_id, portal_id, portal_size, px, py, pz, qx, qy, qz, qw
                row = [
                    image_id,
                    short_id,
                    portal_sizes[short_id],
                    pos[0], pos[1], pos[2],
                    quat[0], quat[1], quat[2], quat[3]
                ]

                row.extend(corner_array)
                # Write the row to the CSV file
                csv_writer.writerow(row)


Portal = collections.namedtuple(
    "Portal", ["id", "short_id", "qvec", "tvec", "image_id", "corners", "size"]
)


def constructPose(qvec: np.array, tvec: np.array) -> pycolmap.Rigid3d:
    return pycolmap.Rigid3d(
        pycolmap.Rotation3d(qvec),
        tvec
    )


def portalPose(portal: Portal) -> pycolmap.Rigid3d:
    return constructPose(portal.qvec, portal.tvec)


def read_portal_csv(path_to_model_file: os.PathLike) -> List[Portal]:
    portals: List[Portal] = []
    with open(path_to_model_file, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for i, row in enumerate(csv_reader):
            image_id = int(row[0])
            short_id = row[1]
            size = row[2]
            tvec = row[3:6]
            qvec = row[6:10]
            coordinates = [float(coord) for coord in row[10:]]

            portal = Portal(
                id=i,
                short_id=short_id,
                qvec=np.array(qvec, dtype=np.float64),
                tvec=np.array(tvec, dtype=np.float64),
                image_id=image_id,
                size=float(size),
                corners=[(coordinates[i], coordinates[i + 1]) for i in range(0, len(coordinates), 2)]
            )
            portals.append(portal)
    return portals


def write_portal_csv(portals: List[Portal], csv_path: str):
    if not portals:
        return

    with open(csv_path, mode='w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        for portal in portals:
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
