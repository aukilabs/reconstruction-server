"""Reads a scan's input files from datasets/<scan>/."""
import json
import csv
import logging
from typing import NamedTuple, Dict
from reconstruction.io.portals import load_portals_json, load_qr_detections_csv
from reconstruction.io.job_meta import load_scan_data_summary


def get_data_paths(group_folder, logger_name=None):
    logger = logging.getLogger(logger_name)
    path_to_truth_portals = group_folder / "portals.json"
    if path_to_truth_portals.exists():
        truth_portal_poses = load_portals_json(path_to_truth_portals)
    else:
        truth_portal_poses = None

    dataset_paths = []
    for scan_folder in group_folder.iterdir():
        if scan_folder.is_dir() and (
            scan_folder.name.startswith("dmt_scan_")
            or scan_folder.name.startswith("20")
        ):
            dataset_paths.append(scan_folder)

    logger.info(f"Using {len(dataset_paths)} scans from folder '{group_folder.name}'")
    
    if truth_portal_poses:
        logger.info(f"Found {len(truth_portal_poses.keys())} truth portal poses: ")
        logger.info("\n".join(f"{id}: {value}" for id, value in truth_portal_poses.items()))
    else:
        logger.info("No truth provided (portals.json). Will skip comparison with ground truth.")

    return truth_portal_poses, dataset_paths


class RefinementData(NamedTuple):
    """Container for refinement data."""
    timestamps_per_image: Dict
    intrinsics_per_timestamp: Dict
    ar_poses_per_timestamp: Dict
    qr_detections_per_timestamp: Dict
    portal_sizes: Dict


def load_scan_summary(scan_folder_path, logger):
    """
    Load scan summary data from json file.
    
    Args:
        job_root_path: Path to job root directory
        logger: Logger instance
        
    Returns:
        Dictionary of portal sizes
    """
    portal_sizes = {}
    job_root_path = scan_folder_path.parent.parent
    try:
        scan_data_summary = load_scan_data_summary(job_root_path)
        if scan_data_summary is not None:
            for portal_id, portal_size in zip(
                scan_data_summary["portalIDs"],
                scan_data_summary["portalSizes"]
            ):
                portal_sizes[portal_id] = portal_size
        else:
            scan_manifest_path = scan_folder_path / "Manifest.json"
            if scan_manifest_path.exists():
                scan_manifest = json.load(open(scan_manifest_path))
                for portal in scan_manifest["portals"]:
                    portal_sizes[portal["shortId"]] = portal["physicalSize"]
    except Exception as e:
        logger.error(f"Failed to read job scan summary: {e}")
        raise

    return portal_sizes


def load_scan(
    paths,
    use_frames_from_video,
    original_image_count,
    logger
):
    """
    Load all metadata for the dataset including timestamps, intrinsics, poses, and QR detections.
    
    Returns:
        RefinementData containing all loaded metadata
    """
    # Load portal sizes
    portal_sizes = load_scan_summary(paths.scan_folder, logger)


    # Load timestamps
    timestamps_per_image = {}
    frames_csv_path = paths.scan_folder / "Frames.csv"
    logger.info(f'Loading image timestamps from {frames_csv_path}')

    with open(frames_csv_path, newline='') as csvfile:
        frame_index = 0
        for row in csv.reader(csvfile):
            timestamp = round(float(row[0]) * 1e9)  # s to ns
            filename = (f"{paths.scan_folder.name}_{frame_index:06d}.jpg" if use_frames_from_video 
                       else row[1])
            frame_index += 1
            timestamps_per_image[filename] = timestamp
    logger.info(f'{len(timestamps_per_image)}, frame timestamps loaded')


    # Load camera intrinsics
    intrinsics_per_timestamp = {}
    cam_intrinsics_path = paths.scan_folder / "CameraIntrinsics.csv"
    logger.info(f'Loading camera intrinsics from {cam_intrinsics_path}')
    
    with open(cam_intrinsics_path, newline='') as csvfile:
        for row in csv.reader(csvfile):
            timestamp = round(float(row[0]) * 1e9)  # s to ns
            intrinsics_per_timestamp[timestamp] = [
                float(row[1]), float(row[2]), # focal distance (fx, fy)
                float(row[3]), float(row[4]), # principal point (cx, cy)
                int(row[5]), int(row[6])      # resolution (w, h)
            ]
    logger.info(f'{len(intrinsics_per_timestamp)}, camera frame intrinsics loaded')


    # Load AR poses
    ar_poses_per_timestamp = {}
    ar_poses_path = paths.scan_folder / "ARposes.csv"
    logger.info(f'Loading AR poses from {ar_poses_path}')
    
    with open(ar_poses_path, newline='') as csvfile:
        for row in csv.reader(csvfile):
            timestamp = round(float(row[0]) * 1e9)  # s to ns
            ar_poses_per_timestamp[timestamp] = [float(val) for val in row[1:8]]
    logger.info(f'{len(ar_poses_per_timestamp)}, AR poses loaded')


    # Load QR detections
    qr_detections_path = (paths.scan_folder / "PortalDetections.csv" 
                         if (paths.scan_folder / "PortalDetections.csv").exists() 
                         else paths.scan_folder / "Observations.csv")
    logger.info(f'Loading QR detections from {qr_detections_path}')
    
    qr_detections_per_timestamp = load_qr_detections_csv(str(qr_detections_path))
    logger.info(f'{len(qr_detections_per_timestamp)}, QR detections loaded')

    # Validate data
    for data_dict, name in [
        (timestamps_per_image, "Timestamps"),
        (intrinsics_per_timestamp, "Camera Intrinsics"),
        (ar_poses_per_timestamp, "AR Poses")
    ]:
        if len(data_dict) != original_image_count:
            raise ValueError(f"Mismatching number of Frames and {name}. "
                           f"Expected {original_image_count}, got {len(data_dict)}")
        

    return RefinementData(
        timestamps_per_image=timestamps_per_image,
        intrinsics_per_timestamp=intrinsics_per_timestamp,
        ar_poses_per_timestamp=ar_poses_per_timestamp,
        qr_detections_per_timestamp=qr_detections_per_timestamp,
        portal_sizes=portal_sizes 
    )
