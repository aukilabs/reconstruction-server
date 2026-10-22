"""Writes refined_manifest.json and the failed job_manifest.json."""
import pycolmap
import json
import torch
import datetime
import platform
import psutil
import GPUtil
import subprocess
from dateutil import parser
from pathlib import Path
from reconstruction.version import VERSION
from reconstruction.geometry.pose import convert_pose_colmap_to_opengl, mean_pose, rectify_portal_pose
from reconstruction.io.job_meta import load_job_metadata, load_scan_data_summary


def save_failed_manifest_json(json_path, job_root_path, job_status_details):
    save_manifest_json({}, json_path, job_root_path, job_status="failed", job_progress=100, job_status_details=job_status_details)


def save_manifest_json(
    portal_poses, json_path, job_root_path,
    job_status=None, job_progress=None, job_status_details=None,
    portal_sizes=None, scan_alignment_transforms=None, previous_scan_files=None,
    portal_alignment_metrics=None):

    job_root_path = Path(job_root_path)

    version = VERSION
    if VERSION == "develop":
        git_branch = None
        git_commit = None
        try:
            git_branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode().strip()
        except:
            pass
        try:
            git_commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
        except:
            pass

        if git_branch and git_commit:
            version = f"{git_branch}-{git_commit}"
        elif git_commit:
            version = f"{git_commit}"

    manifest_data = {
        "portals": [],
        "reconstructionServerVersion": version,
        "jobStatus": job_status if job_status is not None else "unknown",
        "jobProgress": job_progress if job_progress is not None else 0,
        "jobStatusDetails": job_status_details if job_status_details is not None else "",
        "updatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

    # Lots of try catch to just skip data that is not available but still keep the rest

    #-------------------------
    # JOB METADATA
    #-------------------------

    try:
        job_metadata_json = load_job_metadata(job_root_path)
        if job_metadata_json is not None:

            created_datetime = parser.isoparse(job_metadata_json["created_at"])
            current_datetime = datetime.datetime.now(datetime.timezone.utc)

            manifest_data["createdAt"] = job_metadata_json["created_at"]
            manifest_data["jobDuration"] = float((current_datetime - created_datetime).total_seconds())
            manifest_data["jobID"] = job_metadata_json["id"]
            manifest_data["jobName"] = job_metadata_json["name"]
            manifest_data["reconstructionServerURL"] = job_metadata_json.get("reconstruction_server_url", None)
            manifest_data["domainID"] = job_metadata_json["domain_id"]
            manifest_data["domainServerURL"] = job_metadata_json.get("domain_server_url", None)
            manifest_data["processingType"] = job_metadata_json["processing_type"]
            manifest_data["dataIDs"] = job_metadata_json["data_ids"]
            if previous_scan_files is not None:
                manifest_data["dataIDs"].extend(previous_scan_files)

    except Exception as e:
        print("No job metadata found for manifest")
        print(e)
        pass

    #-------------------------
    # SCAN DATA SUMMARY
    #-------------------------

    if portal_sizes is None:
        portal_sizes = {}
    try:
        scan_data_summary = load_scan_data_summary(job_root_path)
        if scan_data_summary is not None:
            manifest_data["scanDataSummary"] = scan_data_summary
            for portal_id, portal_size in zip(scan_data_summary["portalIDs"], scan_data_summary["portalSizes"]):
                portal_sizes[portal_id] = portal_size
    except Exception as e:
        print("No scan data summary found for manifest")
        print(e)
        pass

    #-------------------------
    # SERVER DETAILS
    #-------------------------

    manifest_data["serverDetails"] = {}

    try:
        manifest_data["serverDetails"]["os"] = platform.platform()
    except Exception as e:
        print("Could not get OS details for manifest")
        print(e)
        pass

    try:
        manifest_data["serverDetails"]["cpu"] = {
            "model": platform.processor(),
            "cores": psutil.cpu_count(logical=False),
            "threads": psutil.cpu_count(logical=True),
            "load": psutil.cpu_percent(interval=1),
        }
    except Exception as e:
        print("Could not get CPU info for manifest")
        print(e)
        pass

    try:
        manifest_data["serverDetails"]["memory"] = {
            "total": psutil.virtual_memory().total,
            "available": psutil.virtual_memory().available,
            "used": psutil.virtual_memory().used,
            "usedPercent": psutil.virtual_memory().percent
        }
    except Exception as e:
        print("Could not get memory info for manifest")
        print(e)
        pass

    try:
        if torch.cuda.is_available():
            manifest_data["serverDetails"]["cudaAvailable"] = True
            manifest_data["serverDetails"]["cudaVersion"] = torch.version.cuda
        else:
            manifest_data["serverDetails"]["cudaAvailable"] = False
    except Exception as e:
        print("Could not get CUDA info for manifest")
        print(e)
        manifest_data["serverDetails"]["cudaAvailable"] = False
        pass

    try:
        manifest_data["serverDetails"]["gpus"] = [
            {
                "name": gpu.name,
                "memoryTotal": gpu.memoryTotal,
                "memoryUsed": gpu.memoryUsed,
                "load": gpu.load,
                "driver": gpu.driver,
            }
            for gpu in GPUtil.getGPUs()
        ] if len(GPUtil.getGPUs()) > 0 else [],
    except Exception as e:
        print("Could not get GPU info for manifest")
        print(e)
        pass

    #-------------------------
    # PORTALS
    #-------------------------

    # poses_for_qr has only one pose after refinement, but other parts of the code expects a list of poses per QR.
    # For now we just take the first
    for short_id, poses_for_qr in portal_poses.items():
        if isinstance(poses_for_qr, pycolmap.Rigid3d):
            pose = poses_for_qr
        else:
            # list of one or many poses
            # Don't run mean_pose if we already reduced to one pose before.
            pose = poses_for_qr[0] if len(poses_for_qr) == 1 else mean_pose(poses_for_qr)

        pose = rectify_portal_pose(pose)

        pos, quat = convert_pose_colmap_to_opengl(pose.translation, pose.rotation.quat)

        manifest_data["portals"].append({
            "shortId": short_id,
            "pose": {
                "position": {
                    "x": pos[0],
                    "y": pos[1],
                    "z": pos[2],
                },
                "rotation": {
                    "x": quat[0],
                    "y": quat[1],
                    "z": quat[2],
                    "w": quat[3],
                }
            },
            "averagePose": {
                "position": {
                    "x": pos[0],
                    "y": pos[1],
                    "z": pos[2],
                },
                "rotation": {
                    "x": quat[0],
                    "y": quat[1],
                    "z": quat[2],
                    "w": quat[3],
                }
            },
            "physicalSize": portal_sizes.get(short_id, None)
        })

    if portal_alignment_metrics is not None:
        manifest_data["portal_alignment_metrics"] = portal_alignment_metrics

    #-------------------------

    #-------------------------
    # ALIGNED SCANS (pose & optional scaling to bring local refinement scans into domain coords, as determined by global refinement)
    #-------------------------
    # TODO: should verify this first and save transforms in OpenGL space
    """
    if scan_alignment_transforms:
        manifest_data["scanAlignmentTransforms"] = {}
        for scan_id, sim3 in scan_alignment_transforms.items():
            pos = sim3.translation
            quat = sim3.rotation.quat
            #pos, quat = convert_pose_colmap_to_opengl(pos, quat)
            #colmap_to_gl = np.array([
            #    [0, 1, 0],
            #    [1, 0, 0],
            #    [0, 0, -1]
            #])
            #colmap_to_gl_inv = colmap_to_gl # Inverse is the same since it's symmetric
            #pos = colmap_to_gl @ pos
            #R = scipy_Rotation.from_quat(quat).as_matrix()
            #R = colmap_to_gl @ R colmap_to_gl_inv
            #quat = scipy_Rotation.from_matrix(R).as_quat()
            manifest_data["scanAlignmentTransforms"][scan_id] = {
                "localToDomain": {
                    "scale": str(float(sim3.scale)),
                    "position": {
                        "x": str(float(pos[0])),
                        "y": str(float(pos[1])),
                        "z": str(float(pos[2])),
                    },
                    "rotation": {
                        "x": str(float(quat[0])),
                        "y": str(float(quat[1])),
                        "z": str(float(quat[2])),
                        "w": str(float(quat[3])),
                    }
                }
            }
    """

    with open(json_path, 'w') as json_file:
        json.dump(manifest_data, json_file, indent=4)
