"""What the Rust runners upload from the job folder after Python exits.

The upload tables live in Rust (runner-reconstruction-local/src/refined.rs,
runner-reconstruction-common/src/output.rs and strategy.rs, and the task log in each
runner's lib.rs). This is the Python-side copy: tests/test_upload_contract.py checks it against
the Rust sources, so a change on either side fails there first.
"""
from collections import namedtuple

# path is relative to the job root. {scan} is a scan folder name, {task_id} the DMS task id.
# Global and update append "_<suffix>" to the name when uploading.
UploadSpec = namedtuple("UploadSpec", ["runner", "path", "name", "data_type", "required"])

# The local runner zips refined/local/<scan>/sfm/*.{bin,csv,txt} into RefinedScan.zip,
# but only when all of these exist. Scans missing any of them are skipped, not failed.
REFINED_SCAN_REQUIRED_FILES = ("images.bin", "cameras.bin", "points3D.bin", "portals.csv")
REFINED_SCAN_ZIP_EXTENSIONS = (".bin", ".csv", ".txt")


def _final_outputs(runner):
    prefix = f"refined/{runner}"
    return [
        UploadSpec(runner, f"{prefix}/refined_manifest.json", "refined_manifest", "refined_manifest_json", True),
        UploadSpec(runner, f"{prefix}/RefinedPointCloudReduced.ply", "refined_pointcloud", "refined_pointcloud_ply", True),
        UploadSpec(runner, f"{prefix}/RefinedPointCloud.ply.drc", "refined_pointcloud_full_draco", "refined_pointcloud_ply_draco", False),
        UploadSpec(runner, f"{prefix}/topology/topology_downsampled_0.111.obj", "topologymesh_v1_lowpoly_obj", "obj", False),
        UploadSpec(runner, f"{prefix}/topology/topology_downsampled_0.111.glb", "topologymesh_v1_lowpoly_glb", "glb", False),
        UploadSpec(runner, f"{prefix}/topology/topology_downsampled_0.333.obj", "topologymesh_v1_midpoly_obj", "obj", False),
        UploadSpec(runner, f"{prefix}/topology/topology_downsampled_0.333.glb", "topologymesh_v1_midpoly_glb", "glb", False),
        UploadSpec(runner, f"{prefix}/topology/topology.obj", "topologymesh_v1_highpoly_obj", "obj", False),
        UploadSpec(runner, f"{prefix}/topology/topology.glb", "topologymesh_v1_highpoly_glb", "glb", False),
        UploadSpec(runner, f"{prefix}/refined_sfm_combined/cameras.bin", "colmap_cameras_bin", "colmap_cameras_bin", True),
        UploadSpec(runner, f"{prefix}/refined_sfm_combined/frames.bin", "colmap_frames_bin", "colmap_frames_bin", True),
        UploadSpec(runner, f"{prefix}/refined_sfm_combined/images.bin", "colmap_images_bin", "colmap_images_bin", True),
        UploadSpec(runner, f"{prefix}/refined_sfm_combined/points3D.bin", "colmap_points3d_bin", "colmap_points3d_bin", True),
        UploadSpec(runner, f"{prefix}/refined_sfm_combined/rigs.bin", "colmap_rigs_bin", "colmap_rigs_bin", True),
        # Uploaded when present. Nothing in the pipeline writes these today.
        UploadSpec(runner, "outputs_index.json", "outputs_index", "json", False),
        UploadSpec(runner, "result.json", "result", "json", False),
        UploadSpec(runner, "scan_data_summary.json", "scan_data_summary", "json", False),
    ]


UPLOAD_TABLE = (
    [UploadSpec("local", "refined/local/{scan}/RefinedScan.zip", "refined_scan_{scan}", "refined_scan_zip", False)]
    + _final_outputs("global")
    + _final_outputs("update")
    + [
        UploadSpec(runner, "log.txt", "task_log_{task_id}", "task_log_txt", False)
        for runner in ("local", "global", "update")
    ]
)
