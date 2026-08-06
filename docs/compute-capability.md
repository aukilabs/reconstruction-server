# Compute Capability

Expected Input and Output Files from each compute capability

<details>
  <summary>/reconstruction/local-refinement/v1</summary>

  ### Input
  ```bash
  # Input Files
  {job_root_path}
  ├── datasets
  │   └── {dataset}
  │       ├── Frames.mp4
  │       ├── Accel.csv
  │       ├── ARposes.csv     
  │       ├── CameraIntrinsics.csv     
  │       ├── FeaturePoints.ply     
  │       ├── gyro_accel.csv     
  │       ├── Gyro.csv     
  │       ├── PortalDetections.csv
  │       └── Manifest.json
  ```

  ### Output
  ```bash
  # Output Files
  {job_root_path}
  ├── refined
  │   └── local
  │       └── {dataset}
  │           ├── colmap_rec
  │           │    ├── cameras.bin
  │           │    ├── frames.bin
  │           │    ├── images.bin
  │           │    ├── points3D.bin
  │           │    └── rigs.bin
  │           ├── sfm
  │           │    ├── cameras.bin    # Uploaded as zip
  │           │    ├── database.db
  │           │    ├── feature.h5
  │           │    ├── frames.bin     # Uploaded as zip
  │           │    ├── global_features.h5
  │           │    ├── images.bin     # Uploaded as zip
  │           │    ├── matches.bin 
  │           │    ├── pairs-sfm.txt
  │           │    ├── points3D.bin   # Uploaded as zip
  │           │    ├── portals.csv    # Portal poses relative to colmap world coordinates. Uploaded as zip
  │           │    └── rigs.bin       # Uploaded as zip
  │           └── local_logs
  ```

</details>


<details>
  <summary>/reconstruction/local-refinement-auki-sdk/v0</summary>

  Local refinement of an **Auki SDK capture session** instead of a DMT/ARKit scan.
  Runner: `server/rust/runner-reconstruction-local-auki-sdk`, pipeline entrypoint
  `local_auki_main.py` (`utils/auki_refinement_util.py::refine_auki_session`).

  ### Input

  Exactly **one** domain artifact: a zip of the whole capture folder, e.g.
  `scan_path_recording_2026-08-05_09-33-06.zip`. The runner expands it into the job
  workspace itself; the Python pipeline only ever sees the unpacked folder. The capture
  folder may sit at the root of the archive or inside a single wrapper directory —
  it is located by looking for the directory that owns a session subfolder containing
  `sensorlogs/`. A capture holding more than one session is rejected (one task refines
  one session).

  `{scan}` is derived from the artifact name exactly as for
  `/reconstruction/local-refinement/v1`: the capture timestamp in the name when there is
  one (`scan_path_recording_2026-08-05_09-33-06.zip` → `2026-08-05_09-33-06`), otherwise
  the sanitized name.

  ```bash
  # Input Files (after the runner unzips)
  {job_root_path}
  ├── datasets
  │   └── {scan}
  │       └── {capture folder}          # --dataset_path
  │           ├── registries
  │           │    ├── clocks
  │           │    ├── frames
  │           │    └── sensors
  │           ├── scan_report.json
  │           └── {session_id}
  │               ├── poselogs
  │               │    └── {from__to}/{log_manifest.json,segments/*.seg}
  │               └── sensorlogs
  │                    └── {sensor_id}/{log_manifest.json,segments/*.seg}
  ```

  ### Output

  Same layout, same uploads as `/reconstruction/local-refinement/v1` — the refined `sfm`
  folder is zipped and uploaded as `refined_scan_{scan}` (`refined_scan_zip`), plus the
  job log as `task_log_{task_id}` (`task_log_txt`). The output folder is named after
  `{scan}` (not the internal Auki session id) so the refined scan carries the identifier
  the input artifact was submitted under.

  ```bash
  # Output Files
  {job_root_path}
  ├── log.txt                           # Uploaded as task_log_{task_id}
  ├── refined
  │   └── local
  │       └── {scan}
  │           ├── colmap_rec            # registry-seeded initial reconstruction
  │           │    ├── cameras.bin
  │           │    ├── frames.bin
  │           │    ├── images.bin
  │           │    ├── points3D.bin
  │           │    └── rigs.bin
  │           ├── hloc                  # features.h5 / matches.h5
  │           ├── images                # frames extracted from the sensor logs
  │           ├── qr_detection_crops
  │           ├── sfm
  │           │    ├── cameras.bin              # Uploaded as zip
  │           │    ├── frames.bin               # Uploaded as zip
  │           │    ├── images.bin               # Uploaded as zip
  │           │    ├── points3D.bin             # Uploaded as zip
  │           │    ├── rigs.bin                 # Uploaded as zip
  │           │    ├── pairs-sfm.txt            # Uploaded as zip
  │           │    ├── portals.csv              # Portal poses in colmap world coordinates. Uploaded as zip
  │           │    ├── qr_anchor_poses.csv      # Per-marker poses re-origined at one QR, Auki convention. Uploaded as zip
  │           │    └── qr_detections.csv        # One row per individual QR detection. Uploaded as zip
  │           ├── database.db
  │           ├── point_cloud.ply
  │           ├── point_cloud_qr_anchored.ply
  │           └── local_logs
  ```

  The zip is a superset of local refinement's: `qr_anchor_poses.csv` and
  `qr_detections.csv` are Auki-only extras (the `.bin`/`.csv`/`.txt` filter is unchanged).

  ### Runtime settings

  | Env | Default | Meaning |
  | --- | --- | --- |
  | `LOCAL_AUKI_RUNNER_WORKSPACE_ROOT` | temp dir | Base directory for job workspaces |
  | `LOCAL_AUKI_RUNNER_PYTHON_BIN` | `python3` | Python executable |
  | `LOCAL_AUKI_RUNNER_PYTHON_SCRIPT` | `local_auki_main.py` | Pipeline entrypoint |
  | `LOCAL_AUKI_RUNNER_PYTHON_ARGS` | _(empty)_ | Extra args prepended to the pipeline invocation |
  | `LOCAL_AUKI_RUNNER_EVERY_NTH_IMAGE` | `1` | Subsample each sensor's frames |

  > **Deployment note:** the pipeline imports the Auki SDK Python bindings
  > (`auki_registry`, `auki_logs`, `auki_layout`, `auki_datatypes`, `auki_geometry`,
  > `qr_lab`, `auki_pnplab` — see `utils/auki_data_utils.py`). Those wheels are not
  > installed by `docker/Dockerfile.base` yet, so this capability only runs where they
  > have been installed (currently the dev container). The base image needs them before
  > the capability is advertised by a deployed node.

</details>


<details>
  <summary>/reconstruction/global-refinement/v1</summary>

  ### Input
  ```bash
  # Input Files
  {job_root_path}
  ├── refined
  │   └── local
  │       └── {dataset}
  │           └── reconstruction_refined_x1.zip # this is what is expected to downloaded from domain server
  ```

  ### Output
  ```bash
  # Output Files
  {job_root_path}
  ├── refined
  │   └── global
  │       ├── refined_sfm_combined
  │       │    ├── cameras.bin
  │       │    ├── frames.bin
  │       │    ├── images.bin
  │       │    ├── points3D.bin
  │       │    └── rigs.bin
  │       ├── topology
  │       │    ├── topology_downsampled_0.111.glb    # Uploaded as zip
  │       │    ├── topology_downsampled_0.111.obj
  │       │    ├── topology_downsampled_0.333.glb
  │       │    ├── topology_downsampled_0.333.obj    # Uploaded as zip
  │       │    ├── topology.glb
  │       │    └── topology.obj     # Uploaded as zip
  │       ├── refined_manifest.json
  │       ├── RefinedPointCloud.ply
  │       ├── RefinedPointCloud.ply.drc
  │       ├── RefinedPointCloudFloat.ply
  │       ├── RefinedPointCloudReduced.ply
  │       └── global_logs
  ```

</details>

<details>
  <summary>/reconstruction/update-refinement/v1</summary>

  ### Input
  ```bash
  # Input Files
  {job_root_path}
  ├── refined
  │   ├── local
  │   │   └── {dataset}
  │   │       └── reconstruction_refined_x1.zip # this is what is expected to downloaded from domain server
  │   └── global
  │       ├── refined_sfm_combined
  │       │    ├── cameras.bin
  │       │    ├── frames.bin
  │       │    ├── images.bin
  │       │    ├── points3D.bin
  │       │    └── rigs.bin
  │       └── refined_manifest.json
  ```

  ### Output
  ```bash
  # Output Files
  {job_root_path}
  ├── refined
  │   └── update
  │       ├── refined_sfm_combined
  │       │    ├── cameras.bin
  │       │    ├── frames.bin
  │       │    ├── images.bin
  │       │    ├── points3D.bin
  │       │    └── rigs.bin
  │       ├── topology
  │       │    ├── topology_downsampled_0.111.glb    # Uploaded as zip
  │       │    ├── topology_downsampled_0.111.obj
  │       │    ├── topology_downsampled_0.333.glb
  │       │    ├── topology_downsampled_0.333.obj    # Uploaded as zip
  │       │    ├── topology.glb
  │       │    └── topology.obj     # Uploaded as zip
  │       ├── refined_manifest.json
  │       ├── RefinedPointCloud.ply
  │       ├── RefinedPointCloud.ply.drc
  │       ├── RefinedPointCloudFloat.ply
  │       ├── RefinedPointCloudReduced.ply
  │       └── update_logs
  ```

</details>