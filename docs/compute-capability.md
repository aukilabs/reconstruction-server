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
  <summary>/reconstruction/local-refinement-auki-session/v1 and /reconstruction/local-refinement-auki-session-p2p/v1</summary>

  Local refinement of an **Auki SDK capture session**: a multi-sensor robot capture (registry plus sensor and pose logs) instead of a DMT scan. Runner: `server/rust/runner-reconstruction-local-auki-session`; pipeline: `python3 -m reconstruction local-auki-session`.

  The two capabilities differ only in where the capture zip comes from. Both take exactly **one** input:

  | Capability | Input | How the zip arrives |
  | --- | --- | --- |
  | `/reconstruction/local-refinement-auki-session/v1` | The zip of the capture folder, stored in the Domain (e.g. `scan_path_recording_2026-08-05_09-33-06.zip`) | Downloaded from the Domain |
  | `/reconstruction/local-refinement-auki-session-p2p/v1` | A `scan_path_recording_reference_json` manifest in the Domain (`auki.blob.manifest/1`: the robot's peer id, routes, and the zip's blobs) | Fetched from the robot's peer over Blob v1 |

  For the P2P capability:
  - Only nodes started with a P2P identity (`AUKI_PEER_IDENTITY_FILE`) advertise it, and the lease must carry a P2P grant.
  - Blob v1 caps one blob at 64 MiB, so a larger capture must be published as several blobs. The runner fetches them in order and concatenates them.
  - The manifest's `available_until` is advisory: a passed deadline is logged, and the fetch is attempted anyway.

  The runner expands the zip into the job workspace, so Python only sees the unpacked folder. The capture folder may be at the root of the archive or in a wrapper directory, up to three levels down. It is found as the directory that owns a session folder containing `sensorlogs/`. A capture holding more than one session is rejected: one task refines one session.

  `{scan}` is derived from the input artifact's name as for `/reconstruction/local-refinement/v1`: the capture timestamp when the name has one (`scan_path_recording_2026-08-05_09-33-06.zip` → `2026-08-05_09-33-06`), otherwise the sanitized name.

  ### Input
  ```bash
  # Input Files (after the runner expands the zip)
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

  The same uploads as `/reconstruction/local-refinement/v1`:
  - the refined `sfm` folder, zipped, as `refined_scan_{scan}` (`refined_scan_zip`). It holds the `.bin`, `.csv` and `.txt` files, which here also include `qr_anchor_poses.csv` and `qr_detections.csv`;
  - the job log as `task_log_{task_id}` (`task_log_txt`).

  `portals.csv` uses the same marker convention as a DMT scan's, so global and update refinement can take these scans alongside DMT ones (see [coordinate conventions](coordinate-conventions.md)).

  ```bash
  # Output Files
  {job_root_path}
  ├── log.txt                           # Uploaded as task_log_{task_id}
  ├── refined
  │   └── local
  │       └── {scan}
  │           ├── colmap_rec            # initial reconstruction from the pose logs
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
  │           │    ├── portals.csv              # Portal poses in COLMAP world coordinates. Uploaded as zip
  │           │    ├── qr_anchor_poses.csv      # Every marker's pose re-origined at one marker, Auki anchor convention. Uploaded as zip
  │           │    └── qr_detections.csv        # One row per QR detection, with the camera that saw it. Uploaded as zip
  │           ├── database.db
  │           ├── point_cloud.ply
  │           ├── point_cloud_qr_anchored.ply
  │           └── local_logs
  ```

  ### Runtime settings

  | Env | Default | Meaning |
  | --- | --- | --- |
  | `LOCAL_AUKI_SESSION_RUNNER_WORKSPACE_ROOT` | temp dir | Base directory for job workspaces |
  | `LOCAL_AUKI_SESSION_RUNNER_PYTHON_BIN` | `python3` | Python executable |
  | `LOCAL_AUKI_SESSION_RUNNER_PYTHON_MODULE` | `reconstruction` | Package run with `-m` |
  | `LOCAL_AUKI_SESSION_RUNNER_PYTHON_SCRIPT` | _(unset)_ | Run this script instead of the module |
  | `LOCAL_AUKI_SESSION_RUNNER_PYTHON_ARGS` | _(empty)_ | Extra pipeline arguments, e.g. `--qr_size 0.15`, `--no-refine_intrinsics`, `--external_intrinsics <json>` |
  | `LOCAL_AUKI_SESSION_RUNNER_EVERY_NTH_IMAGE` | `1` | Keep every nth frame of each sensor |
  | `AUKI_PEER_IDENTITY_FILE` | _(unset)_ | The node's P2P identity (created if missing). Set it to serve the P2P capability |

  The capture must be recorded with an Auki SDK that the pinned SDK can read. The Python bindings are built at the `auki-sdk` revision `server/rust/Cargo.toml` pins, and that revision requires `image_encoding` in sensor registry entries, which captures from before August 2026 lack.

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