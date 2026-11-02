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
  │       ├── ARposes.csv     # omitted when Manifest mode is robot_gps
  │       ├── GpsPositions.csv  # robot_gps only: timestamp_sec, enu_e, enu_n, enu_u
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