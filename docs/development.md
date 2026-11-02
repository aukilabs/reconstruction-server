# Development Guide

This document covers local development workflows for contributors.

## Use the dev container (VS Code or Cursor)

The repository includes a ready-to-use dev container under `.devcontainer/`. It mounts your local repository folder at `/app`, enables GPU support, and sets `--shm-size 512m`.

### Prerequisites

- Windows or Linux with NVIDIA GPU and the minimum requirements listed in [Minimum Requirements](minimum-requirements.md).
- Docker Engine / Docker Desktop is installed and running. On Windows, configure Docker to use WSL 2.
- NVIDIA Container Toolkit is installed on Linux hosts so Docker can use `--gpus all`.
- VS Code or Cursor with Dev Containers support enabled.

### Open in a dev container

1. Open this repository in VS Code or Cursor.
2. Open the Command Palette (Ctrl+Shift+P)
3. Run **Dev Containers: Reopen in Container**.
4. Wait for the initial build to complete.

On first start, the post-create script automatically:
- builds the C++ components with CMake,
- builds the Rust `compute-node` binary,
- copies the runnable binary to `/app/compute-node`.

Create your `.env` file as described in [Deployment](deployment.md). Apply the environment inside the container:

```shell
source .devcontainer/apply_env_file.sh
```

Run the server:
```shell
./compute-node
```

### Rebuild after dependency or toolchain changes

If you update Docker/devcontainer settings or system dependencies, rebuild the environment:
- Command Palette -> **Dev Containers: Rebuild Container**

You can also manually rebuild the server binary inside the container, which is often enough after changing Rust code.
```shell
bash .devcontainer/build_server.sh 0.0.0
```

Python code changes don't require a rebuild, just stop the server (Ctrl+C) and run it again (`./compute-node`)

## Optional mono-depth TSDF mesh

Off by default. Enable with CLI `--mono_depth_mesh` on `local_main.py` / `global_main.py` / `main.py`, or set env `MONO_DEPTH_MESH=1` (truthy: `1`, `true`, `yes`, `on`). Local mesh backend: `MONO_DEPTH_MESH_METHOD=da3|delaunay` (default `da3`; case-insensitive).

- **Local refine:** after SfM, runs colmap-monodepth E1 (DA3 depth → fit → carve **fitted** depths → TSDF) into `refined/local/<scan>/mesh/`: colored **undecimated** **`tsdf_mesh.ply`** (`postprocess=False`; primary domain upload), plus optional **`tsdf_points.ply`** and **`meta.json`**. CLI sampling knobs: `--mono_depth_mesh_stride` (default `3`), `--mono_depth_mesh_process_res` (default `504`). Grocery BEST knobs (also monodepth library defaults): carve milder (`subtractive_value=-0.05`, `min_points=2`, `max_depth=5`, `unproject_stride=2`, `mask_stride=1`, plus `voxel_size=0.07`, `min_confidence=2`, `mask_max_depth=5`); local TSDF `voxel_length=0.04`, `sdf_trunc=0.40`, `color_type=rgb8`, `postprocess=False`. Confident temporal mask is still written under `fit/confident/` but is **not** used for carve (it punched holes vs fitted).
- **Global refine:** fuses per-scan carved depths in the domain frame with **no-color** TSDF (depth + pose + intrinsics only; Frames/RGB not required) using the same voxel/trunc as local (`voxel_length=0.04`, `sdf_trunc=0.40`) with `color_type=nocolor` and **`postprocess=True`** (decimated; no Taubin; 0.1 mm / ratio 0.2) into `refined/global/mesh/` (`tsdf_mesh.ply`, optional `tsdf_mesh_raw.ply`, points/meta). When fuse succeeds, the cleaned mesh is also exported as primary **`refined/global/topology/topology.{obj,glb}`** plus `topology_downsampled_*` LODs (same helpers as legacy topology). Sparse alpha-shape from the refined point cloud is written separately as **`topology_alphashape.{obj,glb}`** (both families are uploaded when present).
- **Soft-fail:** mesh steps log warnings and refine continues if monodepth is missing, errors, or produces no files. The global Rust runner skips upload for absent optional mesh artifacts (same as topology).
- **Local zip:** `RefinedScan.zip` includes `sfm/` plus mono-depth intermediates (`infer/`, `fit/`, `carve/`, `mesh/`) so global can fuse without extra DMT inputs. Separate mesh PLY domain artifacts remain optional.
- **Dependency pin:** `docker/colmap-monodepth-pin.txt` — `aukilabs/colmap-monodepth@08e58157cb60afa87dc1f5c5bffdea9f548b2749` (`[da3,mesh,fit]`).