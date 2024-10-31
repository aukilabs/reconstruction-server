# Reconstruction Node
This repository has the code and algorithms for the reconstruction node, part of Auki Network. The reconstruction node is used when setting up large posemesh domains.

The node processes scans from the Domain Management Tool (DMT) app. A custom structure-from-motion (SfM) pipeline improves accuracy and creates 3D representations such as point clouds, bounding boxes, and gaussian splats.

## Build

### Building Docker

> **NOTE:** On Mac with Apple Silicon, the --platform flag is needed. Although running the image with CUDA won't work on Mac, the image can still run on a cloud server for example, pulling from the docker hub.

```bash
# Linux computer or deploy to Linux server
docker buildx build --platform linux/amd64 -t docker.io/library/auki-archive:latest --load  .

# Jetson Device
DOCKER_BUILDKIT=1 docker buildx build --push --platform linux/arm64 -t {/your/docker/repo} -f Dockerfile.jetson .
```

## Running Refinement

### Local refinement for single scan
```
docker run \
--gpus all \
--shm-size=512m \
-v /path/to/jobs/:/path/to/jobs/ \
--entrypoint /usr/bin/python3 \
-it auki-archive:latest \
local_main.py \
--dataset_path /path/to/jobs/my_domain_job/datasets/dmt_scan_2024-06-26_10-29-57 \
--output_path /path/to/jobs/my_domain_job/refined/local \
--every_nth_image 2 \
--remove_outputs
```

### Local refinement for all scans within a folder
```
bash ./scripts/local_run_all.sh
```

### Global refinement pipeline using refined outputs

```
docker run \
--gpus all \
-v /path/to/jobs/:/path/to/jobs/ \
--entrypoint /usr/bin/python3 \
-it auki-archive:latest \
global_main.py \
--data_dir ./datasets \
--all_poses \
--all_observations \
--use_refined_outputs
```

### Global refinement pipeline, basic pointcloud stitch only

```
docker run \
--gpus all \
-v /path/to/jobs/:/path/to/jobs/ \
--entrypoint /usr/bin/python3 \
-it auki-archive:latest \
global_main.py \
--data_dir ./datasets \
--all_poses \
--all_observations \
--use_refined_outputs \
--add_3dpoints \
--basic_stitch_only
```

## Occlusion box generation
Modify [default.yaml](/config/occlusion_box/default.yaml) to change source file and output path.
```
docker run \
occlusion_box.py \
--config ./config/occlusion_box/default.yaml
```