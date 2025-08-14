# Deployment

The Reconstruction server is available on [Docker Hub](https://hub.docker.com/r/aukilabs/reconstruction-node). Both deployment options are Docker-based.

## Before running

1. First check that your NVIDIA driver and CUDA toolkit meet the requirements in the [Minimum Requirements](minimum-requirements.md) and update as needed:
```shell
nvidia-smi
```

2. Allow inbound TCP traffic to port 8080 (or a port of your choosing)

3. Configure a domain name to point to your static IP address

## Option 1 — Use the prebuilt image (recommended)

Start Docker and then run:
```shell
docker run --gpus all --shm-size 512m -p 8080:8080 -d aukilabs/reconstruction-node -cpu-workers 2 -port :8080 -api-key kaffekopp123
```


## Option 2 — Build from source

### Building Docker

> **NOTE:** On Mac with Apple Silicon, the --platform flag is needed. Although running the image with CUDA won't work on Mac, the image can still run on a cloud server for example, pulling from the docker hub.

```bash
# Linux computer or deploy to Linux server
docker buildx build --platform linux/amd64 -t docker.io/library/auki-archive:latest --load  .

# Jetson Device
DOCKER_BUILDKIT=1 docker buildx build --push --platform linux/arm64 -t {/your/docker/repo} -f Dockerfile.jetson .
```

## Running Refinement

**NOTE:** For the closed beta you will not have to worry about manually running refinements; Auki will send jobs to your node. 

The following instructions are provided for reference only.

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
--data_dir /path/to/jobs/my_domain_job/full_store_capture \
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
--data_dir /path/to/jobs/my_domain_job/full_store_capture \
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