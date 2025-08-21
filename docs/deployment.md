# Deployment

The Reconstruction server is available on [Docker Hub](https://hub.docker.com/r/aukilabs/reconstruction-node). Both deployment options are Docker-based.

## Before running

1. First check that your NVIDIA driver and CUDA toolkit meet the requirements in the [Minimum Requirements](minimum-requirements.md) and update as needed:
```shell
nvidia-smi
```

2. Allow inbound TCP traffic to port 8080 (or a port of your choosing)

3. If you have a static IP, optionally configure a domain name for it. \
   Or, if your IP is not static, you need to set up Dynamic DNS pointing to your IP.

## Option 1 — Use the prebuilt image (recommended)

Start Docker and then run:
```shell
docker run --gpus all --shm-size 512m -p 8080:8080 -d aukilabs/reconstruction-node:latest -cpu-workers 2 -port :8080 -api-key aukilabs123
```

💡 **Note 1:** For the -api-key leave as is, or any non-sensitive phrase. During  the community beta, you will need to provide this key to Auki Labs. This key is just an extra gate for incoming jobs, not used to access any user data.

💡 **Note 2:** if your system has an older CPU or less RAM and you notice any issues, you may try to reduce the `-cpu-workers` to 1, or even 0 (to run only on the main thread).


## Option 2 — Build Docker image from source

### Building Docker

> **NOTE:** On Mac with Apple Silicon, the --platform flag is needed. Although running the image with CUDA won't work on Mac, the image can still run on a cloud server for example, pulling from the docker hub.

```bash
# Linux computer or deploy to Linux server
docker buildx build --platform linux/amd64 -t {/your/docker/repo}:latest --load -f docker/Dockerfile .

# Jetson Device
DOCKER_BUILDKIT=1 docker buildx build --push --platform linux/arm64 -t {/your/docker/repo}:latest -f Dockerfile.jetson .
```

Run the image as in Option 1.
