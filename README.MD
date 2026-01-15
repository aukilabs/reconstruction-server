# Reconstruction Node (Rust compute node)

This repository contains the Rust compute node and reconstruction pipeline used on the Auki Network.
The compute node polls DMS for work and runs the Python reconstruction pipeline to generate outputs
(e.g. point clouds, bounding boxes, gaussian splats) from Domain Management Tool (DMT) scans.

## Quickstart (build and run with Docker)

### Prerequisites
- NVIDIA GPU with a driver that supports CUDA 12.x (see `docs/minimum-requirements.md`).
- Docker installed and the NVIDIA Container Toolkit configured (so `docker run --gpus all` works).
- 20+ GB free disk space.

### 1) Clone the repo
```sh
git clone <this-repo>
cd reconstruction-server
```

### 2) Build the Docker image
```sh
docker build -f docker/Dockerfile -t aukilabs/reconstruction-node:local .
```

Note: `docker/Dockerfile` depends on the base image
`ghcr.io/aukilabs/reconstruction-server-base:latest`. If you cannot pull,
build it locally once:
```sh
docker build -f docker/Dockerfile.base -t ghcr.io/aukilabs/reconstruction-server-base:latest docker
```
Then re-run the main build command above.

### 3) Get credentials from the Auki console
Go to `http://console.auki.network/` and:
- Register a node to obtain the DDS registration secret (`REG_SECRET`).
- Create or use an EVM wallet, stake AUKI for the node, and export its private key
  (`SECP256K1_PRIVHEX`).

### 4) Create an env file
Create a `.env` file in the repo root with required credentials:
```sh
NODE_URL=https://your-public-url
REG_SECRET=your-registration-secret
SECP256K1_PRIVHEX=your-evm-private-key
```
Optional overrides (defaults shown):
```sh
DMS_BASE_URL=https://dms.auki.network/v1
DDS_BASE_URL=https://dds.auki.network
REQUEST_TIMEOUT_SECS=60
REGISTER_INTERVAL_SECS=120
REGISTER_MAX_RETRY=-1
LOG_FORMAT=text
```

Notes:
- `NODE_URL` must be reachable by the DDS/DMS services (public IP or a public URL).
- Include the scheme and port in `NODE_URL` (for example `https://example.com:8080`).
- For production, ensure inbound TCP traffic to port 8080 is allowed and forwarded to this host.
- For local development you can use ngrok (see below).

### 5) Run the container
```sh
docker run \
  --gpus all \
  --shm-size 512m \
  -p 8080:8080 \
  --env-file .env \
  --name reconstruction-node \
  aukilabs/reconstruction-node:local
```

### 6) Verify it is working
- Check the health endpoint:
  ```sh
  curl http://localhost:8080/health
  ```
- Follow logs to confirm registration and DMS polling:
  ```sh
  docker logs -f reconstruction-node
  ```
  You should see successful registration and repeated polling for tasks. If there are
  no jobs yet, it will keep polling.

## Local development with ngrok (optional)
If you do not have a public IP, you can expose port 8080 with ngrok:
```sh
ngrok http 8080
```
Set `NODE_URL` in your `.env` to the HTTPS ngrok URL, then start the container.

## Advanced configuration (optional)
The reconstruction runners can be tuned via environment variables (workspace path,
Python script path, CPU workers). These are optional and intended for advanced use.
See:
- `server/rust/runner-reconstruction-local/src/lib.rs`
- `server/rust/runner-reconstruction-global/src/lib.rs`

## Documentation
- Minimum requirements: `docs/minimum-requirements.md`
