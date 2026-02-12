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
bash .devcontainer/build_server.sh
```

Python code changes don't require a rebuild, just stop the server (Ctrl+C) and run it again (`./compute-node`)