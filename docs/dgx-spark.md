# DGX Spark: build and run against Auki dev

Build on the Spark itself from the repository root. This path uses native ARM64,
CUDA 13, and GB10 (`SM 12.1`). The published reconstruction base image currently
contains only AMD64, and the Jetson Dockerfile targets different hardware.
See [NVIDIA's Spark compilation guidance](https://docs.nvidia.com/dgx/dgx-spark-porting-guide/porting/compilation.html).

The Spark Dockerfile preserves the existing COLMAP and HLoc source revisions.
It builds Ceres bindings against Ubuntu's ARM-compatible Ceres/OpenBLAS stack,
uses [PyTorch 2.9.1 with CUDA 13](https://pytorch.org/get-started/previous-versions/),
and uses [Open3D's ARM64 CPU wheel](https://www.open3d.org/docs/latest/arm.html).
CUDA acceleration comes from PyTorch and COLMAP; Open3D geometry operations run
on the CPU. This is a development build, with compiler tools retained in the image.

PyTorch 2.9.1 emits a capability-range warning for GB10 (12.1 versus its reported
maximum of 12.0). GPU matrix multiplication, convolution, and torchvision NMS
passed on the Spark with this build; the warning is not suppressed. Re-run the
smoke test when changing the driver or dependencies, and validate a real capture
before relying on the node for reconstruction work.

## Prerequisites

- Docker Engine, Docker Compose, and NVIDIA Container Toolkit on the Spark.
- `docker run --rm --gpus all nvidia/cuda:13.0.0-base-ubuntu24.04 nvidia-smi`
  succeeds.
- A root `.env` with the **dev environment's** `REG_SECRET` and
  `SECP256K1_PRIVHEX`, prepared as described in [Deployment](deployment.md).
  The file is excluded from the build context and passed only at runtime.
- Enough free shared system/GPU memory. Stop competing GPU workloads before
  building or processing captures; four parallel build jobs are the default.

## Build the checked-out branch

```sh
ssh spark
cd ~/Developer/reconstruction-server
git status --short --branch

revision=$(git rev-parse HEAD)
short_revision=$(git rev-parse --short=7 HEAD)
image="reconstruction-node:uuid-${short_revision}-spark"
docker buildx build --platform linux/arm64 --load \
  --build-arg BUILD_JOBS=4 \
  --build-arg RECONSTRUCTION_VERSION="0.0.0-spark.${short_revision}" \
  --build-arg SOURCE_REVISION="$revision" \
  -t "$image" -f docker/Dockerfile.spark .
```

The first build downloads CUDA/PyTorch and compiles COLMAP, Ceres bindings,
and the Rust host. Later source changes reuse those dependency layers.
The image stays in the Spark's Docker engine; no registry push is necessary.

Before starting a node, exercise GPU operations, native Ceres bindings, point
cloud encoding, and writable directories, then run the Python regression tests.
Neither command needs credentials:

```sh
docker run --rm --gpus all --shm-size 1g \
  -e OMP_NUM_THREADS=2 -e OPENBLAS_NUM_THREADS=2 \
  --entrypoint python3 "$image" -m scripts.smoke_test_spark

docker run --rm --gpus all --shm-size 1g \
  -e OMP_NUM_THREADS=2 -e OPENBLAS_NUM_THREADS=2 \
  --entrypoint python3 "$image" \
  -m unittest discover -s tests -p 'test_*.py' -v
```

## Start and register in dev

```sh
export RECONSTRUCTION_SPARK_IMAGE="$image"
docker compose -f docker/compose.spark.dev.yml config --quiet
docker compose -f docker/compose.spark.dev.yml up -d --no-build
docker compose -f docker/compose.spark.dev.yml ps
docker compose -f docker/compose.spark.dev.yml logs --tail 50
```

Compose explicitly selects `https://dds.dev.aukiverse.com` and
`https://dms.dev.aukiverse.com/v1`, overriding the production defaults. It mounts
named volumes for jobs and model downloads, requests the GPU, and supplies 1 GiB
of shared memory. The node registers itself at startup and processes one task
at a time. This branch advertises both v1 and v2 local/global/update refinement
capabilities, accepting legacy timestamp captures and UUID captures.

Look for `Registration ok` with a successful HTTP status. `compute node starting`
or `Registering node with DDS (SIWE)` alone does not prove registration succeeded.
The registration-specific debug filter logs the result without enabling verbose
HTTP logging. Check for restart loops or authentication/claim failures before
submitting a capture from DMT configured for dev. A real capture is still needed
to validate the entire download/reconstruction/upload path.

The container has a restart policy. Keep the same image environment variable
when recreating it; after a new SSH session, set it to the desired local tag.

## Stop or return memory to vLLM

```sh
# Allow an active reconstruction to finish before returning GPU memory to vLLM.
docker stop --timeout 600 reconstruction-spark-dev
docker start vllm-qwen38
```

SIGTERM stops new claims and lets an active task finish. Docker force-kills the
container if the timeout expires, so check whether a task is running first and
increase the timeout if needed. The stopped node can be resumed with
`docker start reconstruction-spark-dev`; stop the competing vLLM workload first.
Keep the job and model-cache volumes when recreating the container.

## Verified on 2026-10-13

- Host: `spark`, NVIDIA GB10, driver `580.173.02`, native `aarch64`.
- Branch: `feature/fix-capture-ids`, application commit `0891c804df74576eb3be19cfd71b7d6da1ee01b6`.
- Local image: `reconstruction-node:uuid-0891c80-spark`;
  image ID `sha256:a5c7cd57be1d344c35fc6ca21a55d4442ee949d57da261d926dca89db23fed72`.
- All 38 Python regression tests passed inside the image.
- Smoke checks passed using the final image and Compose volumes: GPU matmul,
  convolution, torchvision NMS, COLMAP CUDA SIFT, custom Ceres solve, Ceres/COLMAP
  interoperability, PLY export/merge bindings, Open3D I/O, Draco encoding, and
  writes as the non-root runtime user.
- Container `reconstruction-spark-dev` received DDS registration HTTP 200 at
  `2026-10-13T17:49:00Z`, advertising all six v1/v2 refinement capabilities.
- The pre-existing `vllm-qwen38` container was stopped to free shared memory.

### First job and dependency correction

The first local-refinement v2 task failed on all three attempts at about 20:23
CEST. Python exited while importing `main.py` with
`ModuleNotFoundError: No module named 'sklearn'`. The original checks imported
individual refinement modules but missed the occlusion module imported by the
actual task entrypoint.

The Spark Dockerfile now explicitly installs `scikit-learn`, and both build-time
and runtime checks run `main.py --help` and `update_main.py --help` to exercise
the entrypoints used by the Rust runners. The corrected image passed those checks,
all 38 regression tests, and the GPU/native smoke checks with Compose volumes.

Current image: `reconstruction-node:uuid-0891c80-spark-r2`,
ID `sha256:b8022eccb7e214525f87aac1a7f3e6547948ae094aba54b0029c0db05c979b9d`.
It replaced the idle container and registered with DDS (HTTP 200) at
20:26:39 CEST on 2026-10-13. To recreate this instance, set
`RECONSTRUCTION_SPARK_IMAGE=reconstruction-node:uuid-0891c80-spark-r2`.

`pip check` separately reports that NVIDIA's `nvidia-cusparselt-cu13` 0.8.0 wheel
is unsupported on this platform. Its embedded wheel tag is
`py3-none-manylinux2014_sbsa`; inspection confirmed the shared library is AArch64
(ELF machine 183), loads successfully, and is detected by PyTorch as cuSPARSELt
800. The warning remains; third-party wheel metadata was not modified.

The failed DMT job needs to be resubmitted. End-to-end reconstruction has not yet
passed; startup and synthetic checks alone do not establish that result.
