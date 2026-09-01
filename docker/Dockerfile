##
## Rust build for server
##
ARG RECONSTRUCTION_VERSION=0.0.0-local
FROM --platform=$BUILDPLATFORM rust:1.89-bullseye AS rust-build
ARG RECONSTRUCTION_VERSION
ENV RECONSTRUCTION_VERSION="${RECONSTRUCTION_VERSION}"
WORKDIR /app
COPY server/rust/ ./
# Build release binary for the compute-node crate
RUN cargo build --release -p bin

##
## Runtime
##
FROM ghcr.io/aukilabs/reconstruction-server-base:latest AS final

ARG RECONSTRUCTION_VERSION
ENV RECONSTRUCTION_SERVER_VERSION="${RECONSTRUCTION_VERSION}"

ARG USERNAME=reconstruction-server
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Application code
WORKDIR /app

# MrNeRF / LibTorch (app image only — see plans/mrnerf-hloc-adapters.md §4)
# Pin must stay in sync with third_party/Light_Glue_CPP git submodule commit.
ARG LIGHT_GLUE_CPP_COMMIT=bd825e8d76f88c024ec37b41c6178e2b873059de
ARG LIBTORCH_VERSION=2.7.1
ARG LIBTORCH_CUDA=cu128
# Latest cu128 Linux bundle with explicit cxx11 ABI (matches Ubuntu 22.04 / base GCC).
ARG LIBTORCH_URL=https://download.pytorch.org/libtorch/${LIBTORCH_CUDA}/libtorch-cxx11-abi-shared-with-deps-${LIBTORCH_VERSION}%2B${LIBTORCH_CUDA}.zip

# LibTorch 2.7.1 API patches for pristine Light_Glue_CPP @ bd825e8 (see third_party/patches/).
COPY third_party/patches /app/third_party/patches

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        git git-lfs wget unzip \
        libopencv-dev \
        cuda-nvcc-12-8 cuda-cudart-dev-12-8 libcublas-dev-12-8 \
        libcusparse-dev-12-8 libcusolver-dev-12-8 \
        libcurand-dev-12-8 cuda-nvrtc-dev-12-8 cuda-nvtx-12-8; \
    git lfs install; \
    wget -q "${LIBTORCH_URL}" -O /tmp/libtorch.zip; \
    unzip -q /tmp/libtorch.zip -d /opt; \
    rm /tmp/libtorch.zip; \
    mkdir -p /app/third_party; \
    git clone https://github.com/MrNeRF/Light_Glue_CPP.git /app/third_party/Light_Glue_CPP; \
    cd /app/third_party/Light_Glue_CPP; \
    git checkout "${LIGHT_GLUE_CPP_COMMIT}"; \
    git lfs pull; \
    git apply /app/third_party/patches/light_glue_cpp-libtorch-2.7.patch; \
    git apply /app/third_party/patches/light_glue_cpp-nms-radius.patch; \
    rm -rf /var/lib/apt/lists/*

ENV CUDA_HOME=/usr/local/cuda-12.8
ENV PATH=/usr/local/cuda-12.8/bin:${PATH}
ENV LIBTORCH_ROOT=/opt/libtorch
ENV LD_LIBRARY_PATH=/opt/libtorch/lib:${LD_LIBRARY_PATH}

COPY scripts /app/scripts
COPY src /app/src
COPY config /app/config
COPY utils /app/utils
COPY cmake /app/cmake
COPY update_main.py local_main.py global_main.py occlusion_box.py topology_main.py main.py CMakeLists.txt /app/

RUN chmod -R 755 /app/scripts /app/src /app/utils /app/*.py

RUN cmake -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DPYBIND11_FINDPYTHON=ON \
        -DLIBTORCH_ROOT=/opt/libtorch && \
    cmake --build build

# Run reconstruction server as separate user, not root
RUN set -eux; \
    groupadd --gid "$USER_GID" "$USERNAME"; \
    useradd --uid "$USER_UID" --gid "$USER_GID" -m "$USERNAME"; \
    mkdir -p /app/jobs; \
    chown -R "$USERNAME:$USERNAME" /app/jobs

USER $USERNAME

# Copy Rust binary from builder
COPY --from=rust-build /app/target/release/compute-node /app/compute-node

ENTRYPOINT ["/app/compute-node"]
