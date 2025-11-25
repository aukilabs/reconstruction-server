##
## Rust build for server
##
FROM --platform=$BUILDPLATFORM rust:1.89-bullseye AS rust-build
WORKDIR /app
COPY server/rust/ ./
# Build release binary for the compute-node crate
RUN cargo build --release -p bin

##
## Runtime + native deps
##
FROM --platform=$BUILDPLATFORM nvidia/cuda:12.8.1-base-ubuntu22.04

ARG USERNAME=reconstruction-server
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG DEBIAN_FRONTEND=noninteractive

ENV PIP_NO_CACHE_DIR=1

# Core system + build deps + runtime tools in ONE layer
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      wget \
      curl \
      vim \
      nano \
      less \
      jq \
      git \
      cmake \
      autoconf \
      automake \
      libtool \
      ninja-build \
      build-essential \
      libffi-dev \
      libboost-program-options-dev \
      libboost-filesystem-dev \
      libboost-graph-dev \
      libboost-system-dev \
      libeigen3-dev \
      libflann-dev \
      libfreeimage-dev \
      libmetis-dev \
      libgtest-dev \
      libsqlite3-dev \
      libglew-dev \
      qtbase5-dev \
      libqt5opengl5-dev \
      libcgal-dev \
      libsuitesparse-dev \
      python3-pip \
      python3-tk \
      python3-dev \
      libmkl-full-dev \
      ssh \
      netcat-openbsd \
      ca-certificates \
      iproute2 \
      iputils-ping \
      bind9-dnsutils \
      libblas-dev \
      libsuitesparse-dev \
      libiomp-dev \
    ; \
    rm -rf /var/lib/apt/lists/*

# Python toolchain + base Python deps
RUN set -eux; \
    python3 -m pip install --upgrade pip setuptools wheel; \
    python3 -m pip install "pybind11[global]==3.0.1" \
                           psutil gputil \
                           enlighten \
                           open3d \
                           trimesh \
                           alphashape


WORKDIR /tmp/build
RUN set -eux; \
    wget -q https://cmake.org/files/v3.27/cmake-3.27.9-linux-x86_64.tar.gz; \
    tar xzf cmake-3.27.9-linux-x86_64.tar.gz; \
    cd cmake-3.27.9-linux-x86_64; \
    cp -r bin /usr/; \
    cp -r share /usr/; \
    cp -r doc /usr/share/; \
    cp -r man /usr/share/; \
    cd /; \
    rm -rf /tmp/build/*

# glog
RUN set -eux; \
    cd /tmp/build; \
    git clone --depth 1 --branch v0.6.0 https://github.com/google/glog.git; \
    cd glog; \
    cmake -S . -B build -G "Unix Makefiles"; \
    cmake --build build --target install; \
    cd /; \
    rm -rf /tmp/build/*

# libglvnd
RUN set -eux; \
    cd /tmp/build; \
    git clone --depth 1 https://github.com/NVIDIA/libglvnd.git; \
    cd libglvnd; \
    ./autogen.sh; \
    ./configure; \
    make -j"$(nproc)"; \
    make install; \
    cd /; \
    rm -rf /tmp/build/*

# Workaround for runtime error when loading Intel MKL libraries
# See: https://bugs.launchpad.net/ubuntu/+source/intel-mkl/+bug/1947626
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libmkl_def.so:/usr/lib/x86_64-linux-gnu/libmkl_avx2.so:/usr/lib/x86_64-linux-gnu/libmkl_core.so:/usr/lib/x86_64-linux-gnu/libmkl_intel_lp64.so:/usr/lib/x86_64-linux-gnu/libmkl_intel_thread.so:/usr/lib/x86_64-linux-gnu/libiomp5.so

# Ceres
RUN set -eux; \
    cd /tmp/build; \
    git clone --recursive https://ceres-solver.googlesource.com/ceres-solver; \
    cd ceres-solver; \
    git checkout tags/2.2.0; \
    mkdir build; \
    cd build; \
    cmake .. -GNinja \
      -DBUILD_TESTING=OFF \
      -DBUILD_EXAMPLES=OFF \
      -DLAPACK=ON \
      -DBLA_VENDOR=Intel10_64lp; \
    ninja -j"$(nproc)"; \
    ninja install; \
    cd /; \
    rm -rf /tmp/build/*

# pyCeres
RUN set -eux; \
    cd /tmp/build; \
    git clone --depth 1 --branch v2.5 https://github.com/cvg/pyceres.git; \
    cd pyceres; \
    python3 -m pip install .; \
    cd /; \
    rm -rf /tmp/build/*

# COLMAP (+ its Python bindings)
RUN set -eux; \
    cd /tmp/build; \
    git clone https://github.com/colmap/colmap.git; \
    cd colmap; \
    git checkout 20b2777186654f20745f6c57974924d145bbbd6f; \
    mkdir build; \
    cd build; \
    cmake .. -GNinja \
      -DCUDA_ENABLED=ON \
      -DBLA_VENDOR=Intel10_64lp \
      -DCMAKE_CUDA_ARCHITECTURES="60;61;70;75;80;86;89;90;120"; \
    ninja -j"$(nproc)"; \
    ninja install; \
    cd ..; \
    python3 -m pip install .; \
    cd /; \
    rm -rf /tmp/build/*

# Hloc
RUN set -eux; \
    cd /tmp/build; \
    git clone --recursive https://github.com/aukilabs/Hierarchical-Localization.git; \
    cd Hierarchical-Localization; \
    git checkout --recurse-submodules a698de1a09604b31a42ec21c95419018430a5db4; \
    python3 -m pip install .; \
    cd /; \
    rm -rf /tmp/build/*

# Draco
RUN set -eux; \
    cd /tmp/build; \
    git clone --depth 1 https://github.com/google/draco.git; \
    cd draco; \
    mkdir build; \
    cd build; \
    cmake .. -GNinja; \
    ninja -j"$(nproc)"; \
    cd /; \
    rm -rf /tmp/build/*

# Ensure /usr/local/lib is searched
RUN echo "/usr/local/lib" > /etc/ld.so.conf.d/local.conf && ldconfig

# Application code
WORKDIR /app

COPY scripts /app/scripts
COPY src /app/src
COPY config /app/config
COPY utils /app/utils
COPY local_main.py global_main.py occlusion_box.py topology_main.py main.py /app/

RUN chmod -R 755 /app/scripts /app/src /app/utils /app/*.py

# Run reconstruction server as separate user, not root
RUN set -eux; \
    groupadd --gid "$USER_GID" "$USERNAME"; \
    useradd --uid "$USER_UID" --gid "$USER_GID" -m "$USERNAME"; \
    mkdir -p /app/jobs; \
    chown -R "$USERNAME:$USERNAME" /app/jobs

USER $USERNAME

# Copy Rust binary from builder
COPY --from=rust-build /app/target/release/compute-node /app/compute-node

# Final cleanup of build caches
RUN set -eux; \
    rm -rf /tmp/* /root/.cache || true

ENTRYPOINT ["/app/compute-node"]

