##
## Rust build for server
##
FROM --platform=$BUILDPLATFORM rust:1.89-bullseye AS rust-build
WORKDIR /app
ADD server/rust /app
# Build release binary for the compute-node crate
RUN cargo build --release -p bin


FROM --platform=$BUILDPLATFORM nvidia/cuda:11.8.0-base-ubuntu22.04

ARG USERNAME=reconstruction-server
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    wget \
    curl \
    vim \
    git \
    nano \
    jq \
    less \
    cmake \
    autoconf \
    automake \
    libtool \
    libffi-dev \
    ninja-build \
    build-essential \
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
    python3-tk

RUN apt-get install -y --no-install-recommends \
    ssh \
    netcat-openbsd \
    ca-certificates \
    iproute2 \
    iputils-ping \
    bind9-dnsutils

RUN python3 -m pip install --upgrade pip setuptools wheel

RUN wget https://cmake.org/files/v3.27/cmake-3.27.9-linux-x86_64.tar.gz \
    && tar xvf cmake-3.27.9-linux-x86_64.tar.gz \
    && cd cmake-3.27.9-linux-x86_64 \
    && cp -r bin /usr/ \
    && cp -r share /usr/ \
    && cp -r doc /usr/share/ \
    && cp -r man /usr/share/ \
    && cd .. \
    && rm -rf cmake*

RUN git clone https://github.com/google/glog.git && cd glog && \
    git checkout tags/v0.6.0 && \
    cmake -S . -B build -G "Unix Makefiles" && \
    cmake --build build --target install

RUN python3 -m pip install "pybind11[global]==2.12.0"

RUN python3 -m pip install psutil gputil
    
WORKDIR /src

RUN git clone https://github.com/NVIDIA/libglvnd && \
    cd libglvnd && \
    ./autogen.sh && \
    ./configure && \
    make -j4 && \
    make install

# Ceres
RUN git clone --recursive https://ceres-solver.googlesource.com/ceres-solver \
    && cd ceres-solver \
    && git checkout tags/2.2.0 \
    && mkdir build \
    && cd build \
    && cmake .. -DBUILD_TESTING=OFF -DBUILD_EXAMPLES=OFF \
    && make -j4 \
    && make install

# pyCeres
RUN git clone https://github.com/cvg/pyceres.git \
    && cd pyceres \
    && git checkout tags/v2.3 \
    && python3 -m pip install -e .

# COLMAP
RUN git clone https://github.com/colmap/colmap.git && \
    cd colmap \
    && git checkout release/3.10 \
    && mkdir build \
    && cd build \
    && cmake -DCUDA_ENABLED=ON -DCMAKE_CUDA_ARCHITECTURES="60;61;70;75;80;86;89" .. \
    && make -j 4 \
    && make install \
    && cd ../pycolmap \
    && python3 -m pip install -e .

# Hloc
RUN git clone --recursive https://github.com/aukilabs/Hierarchical-Localization && \
    cd Hierarchical-Localization && \
    git checkout --recurse-submodules 6dc1714ba1b8df653ac973abf2779a718b7634c9 && \
    python3 -m pip install -e . --config-settings editable_mode=compat && \
    python3 -m pip install --upgrade plotly

# Hloc installs latest opencv-python as dependency,
# but the latest opencv 4.11.0.86 introduced a bug
# causing unpacked mp4 frames to be rotated wrong.
# For now we need to downgrade opencv (after hloc).
RUN python3 -m pip install opencv-python==4.10.0.84

RUN python3 -m pip install enlighten

RUN python3 -m pip install open3d trimesh alphashape

RUN git clone https://github.com/google/draco.git && \
    cd draco && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make

WORKDIR /app

COPY scripts /app/scripts
COPY src /app/src
COPY config /app/config
RUN chmod 755 /app/src
COPY CMakeLists.txt /app/
RUN mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON .. && make all

COPY utils /app/utils
RUN chmod 755 /app/utils
COPY local_main.py global_main.py occlusion_box.py topology_main.py main.py /app/
RUN chmod 755 /app/*.py

RUN echo "/usr/local/lib" > /etc/ld.so.conf.d/local.conf && ldconfig

# Run reconstruction server as separate user, not root
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

RUN mkdir -p /app/jobs && chown -R reconstruction-server:reconstruction-server /app/jobs

USER $USERNAME

COPY --from=rust-build /app/target/release/compute-node ./compute-node
ENTRYPOINT ["./compute-node"]
