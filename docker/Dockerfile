##
## Rust build for server
##
FROM --platform=$BUILDPLATFORM rust:1.89-bullseye AS rust-build
WORKDIR /app
ADD server/rust /app
# Build release binary for the server-bin crate
RUN cargo build --release -p server-bin


FROM --platform=$BUILDPLATFORM nvidia/cuda:12.8.1-base-ubuntu22.04

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

RUN apt-get install -y libmkl-full-dev

RUN apt-get install -y --no-install-recommends \
    ssh \
    netcat-openbsd \
    ca-certificates \
    iproute2 \
    iputils-ping \
    bind9-dnsutils

RUN pip install --upgrade pip setuptools wheel

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

RUN pip install "pybind11[global]==3.0.1"

RUN pip install psutil gputil
    
WORKDIR /src

RUN git clone https://github.com/NVIDIA/libglvnd && \
    cd libglvnd && \
    ./autogen.sh && \
    ./configure && \
    make -j$(nproc) && \
    make install

# Workaround for runtime error when loading Intel MKL libraries
# See: https://bugs.launchpad.net/ubuntu/+source/intel-mkl/+bug/1947626
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libmkl_def.so:/usr/lib/x86_64-linux-gnu/libmkl_avx2.so:/usr/lib/x86_64-linux-gnu/libmkl_core.so:/usr/lib/x86_64-linux-gnu/libmkl_intel_lp64.so:/usr/lib/x86_64-linux-gnu/libmkl_intel_thread.so:/usr/lib/x86_64-linux-gnu/libiomp5.so

# Ceres
RUN git clone --recursive https://ceres-solver.googlesource.com/ceres-solver \
    && cd ceres-solver \
    && git checkout tags/2.2.0 \
    && mkdir build \
    && cd build \
    && cmake .. -GNinja -DBUILD_TESTING=OFF -DBUILD_EXAMPLES=OFF -DLAPACK=ON -DBLA_VENDOR=Intel10_64lp \
    && ninja -j$(nproc) \
    && ninja install

# pyCeres
RUN git clone https://github.com/cvg/pyceres.git \
    && cd pyceres \
    && git checkout tags/v2.5 \
    && python3 -m pip install .

# COLMAP
RUN git clone https://github.com/colmap/colmap.git && \
    cd colmap \
    && git checkout 20b2777186654f20745f6c57974924d145bbbd6f \
    && mkdir build \
    && cd build \
    && cmake .. -GNinja -DCUDA_ENABLED=ON -DBLA_VENDOR=Intel10_64lp -DCMAKE_CUDA_ARCHITECTURES="60;61;70;75;80;86;89;90;120" \
    && ninja -j$(nproc) \
    && ninja install \
    && cd .. \
    && python3 -m pip install .

# Hloc
RUN git clone --recursive https://github.com/aukilabs/Hierarchical-Localization && \
    cd Hierarchical-Localization && \
    git checkout --recurse-submodules a698de1a09604b31a42ec21c95419018430a5db4 && \
    python3 -m pip install .

# ONLY NEEDED FOR VISUALIZATION TOOLS (OPTIONAL)
# RUN python3 -m pip install --upgrade plotly

RUN python3 -m pip install enlighten

RUN python3 -m pip install open3d trimesh alphashape

RUN git clone https://github.com/google/draco.git && \
    cd draco && \
    mkdir build && \
    cd build && \
    cmake .. -GNinja && \
    ninja -j$(nproc)

WORKDIR /app

COPY scripts /app/scripts
COPY src /app/src
COPY config /app/config
RUN chmod 755 /app/src
COPY CMakeLists.txt /app/
RUN mkdir build && \
    cmake -B build -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON && \
    cmake --build build

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

COPY --from=rust-build /app/target/release/server-bin ./reconstruction
ENTRYPOINT ["./reconstruction"]

# Overridable using 'docker run ... image_name -api-key <your-api-key>'
# TODO: we probably don't need api key, or make it optional. Just check domain token.
CMD ["--api-key", "aukilabs123"]
