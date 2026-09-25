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

COPY scripts /app/scripts
COPY src /app/src
COPY config /app/config
COPY utils /app/utils
COPY update_main.py local_main.py global_main.py occlusion_box.py topology_main.py main.py CMakeLists.txt /app/

RUN chmod -R 755 /app/scripts /app/src /app/utils /app/*.py

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON && \
    cmake --build build

# Local/dev: install monodepth from vendored tree (private GH not available in build).
# Keep in sync with docker/colmap-monodepth-pin.txt (SHA 9d54bc8…).
COPY third_party/colmap-monodepth /tmp/colmap-monodepth
# DA3 pulls an older opencv-python; restore stock rec-server pin (v0.3.3 / v0.4.2).
RUN python3 -m pip install --no-cache-dir \
      "/tmp/colmap-monodepth[da3,mesh,fit]" \
    && (python3 -m pip uninstall -y opencv-python-headless || true) \
    && python3 -m pip install --no-cache-dir --force-reinstall \
      "opencv-python==4.13.0.92" \
    && rm -rf /tmp/colmap-monodepth

# Run reconstruction server as separate user, not root
RUN set -eux; \
    groupadd --gid "$USER_GID" "$USERNAME"; \
    useradd --uid "$USER_UID" --gid "$USER_GID" -m "$USERNAME"; \
    mkdir -p /app/jobs; \
    chown -R "$USERNAME:$USERNAME" /app/jobs

COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod 755 /app/entrypoint.sh

USER $USERNAME

# Copy Rust binary from builder
COPY --from=rust-build /app/target/release/compute-node /app/compute-node

ENTRYPOINT ["/app/entrypoint.sh"]
