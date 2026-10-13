##
## Rust build for server
##
ARG RECONSTRUCTION_VERSION=0.0.0-local
# Base image (COLMAP/Ceres/pycolmap toolchain) built from docker/Dockerfile.base.
# Override to test against a locally built base, e.g.
#   docker build --build-arg BASE_IMAGE=reconstruction-server-base:colmap-4.2.1 -f docker/Dockerfile .
ARG BASE_IMAGE=ghcr.io/aukilabs/reconstruction-server-base:latest
FROM --platform=$BUILDPLATFORM rust:1.89-bullseye AS rust-build
ARG RECONSTRUCTION_VERSION
ENV RECONSTRUCTION_VERSION="${RECONSTRUCTION_VERSION}"
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
WORKDIR /app
COPY server/rust/ ./
# Build release binary for the compute-node crate
RUN --mount=type=secret,id=github_token \
    git config --global url."https://x-access-token:$(cat /run/secrets/github_token)@github.com/".insteadOf "https://github.com/" \
    && cargo build --release -p bin \
    && rm -f /root/.gitconfig

##
## Runtime
##
FROM ${BASE_IMAGE} AS final

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
