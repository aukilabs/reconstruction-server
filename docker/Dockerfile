##
## Rust build for server
##
ARG RECONSTRUCTION_VERSION=0.0.0-local
# Runtime base = docker/Dockerfile.base (Ubuntu 26.04 + CUDA 13.5, COLMAP 4.2.1,
# Ceres f616f61, Torch 2.15/cu134, Turing+ CUDA arches). After that base is
# published, ghcr.io/.../reconstruction-server-base:latest is the default.
# Local bake on Ny1 / PR #108 tip:
#   docker build -f docker/Dockerfile.base -t reconstruction-server-base:u26-cuda135 .
#   docker build --build-arg BASE_IMAGE=reconstruction-server-base:u26-cuda135 \
#     --secret id=github_token,src=... -f docker/Dockerfile -t reconstruction-server:u26-cuda135 .
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
ARG USER_UID=10001
ARG USER_GID=10001

# Application code
WORKDIR /app
# The runners start `python3 -m reconstruction <stage>`; find the package whatever the cwd.
ENV PYTHONPATH=/app

COPY reconstruction /app/reconstruction
COPY cpp /app/cpp
COPY config /app/config
COPY CMakeLists.txt pyproject.toml /app/
COPY docker/start_sshd.sh /app/scripts/start_sshd.sh

RUN chmod -R 755 /app/reconstruction /app/cpp /app/scripts

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
