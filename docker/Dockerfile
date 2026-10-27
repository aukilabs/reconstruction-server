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
## Auki SDK Python bindings, at the auki-sdk rev server/rust/Cargo.toml pins
##
FROM --platform=$BUILDPLATFORM rust:1.89-bullseye AS auki-python
WORKDIR /src
COPY server/rust/Cargo.toml server/rust/Cargo.toml
COPY docker/build_auki_python.sh docker/build_auki_python.sh
RUN --mount=type=secret,id=github_token \
    git config --global url."https://x-access-token:$(cat /run/secrets/github_token)@github.com/".insteadOf "https://github.com/" \
    && docker/build_auki_python.sh . /wheels \
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

# Auki SDK Python bindings (auki_geometry: frame convention conversions)
COPY --from=auki-python /wheels /tmp/auki-wheels
RUN python3 -m pip install --no-cache-dir --no-deps /tmp/auki-wheels/*.whl && rm -rf /tmp/auki-wheels

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
