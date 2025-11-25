##
## Rust build for server
##
FROM --platform=$BUILDPLATFORM rust:1.89-bullseye AS rust-build
WORKDIR /app
COPY server/rust/ ./
# Build release binary for the compute-node crate
RUN cargo build --release -p bin

##
## Runtime
##
FROM ghcr.io/aukilabs/reconstruction-server-base:latest AS final

ARG USERNAME=reconstruction-server
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG DEBIAN_FRONTEND=noninteractive

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

ENTRYPOINT ["/app/compute-node"]
