# Compute Node workspace

This workspace hosts the Rust implementation of the compute node that talks to
Posemesh DDS/DMS backends and executes reconstruction workloads. The codebase
follows a strict separation of concerns: a thin binary wires together a reusable
engine crate plus capability-specific runners. Everything is designed to be
stateless, fail-fast, and observable.

## Workspace layout
- [`posemesh-compute-node-runner-api`](https://github.com/aukilabs/posemesh/tree/main/core/compute-node-runner-api/README.md) —
  trait-based API surface that all runners implement. Defines the lease/task
  contracts as serde models.
- [`posemesh-compute-node`](https://github.com/aukilabs/posemesh/tree/main/core/compute-node/README.md) —
  engine + shared infrastructure: config, SIWE auth, DDS registration, DMS
  client, heartbeat loop, storage facade, HTTP router, telemetry helpers.
- [`runner-reconstruction-local`](./runner-reconstruction-local/) —
  scaffold runner for local refinement pipeline integration.
- [`runner-reconstruction-global`](runner-reconstruction-global/) —
  scaffold runner for global refinement pipeline integration.
- [`bin`](bin/README.md) — CLI binary
  that loads configuration, selects runners, exposes the registration callback,
  and drives the engine loop.

Supporting directories:
- `scripts/` — helper scripts used by Make targets or CI glue.
- `target/` — build artefacts (ignored in version control).

## High-level data flow
1. The binary boots, installs telemetry, and starts the HTTP server (health +
   DDS registration).
2. `NodeConfig` loads all DMS/DDS settings from environment variables. See
   [`posemesh-compute-node/README.md`](https://github.com/aukilabs/posemesh/tree/main/core/compute-node/README.md)
   for the exhaustive list.
3. Runners are registered in a `RunnerRegistry`; the binary decides which
   capabilities to advertise.
4. Once DDS supplies a SIWE token, the engine polls DMS, leases work, materializes
   inputs, streams heartbeats, and uploads results through the domain storage
   facade.
5. Completion or failure is reported back to DMS, and the cycle repeats until
  the process is stopped or receives `SIGINT`.

## Getting started
1. Install the pinned toolchain (`rustup toolchain install stable` if missing; the
   workspace ships with `rust-toolchain.toml`).
2. Export configuration:
   ```sh
   export REG_SECRET=replace-me
   export SECP256K1_PRIVHEX=32-byte-hex-string
   ```
   Optional overrides (defaults shown):
   ```sh
   export DMS_BASE_URL=https://dms.auki.network/v1
   export DDS_BASE_URL=https://dds.auki.network
   export REQUEST_TIMEOUT_SECS=60
   export LOG_FORMAT=text            # optional for readable logs
   ```
3. Build and run the node:
   ```sh
   cargo run -p bin
   ```
4. Watch the logs for DDS registration and leasing activity.

## Development tooling
- `cargo fmt --all` (or `make fmt`) keeps formatting consistent.
- `cargo clippy --workspace -- -D warnings` (or `make clippy`) enforces lint
  hygiene.
- `cargo test --workspace` (or `make test`) runs unit + integration tests across
  all crates.
- `make ci` executes the full formatter + lint + test pipeline locally.
- The workspace prefers `LOG_FORMAT=json` in production; switch to `text` while
  iterating locally.
