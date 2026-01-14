# bin

The `bin` crate produces the `reconstruction-node` executable that wires
together the engine from `posemesh-compute-node` with the available runners. The
binary itself stays intentionally small: all heavy lifting happens in the shared
crate so we can reuse it in other front-ends or integration tests.

## Runtime responsibilities
- Initialize telemetry (`posemesh_compute_node::telemetry::init_from_env`) so the
  process respects `LOG_FORMAT`.
- Bring up the Axum router exposing:
  - `GET /health` — liveness check for probes.
  - `POST /internal/v1/registrations` — DDS callback that persists the
    registration secret in memory.
- Load `NodeConfig` and wire the local + global scaffold runners.
- Spawn DDS registration if fully configured, and kick off the engine loop via
  `posemesh_compute_node::engine::run_node`.

## Environment cross-check
The binary defers to the configuration code in
[`posemesh-compute-node`](https://github.com/aukilabs/posemesh/tree/main/core/posemesh-compute-node/README.md),
so see that README for exhaustive documentation. At a minimum you will need:
- DDS + DMS URLs and credentials (SIWE private key, registration secret).
- `REQUEST_TIMEOUT_SECS` tuned to your environment (defaults matter if omitted).
- The local/global runners are currently scaffolds; ensure your downstream
  pipeline expectations match the minimal runner behavior.

## Running locally
- `make run`, or
- Build the workspace: `cargo build -p bin`.
- Provide required env vars and launch: `LOG_FORMAT=text cargo run -p bin`.
- Hit `http://localhost:8080/health` to confirm the health endpoint responds.
- The process will log the derived capability list and registration activity;
  set `RUST_LOG=debug` for verbose diagnostics.

## Testing
- Unit tests live alongside the supporting crates (`posemesh-compute-node`,
  runners). There are no binary-specific tests, but the end-to-end happy paths
  are covered by integration tests under that crate’s `tests/` directory.
- `cargo fmt --all` and `cargo clippy --workspace -- -D warnings` keep the
  binary consistent with the rest of the workspace. See the root README for more
  tooling commands.
