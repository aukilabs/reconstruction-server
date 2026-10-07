# bin

The `bin` crate produces the `compute-node` executable that wires the
`node-host` crate (Auki SDK task runtime) to the reconstruction runners. The
binary itself stays intentionally small.

## Runtime responsibilities
- Initialize telemetry (`node_host::telemetry::init_from_env`) so the process
  respects `LOG_FORMAT` and `RUST_LOG`.
- Load `HostConfig` from the environment, register the local, global and
  update runners, and run the host loop via `node_host::run`.

The binary serves no HTTP endpoints.

## Environment cross-check
See `node-host/src/config.rs` for the full list. At a minimum you will need:
- `REG_SECRET` and `SECP256K1_PRIVHEX` (node registration credential and
  staked wallet key).
- `DDS_BASE_URL` / `DMS_BASE_URL` for non-production environments.
- `REQUEST_TIMEOUT_SECS` (1-300, default 60).

## Running locally
- `make run`, or
- Build the workspace: `cargo build -p bin`.
- Provide required env vars and launch: `LOG_FORMAT=text cargo run -p bin`.
- The process logs the capability list and registration activity; set
  `RUST_LOG=debug` for verbose diagnostics.

## Testing
- `bin` has a unit test that every runner capability is routed, plus link
  tests under `tests/`.
- Host behaviour (receipts, upserts, re-claiming) is covered by
  `node-host/tests`; runner wire contracts by tests in the runner crates.
