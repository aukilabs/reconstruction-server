# Compute Node workspace

This workspace hosts the Rust implementation of the compute node that talks to
Posemesh DDS/DMS backends and executes reconstruction workloads. The codebase
follows a strict separation of concerns: a thin binary wires together a reusable
engine crate plus capability-specific runners. Everything is designed to be
stateless, fail-fast, and observable.

## Workspace layout
- [`node-host`](node-host/) — host on the [Auki SDK](https://github.com/aukilabs/auki-sdk)
  task runtime (`auki-sdk`, pinned by git revision in `Cargo.toml`). The SDK
  owns machine registration, authentication, DMS leases and heartbeats;
  `node-host` adds configuration, the claim loop and shutdown, task Domain IO
  (input download layout, artifact upserts), the DMS completion/failure
  receipts and Python process-group handling. It keeps the wire behaviour of
  the former `posemesh-compute-node` 0.3.2 host; `node-host/tests` pins it.
- [`runner-reconstruction-local`](./runner-reconstruction-local/) —
  local refinement runner.
- [`runner-reconstruction-local-auki-session`](runner-reconstruction-local-auki-session/) —
  local refinement of an Auki SDK capture session: the capture zip from the
  Domain, or (`-p2p`) from the robot's peer over Blob v1
  ([docs/compute-capability.md](../../docs/compute-capability.md)).
- [`runner-reconstruction-global`](runner-reconstruction-global/) —
  global refinement runner.
- [`runner-reconstruction-update`](runner-reconstruction-update/) —
  update refinement runner.
- [`runner-reconstruction-common`](runner-reconstruction-common/) — code the
  runners share: starting Python (`python.rs`), the job workspace
  (`workspace.rs`), job metadata and the task log upload (`job.rs`), the
  refined-scan upload (`refined.rs`), and the global and update upload tables
  and upload logic (`output.rs`, `strategy.rs`).
- [`bin`](bin/README.md) — binary that loads configuration, registers the
  runners and drives the host loop.

Supporting directories:
- `scripts/` — helper scripts used by Make targets or CI glue.
- `target/` — build artefacts (ignored in version control).

## High-level data flow
1. The binary boots and installs telemetry.
2. `HostConfig` loads DMS/DDS settings from environment variables
   (`node-host/src/config.rs`).
3. Runners are registered in a `Router`, one per capability; their
   capabilities are advertised to DDS/DMS.
4. The SDK registers and authenticates the node, then the host claims leases
   from DMS (DMS picks the capability), materializes inputs, and the SDK sends
   heartbeats while the runner executes and uploads results.
5. Completion or failure is reported back to DMS, and the cycle repeats. The
   first `SIGINT`/`SIGTERM` stops claiming and lets an active task finish; a
   second interrupts it.

## How runners start Python
Each runner starts `python3 -m reconstruction <stage> <args>` (`local`,
`local-auki-session`, `global` or `update`) in its own process group, from the node's working
directory (`/app` in the image, which also sets `PYTHONPATH=/app`). Each
stdout/stderr line is parsed as a JSON `{level, message}` log line, and on a
non-zero exit `<job root>/fail_reason.txt` becomes the failure reason
(`runner-reconstruction-common/src/python.rs`). Per runner (`LOCAL_`,
`LOCAL_AUKI_SESSION_`, `GLOBAL_` or `UPDATE_` prefix), all optional:

- `<PREFIX>_RUNNER_PYTHON_BIN`: interpreter (default `python3`).
- `<PREFIX>_RUNNER_PYTHON_MODULE`: package run with `-m` (default `reconstruction`).
- `<PREFIX>_RUNNER_PYTHON_SCRIPT`: run this script instead of the module
  (`python3 <script> <args>`).
- `<PREFIX>_RUNNER_PYTHON_ARGS`: extra arguments, placed before the runner's own.

## P2P

Setting `AUKI_PEER_IDENTITY_FILE` (the node's P2P identity; created if
missing) gives the node a P2P peer: it registers the identity with DDS, and a
lease that carries a P2P grant starts a per-task peer that runners reach
through `task.peer()`. The node then also advertises the capabilities that
need it (`/reconstruction/local-refinement-auki-session-p2p/v1`). Leases
without a grant still run HTTP-only. Unset, the node has no peer, as before.

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
   export REGISTER_INTERVAL_SECS=120
   export CLIENT_ID=reconstruction-node/<random uuid per start>
   export LOG_FORMAT=text            # optional for readable logs
   ```
3. Build and run the node:
   ```sh
   cargo run -p bin
   ```
4. Watch the logs for registration and leasing activity.

## Development tooling
- `cargo fmt --all` (or `make fmt`) keeps formatting consistent.
- `cargo clippy --workspace -- -D warnings` (or `make clippy`) enforces lint
  hygiene.
- `cargo test --workspace` (or `make test`) runs unit + integration tests across
  all crates.
- `make ci` executes the full formatter + lint + test pipeline locally.
- The workspace prefers `LOG_FORMAT=json` in production; switch to `text` while
  iterating locally.
