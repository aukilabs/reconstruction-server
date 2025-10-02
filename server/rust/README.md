# Rust Compute Node

This workspace hosts the Rust implementation of the reconstruction compute node.
Instead of exposing a public `/jobs` API, the node registers with DDS, polls the
DMS task queue, executes the existing Python pipeline, and reports task
heartbeats/completion/failure back to DMS.

## Crate Layout

| Crate | Purpose |
| ----- | ------- |
| `server-core` | Job orchestration, access token propagation, periodic manifest helpers |
| `server-adapters` | DMS client/poller/executor, DDS registration HTTP surface, storage adapters |
| `server-bin` | CLI, configuration loader, node bootstrap and graceful shutdown |

The only HTTP endpoints shipped in the binary are the DDS registration callback
(`POST /internal/v1/registrations`) and `/health`.

## Configuration

`server-bin` reads the following environment variables (all available as CLI
flags). Required values are marked `*`.

| Variable | Description |
| -------- | ----------- |
| `DMS_BASE_URL`* | Base URL for the DMS API (e.g. `https://dms.example.com/`) |
| `NODE_IDENTITY`* | Identity used when authenticating to DMS (`siwe:<uuid>`) |
| `NODE_CAPABILITIES`* | Comma‑separated list of capabilities this node can claim |
| `HEARTBEAT_JITTER_MS` | Random jitter applied when scheduling heartbeats (default `250`) |
| `POLL_BACKOFF_MS_MIN/MAX` | Bounds for exponential backoff when the queue is empty (defaults `1000/30000`) |
| `DDS_BASE_URL`, `NODE_URL`, `REG_SECRET`, `SECP256K1_PRIVHEX` | Optional DDS registration configuration |
| `REGISTER_INTERVAL_SECS`, `REGISTER_MAX_RETRY`, `REQUEST_TIMEOUT_SECS` | Optional tuning knobs |

`NODE_CAPABILITIES` determines the default claim capability (first entry) and is
propagated to the Python runner so downstream tooling knows which pipeline to
execute.

## Development Commands

```bash
# Format + lint + test
make ci

# Format
make fmt

# Lint (clippy with warnings as errors)
make clippy

# Run with mock Python pipeline
docker compose up   # if you need supporting services
make run             # equivalent to `cargo run -p server-bin -- --mock-python`
```

To execute a single job request offline:

```bash
cargo run -p server-bin -- \
  --job-request /path/to/request.json \
  --data-dir jobs \
  --mock-python
```

## Observability & Shutdown

- All lease/heartbeat/complete/fail requests emit `tracing` spans tagged with
  task id and capability. Access tokens are redacted.
- The executor exposes optional `metrics` (enable with
  `--features metrics`): `dms.poll.latency_ms` histogram and
  `dms.active_task` gauge.
- SIGINT/SIGTERM triggers a final best-effort heartbeat before the poller loop
  exits.

## Tests

- Unit tests cover DMS client/poller/session behaviour.
- Integration tests (`dms_executor_tests`) exercise the end-to-end executor
  wiring using mocked runners.
- `make ci` runs `cargo fmt`, `cargo clippy`, and `cargo test --workspace`.
