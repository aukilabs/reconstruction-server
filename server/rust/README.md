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
| `TOKEN_SAFETY_RATIO` | Fraction of the SIWE token TTL treated as “safe” before refreshing (default `0.75`) |
| `TOKEN_REAUTH_MAX_RETRIES` | Additional SIWE login attempts before surfacing an error (default `3`) |
| `TOKEN_REAUTH_JITTER_MS` | ± jitter applied to the refresh deadline to stagger multiple nodes (default `500`) |
| `HEARTBEAT_JITTER_MS` | Random jitter applied when scheduling heartbeats (default `250`) |
| `POLL_BACKOFF_MS_MIN/MAX` | Bounds for exponential backoff when the queue is empty (defaults `1000/30000`) |
| `DDS_BASE_URL`, `NODE_URL`, `REG_SECRET`, `SECP256K1_PRIVHEX` | Optional DDS registration configuration |
| `REGISTER_INTERVAL_SECS`, `REGISTER_MAX_RETRY`, `REQUEST_TIMEOUT_SECS` | Optional tuning knobs |

`NODE_CAPABILITIES` determines the default claim capability (first entry) and is
propagated to the Python runner so downstream tooling knows which pipeline to
execute.

### Token lifecycle overview

The compute node keeps SIWE-issued access tokens purely in memory. The high
level flow validated by `e2e_dds_dms_tests` is:

1. Fetch nonce from DDS `/api/v1/auth/siwe/request`, sign the message with the node’s
   secp256k1 key, and verify via `/api/v1/auth/siwe/verify`.
2. Cache the returned bearer token until `TOKEN_SAFETY_RATIO * TTL` elapses
   (with jitter from `TOKEN_REAUTH_JITTER_MS`).
3. Send DMS requests with `Authorization: Bearer …`. If DMS replies `401`, call
   `onUnauthorizedRetry` to force a single re-login, then retry once. Any
   subsequent `401` is surfaced to the caller and the token manager remains in a
   stopped state until the next explicit access request.

Tokens are never logged (all tracing fields use `[REDACTED]`) and no refresh
token or SIWE secret is persisted to disk.

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
  task id and capability. Access tokens and bearer headers are redacted.
- The executor exposes optional `metrics` (enable with
  `--features metrics`): `dms.poll.latency_ms` histogram and
  `dms.active_task` gauge. The token manager publishes
  `token.reauth.success` / `token.reauth.error` counters under the same feature
  flag.
- SIGINT/SIGTERM triggers a final best-effort heartbeat before the poller loop
  exits.

### Troubleshooting re-authentication

- Check DDS availability: the node must reach `/api/v1/auth/siwe/request` and
  `/api/v1/auth/siwe/verify` endpoints. Network failures surface as
  `token.reauth.error` metrics and WARN logs.
- `TOKEN_REAUTH_MAX_RETRIES` governs additional login attempts. Use a larger
  value only if DDS is temporarily unstable; the node waits with
  `TOKEN_REAUTH_JITTER_MS` jitter between attempts.
- If DMS continually returns `401`, the client stops retrying after one
  refresh. Inspect the DMS audit logs—the node intentionally avoids looping on
  invalid credentials.
- When rotating keys, deploy a fresh `NODE_IDENTITY` + private key pair; the
  process does not persist refresh tokens that could be revoked later.

## Tests

- Unit tests cover DMS client/poller/session behaviour.
- Integration tests (`dms_executor_tests`) exercise the end-to-end executor
  wiring using mocked runners.
- `make ci` runs `cargo fmt`, `cargo clippy`, and `cargo test --workspace`.
