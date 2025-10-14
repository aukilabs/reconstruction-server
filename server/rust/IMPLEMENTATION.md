# IMPLEMENTATION

This document is the canonical technical reference for the Rust workspace under `server/rust/`. It describes the current implementation as it exists today: architecture, modules, data flow, configuration, error handling, observability, tests, CI/CD, and conventions.

## Overview

- Purpose: A compute node that registers with DDS, leases tasks from DMS, executes the existing Python reconstruction pipeline, and reports heartbeats/completion/failure back to DMS. It also uploads artifacts to the Domain service.
- Scope: Rust workspace consisting of three crates: `server-core`, `server-adapters`, and `server-bin`.
- Principles: SUCK‑less, KISS, DRY, fail‑fast, no hidden fallback logic. All credentials are redacted in logs. Failures are surfaced early with explicit error types.

## Architecture

- Components:
  - `server-core`: Domain model and orchestration (Job lifecycle, manifest writers/uploaders, traits for Domain I/O and Python runner).
  - `server-adapters`: Adapters for concrete systems (DMS client/poller/executor, DDS registration HTTP surface via external crate, Domain storage HTTP client, SIWE auth, token manager).
  - `server-bin`: Binary wiring (CLI + env config, logging, HTTP server bootstrap exposing DDS endpoints, background executor loop, graceful shutdown).

- Module dependency diagram (text):
  - `server-bin` → `server-adapters::{dms, dds, storage, auth}` → external HTTP services (DMS, DDS, Domain)
  - `server-bin` → `server-core::{usecases, manifest, types, errors}`
  - `server-adapters::storage` → `server-core::{DomainPort, ExpectedOutput, Job}`
  - `server-adapters::dms::{executor,poller,client,session}` → `server-core::{Services, AccessTokenSink}`

- Data/control flow summary:
  1. Node starts, loads config, initializes logging, builds HTTP router for DDS callbacks and `/health`.
  2. Background loop: poll DMS for a lease; if leased, materialize a job, fetch inputs (URIs or data IDs) from Domain, run Python pipeline, stream heartbeats, upload outputs, then report completion/failure to DMS.
  3. Periodic local manifest file is written atomically and optionally uploaded to Domain; a separate uploader also periodically POST/PUTs the manifest.
  4. Optional: if DDS SIWE auth configured, the node uses a token manager to obtain/refresh short‑lived tokens for DMS.

- Domain entities and interactions:
  - Job/JobMetadata (on-disk layout in `jobs/<domain_id>/job_<id>/...`).
  - DomainPort trait for upload/download operations against Domain service.
  - DmsApi/DmsClient for leasing/heartbeats/complete/fail.
  - SessionManager governs active lease state and heartbeat scheduling.
  - TokenManager (optional) governs SIWE token lifecycle with jitter and retries.

## Crate and Module Layout

- Workspace: `server/rust/Cargo.toml`
  - Members: `server-core`, `server-adapters`, `server-bin`
  - Resolver 2; Rust 2021; workspace dependencies pin: `tokio`, `axum`, `serde`, `thiserror`, `anyhow`, `tracing`, `reqwest` (rustls), etc.

- `server-core`
  - Role: Core domain and orchestration logic; no direct HTTP.
  - Public API (lib.rs): re‑exports `errors`, `manifest`, `types`, `usecases`.
  - Modules:
    - `errors.rs`: `DomainError` enum (`Unauthorized`, `BadRequest`, `NotFound`, `Conflict`, `Io`, `Json`, `Http`, `Internal`); `type Result<T>`.
    - `types.rs`: `JobMetadata`, `Job`, `ProcessingType` (serde), `JobRequestData`, Domain data structs, `ExpectedOutput`; `AccessTokenSink` for refreshing per‑lease tokens.
    - `manifest.rs`: Periodic local manifest writer (atomic writes); periodic manifest uploader (POST then PUT); Python helpers for rich manifests; cancellation handling.
    - `usecases.rs`: Traits `DomainPort`, `JobRunner`; `Services`; job creation (`create_job_metadata`), manifest bootstrap (`write_job_manifest_processing`), job execution (`execute_job`) including input fetch logic, progress monitoring, local refined scan zipping+uploads, final output uploads.
  - Boundaries: `DomainPort` and `JobRunner` are the only integration points; everything else is file system and pure Rust.

- `server-adapters`
  - Role: Concrete adapters for DMS, DDS, storage, and authentication.
  - Modules:
    - `dms/client.rs`: HTTP client for DMS (`/tasks`, `/tasks/{id}/heartbeat|complete|fail`), bearer retries on 401, session state updates.
    - `dms/session.rs`: Lease/session state, TTL‑based randomized heartbeat scheduling, capability selection, exposes a `DomainTokenProvider` impl to reuse lease tokens for Domain uploads.
    - `dms/poller.rs`: Poll controller with exponential backoff+jitter, heartbeat and failure reporting flows, `DmsApi` trait with impl for `DmsClient`.
    - `dms/executor.rs`: Task executor translating a DMS lease into `server-core::execute_job` with progress heartbeats; computes and reports completion/failure; graceful shutdown heartbeat.
    - `auth/siwe.rs`: SIWE nonce request, message composition, signing, verify; builds URLs under `/internal/v1/auth/siwe/*`; returns `AccessBundle`.
    - `auth/token_manager.rs`: Background token manager with safety ratio, jitter, bounded retries; `TokenProvider` and `AccessAuthenticator` traits; optional `metrics` counters.
    - `storage/mod.rs`: `HttpDomainClient` implementing `DomainPort` over HTTP to Domain `POST/PUT /api/v1/domains/{domain_id}/data`; download multipart batches; unzip refined scan ZIPs; HTTP→`DomainError` mapping.
    - `storage/token.rs`: `DomainTokenProvider` trait.
    - `lib.rs`: Re‑exports `posemesh-node-registration` as `dds` (for HTTP router and registration helpers).
  - Features: `metrics` (optional, enables metrics macros). Unused in code today but present: `grpc`, `ws`, `openapi` features (no `cfg(feature)` users).

- `server-bin`
  - Role: Binary crate that wires everything together.
  - Entry points:
    - `main.rs`: CLI parsing, offline single job path (`--job-request`), normal mode (DMS poller + DDS HTTP server), logging setup, graceful shutdown.
    - `cli.rs`: `Cli` with flags and env var bindings.
    - `config.rs`: `NodeConfig` from env, including DMS/DDS/node identity/capabilities and polling/heartbeat/token settings.
  - Public API: none (binary).

## HTTP Interface

This service exposes only DDS registration/health endpoints via the re‑exported `posemesh-node-registration` HTTP router.

- Endpoints

| Method | Path                           | Handler                          | Purpose                               | Stability |
|--------|--------------------------------|----------------------------------|---------------------------------------|-----------|
| GET    | `/health`                      | `dds_http::router_dds`           | Liveness/health check                 | Stable    |
| POST   | `/internal/v1/registrations`   | `dds_http::router_dds`           | DDS registration callback; persists node secret (handled by external crate) | Stable    |

- Middleware stack: Axum default service with `tracing_subscriber` configured in `server-bin`. No custom middleware chain; no compression/auth middlewares at server boundary.
- Request/response schema: Determined by `posemesh-node-registration` crate. Tests assert `POST /internal/v1/registrations` accepts JSON with `secret` and does not log the secret.
- Auth model at server boundary: None (registration secret payload handled by external crate). All outbound calls to DMS/Domain use bearer tokens (see Security).

Note: SIWE endpoints `/internal/v1/auth/siwe/request` and `/internal/v1/auth/siwe/verify` are consumed by this node (as a client) and are hosted by DDS, not by this service.

## Configuration & Environment

Configuration is loaded in `server-bin/src/config.rs` and `server-bin/src/cli.rs`. All values may be provided as environment variables; most are also available as CLI flags.

- Required (for normal DMS polling mode):
  - `DMS_BASE_URL` (string, required): Base URL for DMS (e.g., `https://dms.example.com/`).
  - `NODE_IDENTITY` (string, required): `siwe:<uuid>` identity used when not using SIWE against DDS.
  - `NODE_CAPABILITIES` (string, required): Comma‑separated list; first entry is default capability (e.g., `/reconstruction/local-and-global-refinement/v1`).

- Optional (polling/heartbeat/tokens):
  - `HEARTBEAT_JITTER_MS` (u64, default 250).
  - `POLL_BACKOFF_MS_MIN` (u64, default 1000).
  - `POLL_BACKOFF_MS_MAX` (u64, default 30000). Must be >= min.
  - `TOKEN_SAFETY_RATIO` (f64 in [0.0,1.0], default 0.75).
  - `TOKEN_REAUTH_MAX_RETRIES` (u32, default 3).
  - `TOKEN_REAUTH_JITTER_MS` (u64, default 500).

- Optional (DDS registration + SIWE auth):
  - `DDS_BASE_URL` (string): Base URL for DDS. When set with `SECP256K1_PRIVHEX`, SIWE is used for DMS auth.
  - `NODE_URL` (string): Public node URL for DDS registration.
  - `REG_SECRET` (string): Shared registration secret for DDS.
  - `SECP256K1_PRIVHEX` (hex string, 32‑byte priv key, optional `0x` prefix): Used for SIWE.
  - `REGISTER_INTERVAL_SECS` (u64, optional): DDS re‑registration cadence.
  - `REGISTER_MAX_RETRY` (i32, optional): Max registration retries.
  - `REQUEST_TIMEOUT_SECS` (u64, default 10): HTTP client timeout for DDS/DMS.

- Optional (binary/runtime):
  - `PORT` (string, default `:8080`): Server bind address; `:8080` implies `0.0.0.0:8080`.
  - `DATA_DIR` (path, default `jobs`): Root folder for job storage.
  - `JOB_MANIFEST_INTERVAL_MS` (u64, default 2000, min 10): Local manifest write interval.
  - `MOCK_PYTHON` (bool, default false): Use no‑op runner instead of invoking Python.
  - `LOG_FORMAT` (string, default `json`): `json` or `text` for logs.

- Loading & validation:
  - `NodeConfig::from_env` validates required keys, URL formats, ratios and backoff bounds; fails fast on missing/invalid values with explicit `ConfigError` variants.
  - CLI augments config with server binding, file paths, logging settings, and test helpers.

Policy: New variables must be explicit, validated, and documented with defaults. Fail fast on missing/invalid values.

## Error Model

- Unified pattern: Each crate defines a local error enum using `thiserror` and maps external failures at boundaries.
  - `server-core::DomainError` for job orchestration and Domain I/O.
  - `server-adapters::dms::DmsClientError` for DMS client operations.
  - `server-adapters::dms::executor::TaskExecutorError` wraps poll/fail/complete/setup errors.
  - `server-adapters::auth::token_manager::TokenManagerError` and `TokenProviderError` for SIWE lifecycle.
  - `server-adapters::auth::siwe::SiweError` for auth composition/signing/requests.

- HTTP mapping (Domain uploads/downloads):
  - 400 → `DomainError::BadRequest`, 401 → `DomainError::Unauthorized`, 404 → `DomainError::NotFound`, 409 → `DomainError::Conflict`, others → `DomainError::Http` with context.

- Propagation rules:
  - Fail‑fast on I/O/serde errors when writing manifests, reading files, parsing config.
  - DMS 401 is retried once with forced token refresh; subsequent 401 is surfaced.
  - Lease cancellation or loss triggers cancellation of the running job; completion/failure reporting is skipped accordingly.

- Naming/visibility conventions: Error enums are crate‑local; cross‑crate conversion only at seams (e.g., executor mapping). No `unwrap` in critical paths; tests may use it.

## Logging & Telemetry

- Logging: `tracing` with `tracing_subscriber` configured via `RUST_LOG`/CLI log level; JSON or text output. Key spans and events:
  - DMS lease/heartbeat/complete/fail spans include `task_id`, `capability`. Access tokens are redacted (`[REDACTED]`).
  - Storage uploads/downloads log URLs, names, and outcomes; sensitive headers are marked sensitive.
  - Manifest writer/uploader logs startup, ticks, and errors; avoids body dumps.

- Metrics (optional feature `metrics` in `server-adapters`):
  - `dms.poll.latency_ms` (histogram), `dms.active_task` (gauge), `token.reauth` counters.
  - No built‑in Prometheus exporter in this repository. Integrators must initialize a global recorder/exporter if desired.

- Verbosity rules: No request/response body dumps; health endpoints are minimal. Secrets are never logged.

## Database & Persistence

- Database: None. No SQLx/Diesel usage. No migrations.
- Persistence: File system under `DATA_DIR` (default `jobs`):
  - `jobs/<domain_id>/job_<uuid>/job_request.json`
  - `jobs/<domain_id>/job_<uuid>/job_metadata.json`
  - `jobs/<domain_id>/job_<uuid>/job_manifest.json` (periodically updated atomically)
  - `jobs/<domain_id>/job_<uuid>/datasets/<scan>/...` (inputs)
  - `jobs/<domain_id>/job_<uuid>/refined/local/<scan>/sfm/...` (local outputs)
  - `jobs/<domain_id>/job_<uuid>/refined/global/...` (final outputs)
- Atomic writes: Manifest writer writes to a temp file in the same directory, then renames.
- Zip handling: Local refined scan folder zipped and uploaded progressively.

## Dependency Policy

- Core dependencies: `tokio`, `axum`, `serde`, `reqwest` (with `rustls-tls`), `tracing`, `thiserror`, `uuid`, `chrono`.
- Optional features: `metrics` in `server-adapters`; enabled via `--features metrics`.
- TLS: Rustls is selected (`reqwest` feature `rustls-tls`); no native‑tls required.
- Policy: Keep dependency set minimal; use workspace dependencies; prefer compile‑time features over runtime flags; deny warnings in CI.

## Testing Strategy

- Layers:
  - Unit tests in `server-core` (manifest writer atomicity; progress computation; job execution helpers) and `server-adapters` (token manager, session scheduling, poller/controller).
  - Integration tests in `server-adapters/tests/*` for DMS client, executor behavior, storage HTTP client (multipart parsing, ZIP extraction), DDS HTTP router (`/internal/v1/registrations`, `/health`).
  - End‑to‑end style tests mocking DDS SIWE + DMS (`e2e_dds_dms_tests.rs`).
  - Binary smoke test: `server-bin/tests/cli_job_request_tests.rs` exercises `--job-request` path.

- Data setup/teardown: Tests use `tempfile` directories; mocks via `wiremock`/`httpmock`; background tasks use tokio time control.
- Coverage targets: Not enforced. CI runs `cargo test --workspace --all-features --locked`.
- Commands:
  - `cargo test --workspace`
  - `make ci` (fmt, clippy, test)

## CI/CD & Build

- CI (`.github/workflows/ci-rust.yml`):
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  - `cargo test --workspace --all-features --locked`

- Docker builds:
  - `docker/Dockerfile_RUST`: minimal runtime with just the Rust binary (`server-bin` as `/usr/local/bin/reconstruction`).
  - `docker/Dockerfile`: full runtime with CUDA, Python toolchain and dependencies, builds C++/Python components, and copies the Rust release binary as `./reconstruction` (entrypoint).
  - Tagging/push configured in CI; optional ECR push on tags (`.github/workflows/tag.yml`).
  - Layering/caching: Cargo sources for Rust workspace copied as a unit; Python deps built in layers to maximize cache hits.

## Security

- Authentication/authorization:
  - Outbound to DMS: Bearer tokens from either static `NODE_IDENTITY` or SIWE token manager; 401 triggers a single forced refresh then retry.
  - Outbound to Domain (uploads/downloads): Prefers DMS lease access token via `SessionManager` (`DomainTokenProvider`); falls back to legacy metadata token only when session token absent (e.g., offline single‑job).
  - Inbound: `POST /internal/v1/registrations` handled by external router; secret is persisted via that crate and never logged (tests verify redaction).

- Secret management: SIWE uses in‑memory token manager; no refresh tokens persisted by this code. Registration secret persistence is handled by `posemesh-node-registration` (external); logs redact secrets.

- Safe defaults:
  - HTTP timeouts applied to DMS/Domain clients (`REQUEST_TIMEOUT_SECS`).
  - Exponential backoff with jitter for polling; randomized heartbeat scheduling within a safe ratio band.
  - No body dumps in logs; sensitive headers marked sensitive.

## Public API & Versioning

- Public crates: `server-core` and `server-adapters` are currently workspace members; versions `0.1.0` and not published.
- Binary: `server-bin` (default run).
- Features: `server-bin` exposes feature `metrics` that enables metrics in adapters.
- Versioning: Semver intended within workspace boundaries; external API surface is the DDS HTTP router (defined externally) and DMS/Domain client contracts (consumed, not provided).
- Release checklist:
  - Ensure CI green: fmt, clippy (deny warnings), tests.
  - Build Docker images (`docker/Dockerfile` for full runtime, `docker/Dockerfile_RUST` for minimal).
  - Tag and push via GitHub Actions (see `tag.yml`).

## Conventions

- Code style: `rustfmt` enforced; `clippy` warnings denied in CI.
- Visibility: Keep items `pub(crate)` unless required across crates; expose traits and types at the seam only.
- Errors: Prefer `thiserror` enums per crate; map precisely at boundaries; avoid `unwrap`/`expect` in non‑test code.
- Logging: Structured `tracing`; redact tokens/secrets; avoid verbose bodies.
- Branch/commit: CI recognizes `feature/**`, `bug/**`, `chore/**`, `hotfix/**`.

## Future Work / Known Gaps

- Unused features in `server-adapters`: `grpc`, `ws`, `openapi` appear unused; consider removal.
- Unused module: `server-adapters/src/config.rs` (`AppConfig`) not referenced; candidate for deletion.
- Visibility tightening in `server-core`: Narrow re‑exports; make helpers `pub(crate)` where possible.
- Metrics export: No Prometheus exporter included; add integration if required.

## Verification Commands Appendix

```bash
# Find unsafe unwraps/hidden fallbacks
rg "unwrap" --type rust server/rust
rg "expect\(" --type rust server/rust

# Track technical debt
rg "TODO" --type rust server/rust

# Ensure formatting and linting
cargo fmt --all --check
cargo clippy --workspace -- -D warnings

# Run full test suite
cargo test --workspace

# Binary smoke test (offline single job)
cargo run -p server-bin -- --job-request path/to/request.json --data-dir jobs --mock-python
```

