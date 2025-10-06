Reconstruction Server (Rust)

- Binary: `server-bin`
- Starts HTTP server with endpoints:
  - `POST /jobs` (header `X-API-Key` required)
  - `GET /jobs`
- Single-job mode: `--job-request path/to/request.json` (exits after completion)

Quickstart

- Build: `cargo build --workspace`
- Run: `cargo run -p server-bin -- --api-key secret --port :8080`

## Environment

Token lifecycle settings are configured via environment variables when the node starts:

- `TOKEN_SAFETY_RATIO` — fraction (0.0–1.0, default `0.75`) of token lifetime to treat as "safe" before requesting refresh.
- `TOKEN_REAUTH_MAX_RETRIES` — maximum retry attempts for token re-authentication (default `3`).
- `TOKEN_REAUTH_JITTER_MS` — random jitter in milliseconds applied between retries (default `500`).

The existing `REQUEST_TIMEOUT_SECS` value continues to apply to outbound DDS calls used during token refresh.
