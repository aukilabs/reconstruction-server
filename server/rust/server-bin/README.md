Reconstruction Server (Rust)

- Binary: `server-bin`
- Starts HTTP server with endpoints:
  - `POST /jobs` (header `X-API-Key` required)
  - `GET /jobs`
- Single-job mode: `--job-request path/to/request.json` (exits after completion)

Quickstart

- Build: `cargo build --workspace`
- Run: `cargo run -p server-bin -- --api-key secret --port :8080`
