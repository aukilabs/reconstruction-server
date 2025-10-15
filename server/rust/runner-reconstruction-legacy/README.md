# runner-reconstruction-legacy

`runner-reconstruction-legacy` reimplements the legacy reconstruction pipeline
on top of the shared `compute-runner-api`. The runner is capable of servicing
three capabilities that map to the historical product SKUs:

- `/reconstruction/local-refinement/v1`
- `/reconstruction/global-refinement/v1`
- `/reconstruction/local-and-global-refinement/v1`

A thin configuration layer selects Python entrypoints, staging directories, and
mock mode toggles so the same crate can be used for production workloads and
offline smoke tests.

## Execution pipeline
1. **Workspace bootstrap** — `workspace::Workspace` creates a per-task directory
   under the optional `LEGACY_RUNNER_WORKSPACE_ROOT` (or a temp dir) and lays
   out expected subfolders (`datasets`, `refined`, etc.).
2. **Job metadata** — `JobContext` extracts legacy metadata from the lease,
   determines overrides (job name, manifest ID), and writes a JSON record under
   `job_metadata.json`.
3. **Manifest tracking** — `manifest` keeps a `ManifestState` that is periodically
   flushed to disk (`job_manifest.json`) and optionally uploaded to the domain
   server. Progress updates are bridged to the engine via an async channel.
4. **Input materialisation** — `input::materialize_datasets` downloads all
   declared CIDs using the `InputSource`, unpacks refined zips when present, and
   prepares the on-disk layout expected by the Python tooling.
5. **Python control loop** — Unless `LEGACY_RUNNER_MOCK=1`, the runner launches
   the configured Python script and streams its stdout/stderr into
   `python.log`. Cancellation is observed through a `CancellationToken`.
6. **Refined artifact uploads** — `refined::RefinedUploader` periodically scans
   the workspace and uploads new or updated refined outputs via the `ArtifactSink`.
7. **Summary generation** — When the capability implies local/global refinement,
   `summary::write_scan_data_summary` emits `scan_data_summary.json`.
8. **Finalization** — `output::upload_final_outputs` pushes the expected
   manifest, result, and refined artifacts; progress is reported at 100%.

If the Python pipeline errors or cancellation is requested, manifests are
updated to reflect failure and the error is bubbled up so DMS receives a
failure notification.

## Configuration surface

All knobs are exposed through environment variables with sensible defaults:

- `LEGACY_RUNNER_WORKSPACE_ROOT` — override the root directory used for jobs.
  Defaults to a temporary directory under `/tmp`.
- `LEGACY_RUNNER_PYTHON_BIN` (default `python3`) — interpreter used to launch
  the pipeline.
- `LEGACY_RUNNER_PYTHON_SCRIPT` (default `main.py`) — script invoked relative to
  the workspace.
- `LEGACY_RUNNER_PYTHON_ARGS` — additional args (split on ASCII whitespace).
- `LEGACY_RUNNER_CPU_WORKERS` (default `2`) — propagated to the Python process
  via arguments or environment (wiring is performed in the Python helper).
- `LEGACY_RUNNER_MOCK` (`true`/`1` etc.) — when set, skip the Python launch and
  create placeholder outputs instead.

The runner reads most of its behavioural toggles from the lease’s `meta.legacy`
payload:

- `skip_manifest_upload` — keep manifests local rather than re-uploading.
- `override_job_name` / `override_manifest_id` — use provided identifiers when
  generating manifests.
- `processing_type` — bypass automatic capability → processing-type inference.
- `domain_server_url` — override the domain server endpoint embedded in the
  lease.

## On-disk contract
The workspace modules ensure interoperability with the legacy stack:
- `datasets/` — materialized inputs grouped by scan folder.
- `refined/` — refined outputs (`global`, `local`) ready for upload.
- `python.log` — captured Python pipeline output.
- `result.json` / `outputs_index.json` — final payloads shipped back to DMS via
  the artifact sink.

## Testing and development
- `cargo test -p runner-reconstruction-legacy` covers manifest management,
  workspace layout, refined uploader behaviour, and configuration parsing.
- Use `LEGACY_RUNNER_MOCK=1` when iterating locally to avoid invoking the Python
  stack; the runner will still exercise storage, manifests, and uploads.
- The runner logs progress through the control plane; set `RUST_LOG=debug` to
  observe detailed transfer and manifest messages.
