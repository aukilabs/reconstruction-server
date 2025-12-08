# runner-reconstruction-legacy-noop

`runner-reconstruction-legacy-noop` implements a capability-compatible version
of the legacy reconstruction runner that exercises all orchestration plumbing
without invoking the expensive Python stack. It is ideal for local smoke tests,
CI environments, or DDS/DMS contract validation.

## Behaviour
- Declares the same legacy capabilities as the full runner:
  `/reconstruction/local-refinement/v1`,
  `/reconstruction/global-refinement/v1`, and
  `/reconstruction/local-and-global-refinement/v1`.
- Materializes every requested CID via the `InputSource` to validate domain
  access. Metadata from the domain server is reflected in control-plane events.
- Sleeps for a configurable duration to mimic compute time.
- Uploads realistic placeholder artifacts:
  - `job_manifest.json`
  - `refined/global/refined_manifest.json`
  - `refined/global/RefinedPointCloudReduced.ply` (empty PLY scaffold)
  - `outputs_index.json`, `result.json`, `scan_data_summary.json`
- Reports progress updates (`inputs_materialized`, `completed`) through the
  control plane so heartbeats exercise the normal path.

## Configuration
- `RunnerReconstructionLegacyNoop::new(sleep_secs)` is exposed for manual
  wiring. The binary pipes through `NOOP_SLEEP_SECS` to keep the delay easy to
  tweak without recompiling.
- The crate itself has no environment-variable surface; it is entirely
  controlled via constructor arguments.

## When to use it
- **Local dev** — launch the node with `ENABLE_NOOP=true` and observe leasing,
  storage IO, and DMS completion without Python or GPU dependencies.
- **Contract testing** — validate that DDS registration + DMS integration works
  end-to-end before rolling out the full runner.
- **CI** — the noop runner keeps tests deterministic and fast while covering
  the majority of the orchestration code paths.

## Development notes
- `cargo test -p runner-reconstruction-legacy-noop` covers capability wiring and
  artifact generation.
- The placeholder artifacts are intentionally minimal but structured. If you
  need to adjust them for downstream consumers, keep filenames stable to avoid
  breaking ingestion expectations.
