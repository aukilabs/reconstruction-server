# AGENTS.md

Guidance for AI agents working in `aukilabs/reconstruction-server`.

## Repository identity

This repo contains the Auki Network Reconstruction Node. The node processes DMT scans into 3D reconstruction artifacts such as point clouds, bounding boxes, and gaussian splats. The runtime entrypoint is the Rust compute-node binary, but it still invokes the Python/C++ reconstruction pipeline under the hood.

## Scope and safety boundaries

- Default branch is `develop`; create task branches from the current remote default unless a card says otherwise.
- Keep documentation-only changes documentation-only. Do not modify product code, charts, workflows, generated files, runtime config, credentials, or infrastructure unless the task explicitly asks for that file and risk class.
- Treat Kubernetes/ArgoCD/Terragrunt changes as deployment-sensitive. Do not apply, sync, restart, scale, delete, or otherwise mutate live workloads from this repo-local task context.
- Do not read, print, commit, or copy secrets. Runtime credentials include registration secrets, SIWE/EVM private keys, DDS/DMS credentials, and any inline `apiKey`-like values found in chart value files.
- If you need to inspect chart values, quote only field names or redacted shapes in comments and handoffs; never paste raw secret-looking values into prompts, PR bodies, or Kanban metadata.

## Architecture map

- `server/rust/` is the Rust compute-node workspace.
  - `bin/` builds the executable that exposes health and registration endpoints, loads configuration, registers runners, and drives the engine loop.
  - `runner-reconstruction-local/` and `runner-reconstruction-global/` adapt local/global reconstruction capabilities.
  - The workspace depends on Posemesh compute-node crates for DDS registration, DMS polling/leasing, SIWE auth, storage, telemetry, and HTTP routing.
- Root Python files (`main.py`, `local_main.py`, `global_main.py`, `topology_main.py`, `occlusion_box.py`) and `utils/` hold the reconstruction pipeline code invoked by the runners.
- `src/` and `CMakeLists.txt` hold C++/pybind components used by the Python pipeline.
- `docker/Dockerfile` builds the Rust release binary, copies Python/C++ assets into `/app`, builds the C++ bindings, and runs `/app/compute-node` as a non-root user.
- `charts/reconstruction-server/` contains the Helm chart wrapper and environment values for deployed reconstruction-server instances.
- `.devcontainer/` provides the GPU-capable development container and build helpers.

## Runtime and deployment cautions

- The node is environment-driven. Public docs list `REG_SECRET`, `SECP256K1_PRIVHEX`, `DMS_BASE_URL`, `DDS_BASE_URL`, `REQUEST_TIMEOUT_SECS`, `REGISTER_INTERVAL_SECS`, `REGISTER_MAX_RETRY`, `LOG_FORMAT`, `LOCAL_RUNNER_CPU_WORKERS`, and `GLOBAL_RUNNER_CPU_WORKERS` as relevant runtime settings. Use placeholder values only.
- DDS/DMS settings affect registration, task leasing, heartbeats, and result reporting. Be careful when changing config parsing, URL defaults, retry behavior, or capability registration.
- The chart defaults request substantial CPU/memory and a GPU (`nvidia.com/gpu: 1`) and include a `dedicated=karpenterGPU` toleration. Treat replicas, StatefulSet behavior, GPU resources, tolerations, ingress, and secret pool wiring as operationally sensitive.
- Dev deployment context observed by the broader Auki v0 workspace maps this repo to the `reconstruction-server` StatefulSet and `reconstruction-<n>.dev.aukiverse.com` host pattern. Treat that as context, not permission to mutate the cluster.

## Development and verification commands

Read the relevant README/docs before running heavy commands. For most docs-only changes to this file, prefer lightweight checks:

```sh
git diff --check
test -f AGENTS.md
git status --short
```

Rust checks live under `server/rust/` and are delegated by the root `Makefile`:

```sh
make fmt
make clippy
make test
make ci
```

Those commands run `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `cargo test --workspace`. CI also runs Docker build/push logic, so do not run Docker/GPU/CUDA builds for docs-only changes unless a reviewer explicitly requests it.

## PR hygiene for agents

- Keep changes focused. A repo-local AGENTS.md PR should change only `AGENTS.md`.
- Preserve existing repo-specific guidance if this file already exists; improve it rather than replacing useful rules.
- Before pushing, verify the diff contains no unrelated files, no secrets, and no accidental line-number prefixes from tool output.
- PR bodies should summarize the docs-only nature of the change and list the lightweight verification commands actually run.
