# tools

Developer tools. None of them run in production.

## Golden run

The golden run catches behaviour changes in the Python pipeline during refactors. It runs local, global and update on a fixed dataset the way the Rust runners do, then summarizes what should survive a refactor.

- `golden_run.py` stages one fresh workspace per stage and starts Python with the runner's exact command line, from the repository root (the node's cwd, `/app` in the image). Then it writes a summary JSON.
- `golden_compare.py` compares two summaries. It exits 1 on any difference beyond tolerance.
- `golden_tolerances.json` holds the numeric tolerances (see [Tolerances](#tolerances)).

The golden run bypasses Rust entirely. It does not check input download, log forwarding, uploads or receipts.

### What the summary holds

For each stage:

| Field | Compared |
|---|---|
| `files`: every file under `refined/`, with its size as an order of magnitude | File set exactly; sizes may differ by one order of magnitude |
| `root_files`: files directly in the job root (`log.txt`, `fail_reason.txt`, `job_manifest.json`, …) | Exactly |
| `sfm`: cameras, registered images, 3D points and mean reprojection error for every COLMAP model under `refined/` | Within tolerance |
| `ply_vertices`: vertex count of every PLY under `refined/` | Within tolerance |
| `manifests`: key structure (keys only) and portal ids of each `refined_manifest.json` / `job_manifest.json` | Exactly |
| `uploads`: whether each path in the upload table (`tests/test_upload_contract.py`) exists | Exactly |
| `exit_code`, `fail_reason` | Exactly |
| `json_lines_missing_level_or_message`: stdout/stderr lines that are JSON but lack `level` or `message` | Exactly (should be 0) |
| `info`: duration and log line counts | Not compared |

### Source layout

A golden job folder looks like the workspace the runners leave after downloading inputs. Keep it under `_local/golden/` (gitignored) and don't commit it.

```
_local/golden/<job>/
├── golden.json                          # which scans each stage takes, and the domain id
├── datasets/<scan>/                     # raw captures with the runner's renamed inputs: local's input
│   └── Manifest.json, ARposes.csv, Frames.mp4, Frames.csv, CameraIntrinsics.csv, PortalDetections.csv, …
├── refined/local/<scan>/sfm/            # already refined scans: global and update inputs
└── refined/global/                      # a previous global map: update's input
    ├── refined_manifest.json
    └── refined_sfm_combined/{cameras,frames,images,points3D,rigs}.bin
```

```json
{
  "domain_id": "00000000-0000-0000-0000-000000000000",
  "stages": {
    "local": {"scans": ["<raw scan>"]},
    "global": {"scans": ["<refined scan>", "..."]},
    "update": {"scans": ["<refined scan>", "..."]}
  }
}
```

The renames from Domain data names to the files Python reads are in `server/rust/runner-reconstruction-local/src/input.rs` (for example `dmt_arposes_*` becomes `ARposes.csv`, `dmt_observations_*` becomes `PortalDetections.csv` and `dmt_recording_*` becomes `Frames.mp4`).

Stages chain the way they do in production. Global and update take a scan's `sfm` from this run's local output when local produced it, and from `refined/local/<scan>/sfm` in the source otherwise. Each refined scan is staged as the runner does it: `datasets/<scan>/RefinedScan.zip` with the `.bin`, `.csv` and `.txt` files, extracted into `refined/local/<scan>/sfm`.

### Running it

Run it inside the base image, `ghcr.io/aukilabs/reconstruction-server-base:<sha>`, with the working tree mounted at `/app`. `build-base-image.yml` publishes one tag per commit that changes `docker/Dockerfile.base`, so use the last such commit on your base branch (`git log -1 --format=%H origin/develop -- docker/Dockerfile.base`). Compare runs only within one base image. The mount hides the native modules built into the image, so build them in the mount first (`*.so` and `build/` are gitignored).

The command runs as your uid, like the node's non-root user (uid 1000). `USER` must be set because the base image has no passwd entry for that uid, and LightGlue's torch compile calls `getpass.getuser()`. hloc and EigenPlaces download their weights at runtime, so `HOME` points at a cache under `_local/` that survives between runs.

```sh
mkdir -p _local/cache/home
docker run --rm --gpus all --shm-size 512m -v "$PWD:/app" -v "$PWD/_local/cache:/cache" -w /app \
  --user "$(id -u):$(id -g)" -e USER=reconstruction-server -e HOME=/cache/home \
  --entrypoint bash <image> -c '
    cmake -B build -DCMAKE_BUILD_TYPE=Release -DPYBIND11_FINDPYTHON=ON && cmake --build build -j &&
    python3 tools/golden_run.py _local/golden/<job> --out _local/golden/<name>.json'

python3 tools/golden_compare.py _local/golden/baseline.json _local/golden/<name>.json
```

`golden_compare.py` only needs the standard library, so it runs on the host. The stage workspaces and their stdout/stderr logs land next to the summary, in `_local/golden/<name>_work/`.

To summarize something that already ran, such as a Tier A smoke test or extracted job artifacts:

```sh
python3 tools/golden_run.py --summarize-only <dir> --out <name>.json               # <dir>/{local,global,update}/
python3 tools/golden_run.py --summarize-only <job root> --stage global --out <name>.json
```

### Tolerances

GPU feature matching is not bit-exact, so two runs of the same code differ. `golden_tolerances.json` keys are `<stage>:<metric>` or `<metric>`. A metric without an entry must match exactly.

The values come from five golden runs of unmodified `develop` (`bb5a30a`, base image of the same commit) on the update-refinement test job. Each tolerance is about twice the spread across those runs, measured as (max − min) / max:

| Key | Tolerance | Spread over 5 runs | Why it varies |
|---|---|---|---|
| `local:sfm.num_points3D` | 6% | 2.9% | GPU feature matching. Runs cluster around two outcomes (about 66.3k and 64.4k points). |
| `local:sfm.mean_reproj_error` | 1.5% | 0.7% | Same |
| `update:sfm.num_points3D` | 6% | 2.9% | Update merges this run's local output, so it inherits local's spread |
| `update:sfm.mean_reproj_error` | 1.5% | 0.7% | Same |
| `update:sfm.num_reg_images` | 1% | 0.4% (2287 vs 2277) | Carving drops reference images left with fewer than 50 points, which depends on the new scan's points |
| `update:ply.vertices` | 1.2% | 0.6% | Follows the merged points |

Everything else was identical in all five runs and is compared exactly. That includes everything from global, which only reads the provided refined scans, and local's registered image count (all 601 frames).

Re-measure the tolerances if the dataset or the base image changes: run the golden run a few times on the base branch and compare every pair. `golden_compare.py` prints the largest difference it saw for each metric.
