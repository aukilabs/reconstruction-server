# Initiative: Mono depth mesh UX v2 (rec-server)

Status: integrating  
Tier: feature  
Stage: 10-integrate  
Skips: _(none)_  
Last updated: 2026-10-09

```yaml
h2k:
  initiative: mono-depth-mesh-ux-v2
  phase: integrating
  awaiting_human: true
  plan: plans/mono-depth-mesh-ux-v2.md
```

Living document — continuation of **mono-depth-mesh-refine** (done).  
**§1 Intent (feature) and §4 System design stay distinct — never merge.**

### Bounce / lineage

- Parent: `plans/mono-depth-mesh-refine.md` (stage 11 accept).
- Offline grocery tune (2026-10-08) on scan `11-41-36` locked mesh knobs in **`aukilabs/colmap-monodepth@d64d5df`**: colored TSDF, `tsdf_mesh_raw.ply` + `tsdf_mesh.ply`, milder carve, postprocess without Taubin (0.1 mm / ratio 0.2).
- Rec-server still pinned monodepth `@9d54bc8` and called `run_mesh` with default Carve/Tsdf configs (global fuse forced `nocolor` only).
- **2026-10-08 bounce:** Human: pin latest monodepth (`08e5815`); local upload = colored mesh **without** decimation; global = decimated lower-detail **uncolored**. Confirm: local `postprocess=False` so primary `tsdf_mesh.ply` is undeciimated colored.

---

## 1. Intent

- **Customer / internal need:** Domain consumers get a dense colored local surface (no decimation), and a lighter uncolored global mesh for topology / overview.
- **Framing:** Wire locked monodepth UX into reconstruction-server: bump pin to latest master, local TSDF without postprocess, global TSDF with postprocess + nocolor; verify on dedicated DEV grocery-small.
- **Why now:** Upstream locked grocery carve/TSDF defaults + Delaunay helpers just landed; local product mesh should not be the decimated postprocess output.
- **Success look like:**
  - Pin + vendored tree at `08e5815`.
  - Local refine (`MONO_DEPTH_MESH=1`) writes colored `mesh/tsdf_mesh.ply` with **postprocess off** (no decimation); that is the primary domain upload.
  - Global fuse writes **nocolor** `tsdf_mesh.ply` with **postprocess on** (decimated); soft-fail unchanged.
  - Dedicated DEV node only running; full grocery-small 4-scan recon after deleting prior local mesh artifacts + refined_scan zips.
- **Locked decisions:**
  - Carve from **fitted** depths via `run_mesh` (library defaults = grocery BEST after `0259608`).
  - Local: `postprocess=False`, `color_type=rgb8`.
  - Global: `postprocess=True`, `color_type=nocolor`, same voxel/trunc.
  - Optional raw artifact may still exist when postprocess runs (global); local primary is undeciimated `tsdf_mesh.ply`.

### Bounce log

- **2026-10-08:** Human flagged committed monodepth carve/TSDF defaults still old (−0.2 / pts3 / trunc 0.16) and confident gate worse than BEST. Bounce §1/§4: carve **fitted**; library defaults = BEST (`sub=-0.05`, pts2, max_depth5, strides 2/1, `sdf_trunc=0.40`).
- **2026-10-08 (evening):** Pin → `08e5815`; local mesh upload = colored **no decimation** (`postprocess=False`); global = decimated + uncolored. Auto mode continues.

## 2. Alternatives

| Option | Pros | Cons |
|--------|------|------|
| **A. Local postprocess off; global postprocess on + nocolor (chosen)** | Matches product ask; simple config flip | Local mesh files larger |
| B. Keep postprocess; upload raw as primary | Still writes decimated local file | Confusing dual artifacts |
| C. Pin only | Minimal | Misses upload semantics |

- **Decision:** **A** (AskQuestion confirmed).

## 3. Priority

- **Rank:** Continuation of shipped mesh work; insert now.
- **Decision:** proceed.

## 4. System design

| Repo | Change |
|------|--------|
| **colmap-monodepth** | Consumed at `08e5815` (grocery-locked defaults @ `0259608` + Delaunay helpers). No further upstream change in this bounce. |
| **reconstruction-server** | Bump pin + sync `third_party/colmap-monodepth`; `locked_local_tsdf_config` → `postprocess=False` / rgb8; `locked_global_tsdf_config` → `postprocess=True` / nocolor; tests + docs; rebuild dedicated image; clear domain caches; grocery-small full recon. |

### Locked knobs

**Carve (override; matches library after `0259608`):**  
`voxel_size=0.07`, `subtractive_value=-0.05`, `min_confidence=2`, `min_points=2`, `max_depth=5`, `mask_max_depth=5`, `unproject_stride=2`, `mask_stride=1`.

**TSDF local:** `voxel_length=0.04`, `sdf_trunc=0.40`, `color_type=rgb8`, **`postprocess=False`**.

**TSDF global fuse:** same voxel/trunc; `color_type=nocolor`, **`postprocess=True`** (decimate floor ratio 0.2 / 0.1 mm).

## 5. Verification

- Unit: `test_mono_depth_mesh` asserts local `postprocess=False` / rgb8 and global `postprocess=True` / nocolor; pin SHA string matches `08e5815`.
- Soft-fail behavior unchanged.
- Ops: build `reconstruction-node:0.4.3-monodepth.8` (or next); run **only** dedicated node with `posemesh-harness/reconstruction-node.env` + `MONO_DEPTH_MESH=1`; delete grocery-small local mesh artifacts + `refined_scan_zip`; dedicated full send all 4 scans; job completes with colored local meshes and uncolored global mesh.

## 6. Work breakdown

| ID | Slice | Status |
|----|-------|--------|
| S1 | Bump monodepth pin + sync third_party to `d64d5df` | done |
| S2 | Pass locked Carve/Tsdf from `mono_depth_mesh` (local rgb8, global nocolor) + tests | done |
| S3 | Register/upload `tsdf_mesh_raw.ply` (local + global optional artifacts) | done |
| S4 | Docs (`development.md`) | done |
| S5 | Bump pin + sync third_party to `08e5815` | done |
| S6 | Local `postprocess=False`; global decimated nocolor; tests + docs | done |
| S7 | Build/run dedicated-only node; clear meshes+zips; grocery-small 4-scan verify | done |
| S8 | Wire `MONO_DEPTH_MESH_METHOD=delaunay` into `mono_depth_mesh` + tests/docs | done |
| S9 | Create `grocery small delaunay` + copy 4 DMT scans (leave grocery small untouched) | done |
| S10 | Rebuild dedicated image; Delaunay job on new domain | done |

## 7. Progress / Auto mode

- **Auto mode: on** (continues; mass-approve remaining including S8–S10).
- Deferred / assumptions logged:
  - Carve stays on **fitted** via `run_mesh` for **da3**.
  - Local may still write `tsdf_mesh_raw.ply` depending on upstream when postprocess is off; primary upload remains `tsdf_mesh.ply`.
  - Delaunay via `MONO_DEPTH_MESH_METHOD=delaunay` (fit/carve off per library defaults); side-by-side on new domain so grocery-small mono-depth meshes stay.
  - Image tag: `0.4.3-monodepth.8` → next `.9` for S10.
  - AskQuestion 2026-10-08: local postprocess off (Recommended).
  - AskQuestion 2026-10-09: parallel domain `grocery small delaunay` (not purge grocery small).
- **S7 verify (2026-10-08):** dedicated-only `harness-recon-dedicated` / `0.4.3-monodepth.8`; cleared zips+meshes; job `job_4f1c7749` complete on grocery-small. Local mesh vertex colors on; global no colors; global raw→post ~38MB→8.3MB. Evidence under `posemesh-harness/tmp/mono_depth_ux_v2/`.
- **S9 domain (2026-10-09):** `grocery small delaunay` UUID `407f1d6b-6f0c-41a0-b1cf-0510937bb474`.
- **S10 verify (2026-10-09):** image `0.4.3-monodepth.9`; `METHOD=delaunay`; `job_cadf5b8b` + global rerun `job_e8caa493` complete (`mesh_total=6`). Gallery side-by-side at `http://100.124.160.70:8771/` (`delaunay_*` + DA3). Evidence `posemesh-harness/tmp/mono_depth_delaunay/`. Board S10 → done; initiative integrating — human mesh review + commit/PR gate.
