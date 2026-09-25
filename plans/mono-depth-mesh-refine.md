# Initiative: Mono depth mesh in refine

Status: done  
Tier: feature  
Stage: 11-assess  
Skips: _(none)_  
Last updated: 2026-09-25

```yaml
h2k:
  initiative: mono-depth-mesh-refine
  phase: done
  awaiting_human: false
  plan: plans/mono-depth-mesh-refine.md
```

Living document — update in place when later work changes earlier conclusions.  
**§1 Intent (feature) and §4 System design stay distinct — never merge.**

### Bounce log

- **2026-09-22:** Human ruled boundary: generally useful COLMAP-sparse→mesh lives in **`aukilabs/colmap-monodepth`** (push first); reconstruction-server **clones/calls** it with scan-specific params — do **not** reimplement the mesh pipeline in rec-server. Invalidates draft §4 “vendored into rec-server package” and §6 slice 1 “extract into reconstruction-server”. Stage → 4-design to re-lock §4/§6.
- **2026-09-22 (scope):** Full kept E1 chain in first monodepth push; **each stage must be independently callable** with clear file structure. No dedicated “engineering wisdom” skill in plugin — apply staged API + thin orchestrator below.
- **2026-09-25 (mesh UX):** After A–G ship, human wants denser / cleaner product meshes: global **no-color** TSDF promoted to primary `topology.*`; alpha-shape kept as `topology_alphashape.*`; local carve milder voxel; light Taubin + shape-aware decimate on **local + global** TSDF; stride 3; binary `tsdf_points.ply`; TSDF LODs reuse topology downsample helpers. Invalidates §1 “topology remains additive-only / do not replace”, §4 outlier carve voxel 0.05, §5 “out of scope: replacing topology”, and §6 done-at-G. Stage → 1-intent then re-lock §4–§6 for follow-on slices **H–J**.

---

## Evidence from latest runs (intake)

**Kept experiment (2026-09-21):** `D:\tmp\monodepth_compare_2026-09-18\out_exp_e1_carve\`  
Pipeline locked in `EXPERIMENTS.md`:

1. DA3-BASE pose-conditioned depths @ 80 frames (stride 5, process_res 504)
2. Joint scale + residual fit (covis ∪ temporal ∪ track), clamp [0.85, 1.15]
3. ICP rematch + confident temporal mask
4. **E1 keep:** VDA-style subtractive carve (−0.2) + multi-view voxel gate (min_pts≥2, conf≥1) → Open3D `ScalableTSDFVolume` (voxel_length=0.02, sdf_trunc=0.08)

| metric | ICP baseline TSDF | E1 carve→TSDF |
|--------|-------------------|---------------|
| median \|Δz\| | 0.0030 m | **0.0024 m** |
| RMSE | 0.112 m | **0.083 m** |
| ≤2 cm | 0.795 | **0.855** |
| valid depth frac | 0.911 | 0.817 (intentional: empty > wrong) |
| mesh verts | ~40k | ~27k |

**Discarded:** far-cut z>4/6 m (no effect after E1); global free scale in ICP (slightly worse).

**Downstream (today):** `D:\tmp\splat_mesh_init_compare\` — mesh used as splat init; quality vs COLMAP-init still mixed (e.g. Sep-10 PSNR worse for abbrev mesh-init). **This initiative is about shipping mesh artifacts from refine, not splat training.**

**Libraries:**
- Depth inference / COLMAP I/O: `D:\colmap-monodepth` (DA3)
- Carve / voxel gate: `D:\splatter-server\ColmapMonodepth\voxel_grid.py` (imported by experiment scripts)
- Experiment glue: `D:\tmp\monodepth_compare_2026-09-18\run_experiments.py`

**Reconstruction-server today:** local = COLMAP SfM per scan; global = portal align → merge → `RefinedPointCloud.ply` → topology meshes. **No monodepth / TSDF.** Topology is sparse-cloud-derived, not dense surface.

---

## 1. Intent

- **Customer / internal need:** Domain / reconstruction consumers who want a dense surface mesh per scan and a combined domain mesh after multi-scan refine — denser and more complete than sparse COLMAP + topology alone.
- **Framing:** Offline DA3→carve→TSDF already produces usable local meshes from refined SfM poses; wire that into reconstruction-server so local refinement emits a per-scan mesh, and global refinement fuses those into one domain mesh, with outlier cleanup so floaters do not poison the fuse.
- **Why now:** E1 is locked as kept (clear metric + visual win). Splat experiments already consume `tsdf_mesh.ply`. Refine is the natural production hook (poses + images already present).
- **Success look like:**
  - After local refine: `refined/local/<scan>/…` includes a dense mesh (and optionally depths) suitable for download / splat init.
  - After global refine: a single combined dense mesh in `refined/global/…`, uploaded alongside existing point cloud / topology artifacts.
  - Meshes are visibly cleaner than raw TSDF (fewer floaters); empty regions preferred over wrong surface.
- **2026-09-25 mesh UX (locked ask-user):**
  - Primary domain surface consumers should get **dense no-color TSDF** as the main `topology.*` artifacts (with LODs), not only sparse alpha-shape.
  - Alpha-shape topology remains available under **`topology_alphashape.*`** (both kept).
  - Local + global TSDF meshes cleaned with **light Taubin** + **mild shape-aware decimate** (not raw poly-count-only stop).
  - Local carve slightly less aggressive via coarser voxels; denser DA3 sampling via stride **3**.
- **Open:** none for this bounce (consumer = domain topology slots + existing mesh uploads).

## 2. Alternatives

| Option | Pros | Cons |
|--------|------|------|
| **A. Integrate E1 pipeline into rec-server (proposed)** | Uses locked experiment; poses already from local refine; one place for artifacts | GPU + deps (DA3, Open3D); job time ↑ |
| B. Keep offline-only; manual mesh copy | Zero server change | Not productized; no global fuse |
| C. Mesh only in splatter-server | Closer to splat consumers | Duplicates pose/refine context; global multi-scan harder |
| D. Replace topology with TSDF mesh | One mesh story | Topology serves different (low-poly / navigation) use; higher risk |
| E. TSDF as primary topology + keep alpha-shape renamed (2026-09-25) | Dense surface in existing consumer slots; alpha-shape still downloadable | Need dual artifact names + LOD helper reuse |

- **Decision:** **A revised** — E1 mesh pipeline ships in **colmap-monodepth**; rec-server integrates by calling it (not by inlining).
- **2026-09-25:** **E** — TSDF (no-color global) writes primary `topology.*` (+ downsampled LODs via existing helpers); alpha-shape → `topology_alphashape.*`. Topology is no longer “sparse-only additive.”
- **Outlier path:** Prefer **E1 carve + multi-view gate before TSDF** (proven). **Carve voxel_size → 0.07** (human 2026-09-25; milder than locked E1 0.05); keep `subtractive_value=-0.2` (E1 best). Post-TSDF: light Taubin + shape-aware decimate on local and global meshes.

## 3. Priority

- **Rank:** Unranked vs portal / CUDA plans — insert after human confirmation.
- **Capacity:** Feature-sized: Python pipeline module + local/global hooks + Rust artifact registration + dependency packaging.
- **Decision:** pending human — top-of-queue | scheduled | parked.

## 4. System design

### Products / codebases (boundary rule)

| Repo | Owns | Does not own |
|------|------|--------------|
| **`aukilabs/colmap-monodepth`** (`D:\colmap-monodepth`) | Anything generally useful to go from COLMAP sparse → clean dense mesh: DA3 pose-conditioned depth, metric fit (joint residual + ICP rematch), E1 carve + multi-view gate, TSDF → `tsdf_mesh.ply` / depths; CLI + library API with knobs | Domain refine orchestration, portal alignment, artifact upload lists, rec-server job flags |
| **`reconstruction-server`** | Thin caller: clone/pin monodepth, invoke with **scan-specific params** (paths, stride, frame cap, soft/hard fail), place outputs under `refined/…`, register uploads; global fuse may call monodepth (or thin glue) with `alignment_transforms` | Reimplement carve/TSDF/fit |

- **Source of truth for mesh quality:** E1 kept pipeline in `D:\tmp\monodepth_compare_2026-09-18` + `VoxelGrid` from `D:\splatter-server\ColmapMonodepth` (port into colmap-monodepth; do not leave dual maintenance).
- **Not in v1:** LichtFeld / splat training changes.

### Proposed pipeline

```
┌─ colmap-monodepth (library + CLI) ─────────────────────────┐
│  COLMAP sparse + images                                    │
│    → DA3 depths → joint residual + ICP rematch             │
│    → E1 carve + multi-view gate → TSDF                     │
│    → depth/*.png, tsdf_mesh.ply, tsdf_points.ply, meta.json│
└────────────────────────────────────────────────────────────┘
         ▲ called with params
         │
Local refine SfM ──► mono_depth_mesh_local (rec-server glue)
         │              writes refined/local/<scan>/mesh/
         ▼
Global refine ──► mono_depth_mesh_global (rec-server glue)
         │         transforms poses/depths via alignment_transforms
         │         preferably re-invokes monodepth fuse / TSDF API
         ▼
      refined/global/mesh/tsdf_mesh.ply
```

### colmap-monodepth deliverable (push first)

**Principle:** one stage ≈ one module; typed configs in / out; `pipeline` only wires stages; CLI subcommands mirror stages. No god-script dump of experiment files.

```
colmap_monodepth/
  colmap_io.py      # (existing) COLMAP → poses/K/paths
  inference.py      # (existing) DA3 pose-conditioned depth
  export.py         # (existing) depth PNG + colored PLY helpers
  depth_io.py       # load/save 16-bit mm depth; shared by fit/carve/tsdf
  types.py          # FrameSet, DepthBundle, *Config / *Result dataclasses
  fit.py            # joint scale+residual + ICP rematch + confident mask
  voxel_grid.py     # port VoxelGrid + empty-ray helpers (from old ColmapMonodepth)
  carve.py          # E1 subtractive carve + multi-view gate → masked depths
  tsdf.py           # Open3D ScalableTSDFVolume → tsdf_mesh.ply / tsdf_points.ply
  pipeline.py       # run_infer / run_fit / run_carve / run_tsdf / run_mesh (compose)
  cli.py            # subcommands: infer | fit | carve | tsdf | mesh
```

| Stage API (library) | Input | Output |
|---------------------|-------|--------|
| `run_infer` | COLMAP dir, model/device/stride | `depth/` PNGs (+ optional raw PLY) |
| `run_fit` | COLMAP + raw depths | fitted/masked `depth/` |
| `run_carve` | COLMAP + fitted depths | carved `depth/` + meta |
| `run_tsdf` | COLMAP + images + depths | `tsdf_mesh.ply`, `tsdf_points.ply` |
| `run_mesh` | COLMAP dir + knobs | full E1 chain end-to-end |

- Defaults = locked E1 params; all overridable via config/CLI.
- Optional extras: `da3` (torch), `mesh` (open3d, numba, opencv) so unit tests can import carve/tsdf math without GPU.
- **Push** to `aukilabs/colmap-monodepth` before rec-server pins it.

### reconstruction-server (after monodepth pushed)

- Depend via git clone / pinned commit (prefer explicit SHA in requirements or submodule — decide at §6).
- Local hook after `refine_dataset_part_two`; flag/env; pass colmap dir + out dir + stride/process_res/E1 defaults.
- Global: preferred **depth re-fuse in domain frame** via monodepth APIs + `alignment_transforms`; fallback mesh merge only if needed.
- Soft vs hard fail on mesh step: **soft-fail** (2026-09-22 human) — log + skip mesh; SfM/refine still succeeds. Optional later: hard mode via require-mesh flag.

### Outlier cleanup (ordered) — lives in monodepth

1. **Carve defaults (2026-09-25):** subtractive_value=-0.2 (E1 kept), **voxel_size=0.07** (milder than E1 0.05), min_points=2, min_confidence=1.0 → TSDF 0.02 / 0.08.
2. **Post-extract (local + global):** one light **Taubin** pass (edge-preserving vs Laplacian), then **mild shape-aware / max-error-style decimate** (prefer deformation/error stop over hard poly count; Open3D quadric iterative or max-error API when available).
3. Tighten gate / confident mask / connected-component only if metrics regress.
4. TSDF-only retune last.

### Color / fusion (2026-09-25)

- **Global fuse:** **no-color** TSDF (`TSDFVolumeColorType.NoColor` or equivalent) — geometry-only; do not require Frames/RGB for global mesh success.
- **Local:** may keep RGB8 when images exist (unchanged unless implementer finds shared path forces one mode); global path must not fail without RGB.
- **`tsdf_points.ply`:** write **binary** PLY explicitly (`write_ascii=False`).

### Frame sampling (2026-09-25)

- Rec-server default **`--mono_depth_mesh_stride=3`** (was 5; matches denser sampling vs E1 experiment’s stride 5).

### Interfaces / artifacts

| Stage | Paths | Who writes |
|-------|-------|------------|
| Monodepth CLI/lib | caller-chosen `output-dir` (`tsdf_mesh.ply`, binary `tsdf_points.ply`, meta) | colmap-monodepth |
| Local | `refined/local/<scan>/mesh/tsdf_mesh.ply` (post-cleaned) | rec-server after invoking monodepth |
| Global mesh | `refined/global/mesh/tsdf_mesh.ply` (+ points/meta) | rec-server no-color fuse |
| Global topology (primary) | `refined/global/topology/topology.{obj,glb}` + `topology_downsampled_{0.333,0.111}.{obj,glb}` from **cleaned TSDF** via existing LOD helpers | rec-server |
| Global topology (alpha) | `refined/global/topology/topology_alphashape.{obj,glb}` (+ optional downsampled alphashape if cheap) | existing alpha-shape path, renamed |

### Constraints / risks

- GPU for DA3 on local; global may CPU-TSDF from cached depths.
- Do not fork logic into `splatter-server/ColmapMonodepth` going forward — port what we need into `aukilabs/colmap-monodepth`.
- Mesh-init splat quality still mixed — mesh is a surface artifact, not a splat proof.

### Decision

**Design change needed (bounce 2026-09-25).** A–G remain shipped baseline. Follow-on: no-color global TSDF → primary topology slots; alphashape rename; carve voxel 0.07; stride 3; Taubin + shape-aware decimate local+global; binary points PLY; reuse topology LOD helpers.  
**Locked:** soft-fail on mesh errors (2026-09-22); subtractive −0.2; both topology families kept.

## 5. Verification design

### colmap-monodepth (before / with push)

- **Unit (no GPU):** `pytest` — COLMAP round-trip (existing); carve gates a synthetic multi-view floater (voxel kept set excludes free-space carve); depth_io round-trip mm↔m; `cli --help` / subcommand help exit 0.
- **Stage smoke:** Given fixture COLMAP + precomputed depths, `run_carve` then `run_tsdf` write non-empty `tsdf_mesh.ply` (Open3D; skip if mesh extra missing).
- **E2E (GPU, human/CI optional):** `run_mesh` on 7Scenes chess or known experiment scan; sequential depth median |Δz| not worse than E1 kept by >10% vs ICP baseline when re-run on same selection.
- **API contract:** Each of `run_infer|fit|carve|tsdf|mesh` importable and callable without running the others.
- **2026-09-25 H:** unit/smoke — default `CarveConfig.voxel_size == 0.07`; `run_tsdf` supports no-color path without RGB files; points PLY header is binary; postprocess applies Taubin then reduces triangle count without requiring a fixed target-only API when error stop is available.

### reconstruction-server (after pin)

- **Local:** flag on → `refined/local/<scan>/mesh/tsdf_mesh.ply` exists after refine; flag off → no mesh path / SfM unchanged.
- **Global:** ≥2 local meshes → `refined/global/mesh/tsdf_mesh.ply` in domain frame; no huge floater component vs RefinedPointCloud extent.
- **2026-09-25 I–J:**
  - Default stride **3** in CLI/docs/`DEFAULT_STRIDE`.
  - Global fuse succeeds **without** Frames/RGB (no-color).
  - After global: `topology/topology.{obj,glb}` exist from TSDF when fuse succeeds; `topology_alphashape.{obj,glb}` from alpha-shape; LOD `topology_downsampled_*.{obj,glb}` from TSDF via shared helpers.
  - `output.rs` registers alphashape + keeps topology display names pointing at TSDF primary paths.
- **Out of scope this bounce:** splat PSNR; mobile UX; re-running full E1 metric suite for voxel 0.07 (accept visual/human smoke).

## 6. Work breakdown (AI-oriented)

**A–G complete** (see §8). Follow-on **H → IJ** (one implementer per slice):

| ID | Slice | Repo | Done when |
|----|-------|------|-----------|
| **H** | **Monodepth: carve voxel 0.07, no-color TSDF, binary points, Taubin+shape decimate postprocess** | colmap-monodepth (vendored `third_party` + upstream pin) | `CarveConfig.voxel_size=0.07`; `run_tsdf` color_type selectable (NoColor usable without RGB); explicit binary `tsdf_points.ply`; shared postprocess helper used by local+global extract; tests updated |
| **IJ** | **Rec-server: stride 3 + topology promotion + alphashape rename + LOD helpers + Rust uploads** | rec-server | `DEFAULT_STRIDE`/CLI/docs=3; hooks use new carve/postprocess; alpha-shape → `topology_alphashape.*`; cleaned global no-color TSDF → `topology.*` + downsampled via shared helpers; `output.rs` lists both; docs note dual artifacts |

**Dependencies:** **H before IJ**.

**What stays human:** rebuild/redeploy `rec-dev` image; visual mesh smoke on a real job; optional push to `aukilabs/colmap-monodepth` if not only vendored.

**Human check after verify:** IJ (topology names in domain download / viewer).

**Stop / escalate:** Open3D lacks usable shape-error decimate → implement mild iterative quadric with documented stop heuristic and bounce §4 if human rejects; color API split breaks local RGB consumers → keep dual color modes.

**Slice-shape (2026-09-25):** Human chose merge I into J → **IJ**.

## Approvals

- **2026-09-22:** Mass-approve all remaining slices (A–G).
- **2026-09-25:** Follow-on **H → IJ** (I merged into J). **Mass-approve** remaining (H+IJ).

## 7. Implementation notes

- Key hooks (rec-server): `utils/refinement_util.py` (`refine_dataset_part_two` end), `global_main.py` (after align/merge), `runner-reconstruction-local`, `runner-reconstruction-global/src/output.rs`.
- Reuse params from `out_exp_e1_carve/experiment.json`.
- Source ports: `run_experiments.py` (carve/TSDF), `run_da3_joint_fit.py` / ICP scripts (fit), `splatter-server/ColmapMonodepth/.../voxel_grid.py`.
- Work tree for monodepth slices: `D:\colmap-monodepth` (not in orchestrator allowlist — edit via absolute path; push from that repo).
- Bounce if splat-only consumers want different mesh (decimated / points-only).

### Approvals record

- **2026-09-22:** Mass-approve all remaining slices (A–G). Still one implementer → review → verify per slice; do not re-ask until blocked or done.
- **2026-09-22:** Soft-fail for local (and later global) mesh step — log + skip; refine succeeds.

## 8. Verification record

- **2026-09-22 slice A:** code-review **approve**; behavior-verifier **pass** — `pytest tests/ -v` → 11 passed in colmap-monodepth venv; `run_carve`/`run_tsdf` APIs OK.
- **2026-09-22 slice B:** code-review **approve**; behavior-verifier **pass** — 14 passed; `run_fit` alone OK; default `residual_icp`. Wire **confident/** → carve in C.
- **2026-09-22 slice C:** code-review **approve**; behavior-verifier **pass** — 21 passed; CLI subcommands OK; pushed `4ab26a90ff4cdbcba53818f8d608bb7506ce54ee` on `origin/master` (`https://github.com/aukilabs/colmap-monodepth`).
- **2026-09-23 slice D:** soft-fail locked; code-review **approve**; behavior-verifier **pass** — unittest 7 OK; pin SHA in Dockerfile; flag default off.
- **2026-09-23 slice E:** code-review **approve**; behavior-verifier **pass** (cargo unavailable — code inspection); optional mesh domain artifacts.
- **2026-09-23 slice F:** revise for Sim3 depth×scale; then approve + pass — 15 unittest/pytest OK.
- **2026-09-23 slice G:** code-review **approve**; behavior-verifier **pass** (cargo unavailable); global optional mesh uploads + `docs/development.md` ops note.
- **2026-09-25 slice H:** code-review **approve**; behavior-verifier **pass** — carve voxel 0.07; nocolor TSDF; binary points; Taubin+quadric_max_error; `test_slice_h` 2 passed / 3 skipped (empty synthetic TSDF).
- **2026-09-25 slice IJ:** code-review **approve**; behavior-verifier **pass** — stride 3; global nocolor fuse; TSDF→`topology.*` + LODs; alpha→`topology_alphashape.*`; `output.rs` dual; `pytest tests/test_mono_depth_mesh.py` 16 passed.

**A–G complete** (automated gates). **H–IJ complete** (2026-09-25). Remaining human: rebuild `rec-dev` image; GPU local/global mesh smoke (topology names + denser stride/carve); then `/integrate` + `/assess-release`.

## Gate (stage 11)

- **Decision:** **accept** (2026-09-25 human)  
- **Notes:** H+IJ shipped; log smoke on `job_7c1ca5f5` (stride 3, local+global TSDF, dual topology uploads). Domain SIWE visual download 401 (node≠domain lease) — not a product defect for this bounce.
