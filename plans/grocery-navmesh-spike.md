# Initiative: Grocery navmesh spike

Status: active  
Tier: spike  
Stage: 11-gate  
Skips: 10-integrate (spike — offline scripts only); 11-gate until human accept  
Last updated: 2026-10-16

```yaml
h2k:
  initiative: grocery-navmesh-spike
  phase: building
  awaiting_human: false
  plan: plans/grocery-navmesh-spike.md
```

Living document — update in place when later work changes earlier conclusions.  
**§1 Intent (feature) and §4 System design stay distinct — never merge.**

---

## 1. Intent

- **Customer / internal need:** Walkable floor / nav surface for grocery domains from dense refine meshes — aisles have holes; shelves/walls must not be filled through.
- **Framing:** Offline spike: extract a constrained walkable mask (and optional flat mesh) from the locked gallery global TSDF + camera trajectory; prove fill policy before any recon-server product wiring.
- **Why now:** Global mono-depth TSDF is good enough to try; aisle holes and false wall-gaps are the next blocker for navigation / layout use.
- **Success look like:** On grocery-small stronger-carve global mesh, overlays show trajectory seeds, wall blockers, filled aisle holes that stop at shelves/walls; human can accept or bounce the fill policy. **Not** a shipped domain artifact.
- **Time-box:** ~1–2 sessions; stop when overlays are reviewable or a clear design bounce is needed.
- **Locked input:** `posemesh-harness/tmp/mono_depth_ux_v2/meshes/da3_global_stronger_carve_colored_nofloaters.ply` + `traj_global_aligned.json` (Y-up). Offline stronger carve (−0.08), not production BEST (−0.05).

## 2. Alternatives

| Option | Pros | Cons |
|--------|------|------|
| **A. Offline spike first (chosen)** | Fast visual feedback; no upload contract | Not productized |
| B. Wire into recon-server now | Path to domain artifact | Premature before fill policy |
| C. Library-first in colmap-monodepth | Matches mesh boundary | Needs stable API before spike learns knobs |

- **Decision:** **A**.

## 3. Priority

- **Decision:** top-of-queue for this chat (human directed after mesh gallery review).

## 4. System design

- **Products / codebases:** Spike scripts only under `tmp/grocery_navmesh_spike/` in reconstruction-server (or harness tmp if needed). **No** recon-server runner / Rust upload / pin changes in this spike.
- **Approach:** Trajectory-seeded top-down heightfield + wall blockers + constrained flood fill; optional floor-triangle normal filter and ~20 cm layer snap.
- **Inputs:** Locked PLY + traj JSON above.
- **Outputs (spike):** PNG overlays (seeds / blockers / evidence / walkable), optional `nav_floor.ply` / mask NPZ, short `README.md` / `summary.json`.
- **Constraints:** Y-up; do not expand through vertical near-floor barriers; cap distance from floor evidence for invented cells.
- **Risks:** Unscanned walls with no remnant may still leak; fill knobs need human tune.
- **Decision:** spike first — **no system-design change** to production refine yet.

## 5. Verification design

- Script runs end-to-end on locked mesh+traj without error.
- Writes overlay PNGs + summary counts (seed cells, blocker cells, filled cells).
- Human eyeball: aisle centers filled; no obvious walk-through of shelf runs / outer walls.
- Out of scope (original): multi-domain, production upload. **In scope later in spike:** Recast bake + viewer (added 2026-10-16).

## 6. Work breakdown

| ID | Slice | Done when |
|----|-------|-----------|
| **S1** | Offline pipeline + overlays on locked mesh | Script + overlays + summary; human can review at `/meshes` or local out dir |
| **S2** (optional) | Knob sweep / gallery hook if S1 policy looks promising | Only if human asks after S1 |

- **Human:** Accept/bounce fill policy from overlays; choose whether to productize later.
- **Agents:** Implement S1; do not touch production mono-depth defaults.
- **Stop / escalate:** If fill cannot avoid wall leaks without new sensors/semantics → bounce §4.

## 7. Implementation notes

- Approvals: human chose offline spike + mesh **2** + start (**1**) → mass-approve **S1**.
- Key knobs (initial): grid ~5 cm; up-normal ~30°; floor band ±20 cm around Y=0 or RANSAC; seed dilate ~0.5 m; max invent distance ~1.5–2 m; wall normal near-horizontal exclusion.
- Bounce → update §4 if heightfield approach fails.

## 8. Verification record

- **S1 run (2026-10-15):** overlays written but **wrong view** — human: “top-down is clearly a side view.”
- **Bounce:** Gallery PLY is still COLMAP **+X-up** on disk; `traj_global_aligned` is already Y-up via `(x,y,z)→(−y,x,z)`. Three.js remaps at display; spike did not → XZ grid was an elevation on height×Z.
- **S1b (2026-10-16):** Remap mesh when traj is Y-up; fix PNG so width=X, height=Z. Re-ran. `mesh_remapped_x_up_to_y_up: true`, snap_floor_y **0.0**, floor tris ~166k / evidence ~48k / walkable ~123k. Gallery: `https://robin-ny1.tail2ecdfd.ts.net/meshes/navmesh_spike/`
- **Human:** re-eyeball true top-down (§5).
- **S2 (2026-10-16):** Dropped invent flood (leaked through walls). Walkable = evidence + **enclosed** holes only if small (≤2.5 m² / ≤2 m extent), rim plane up-facing (≤25°), planar RMS ≤6 cm; never fill border-touching exterior. Result: holes_filled 179 cells, walkable ~49k (≈ evidence). Gallery refreshed.
- **S2b preprocess:** `filter_smooth_simple` ×2 before normals; drop blocker CCs &lt;16 cells. Blockers 5012→4012; holes_filled 289. Outliers = TSDF vertical micro-facets / shelf-lip stamps / vertex scatter speckles, not continuous wall runs.
- **S2c:** For hole fill / hard blockers, ignore stamps within **0.3 m XZ** of camera traj and **|y − cam_y| ≤ 1.5 m** (feet / nearby people). Ignored 169 cells; hard blockers 3843.
- **S2d:** Open **traj corridor gaps** (not closed rings): same planar/up gates; corridor 0.75 m; within 1.5 m of evidence; rim on ≥2 bbox sides. Filled 2761 cells / 17 gaps.
- **S2e:** Tighten gaps: max **1 m width**, ≤0.5 m to nearest evidence, **opposite-side** evidence required (no one-sided void expand). Filled 1329 cells / 13 gaps.
- **S2f:** Opposite rim is **PCA across-gap / angle wedges**, not voxel-box left-right.
- **S3:** Contiguous floor mesh from heightfield → `nav_floor.ply`.
- **S4 (2026-10-16):** Thin enclosed pre-mesh holes (&lt;4 cells, ≤1 cell wide); same-surface median flatten; Recast (`recast-navigation`, cs/ch 0.05 m, agent radius 0.10 m); post-Recast median plane snap on viewer PLY (hard ≤5 cm, smoothstep to 15 cm); 3D viewer with TSDF overlay + trajectories. Scripts under `tmp/grocery_navmesh_spike/`; gallery mirror `posemesh-harness/.../meshes/navmesh_spike/`.

## Gate (stage 11)

- **Decision:** deferred (human accept of floor + Recast + viewer)
- **Notes:** Depth-edge snap / carve max-depth changes are a **separate** colmap-monodepth initiative — not this commit.
