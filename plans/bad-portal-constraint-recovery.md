# Initiative: Bad portal constraint recovery

Status: active  
Tier: feature  
Stage: 6-breakdown  
Skips: _(none)_  
Last updated: 2026-09-11

```yaml
h2k:
  initiative: bad-portal-constraint-recovery
  phase: building
  awaiting_human: true
  plan: plans/bad-portal-constraint-recovery.md
  git_branch: feature/bad-portal-constraint-recovery
  git_base: feature/portal-refinement-metrics
  next_step: "Human re-run job (gravity + cov×1e4 + 3σ); compare tilt vs prior runs"
```

Living document — update in place when later work changes earlier conclusions.  
**§1 Intent (feature) and §4 System design stay distinct — never merge.**

Approvals / decisions log:
- 2026-09-11 — Name `bad-portal-constraint-recovery`; new branch from `feature/portal-refinement-metrics` (not `pose-graph-and-metrics`, not `develop`). Tier: feature.
- 2026-09-11 — §1 Intent locked (human option 1).
- 2026-09-11 — Outlier policy direction: **threshold = median + 2·σ** (sample stdev) on logical edge scores; **no absolute floor** (human). Dry-run notes below.
- 2026-09-11 — Outlier policy: dynamic threshold `median + 2·σ` (no absolute floor); **not** fixed top-k; **not** iterative re-solve loops for v1.
- 2026-09-11 — No max-k cap for now: downweight **all** edges above threshold. Heavy downweight only (never remove). **No connectivity / disconnect checks** — downweight cannot hard-split the scan graph, so skip that complexity.
- 2026-09-11 — Downweight factor: **cov × 1000** on flagged edges (cost pull ÷1000; residual scale ≈ ÷√1000 ≈ ÷31.6). Between mild ×100 and near-ignore ×1e6.
- 2026-09-11 — Score signal locked (human option 1): post-solve per-edge portal pose gaps `Δp` (m) / `Δr` (deg); within-scan `mean_pose` first; flag if either axis `> median+2σ`. Not Ceres residual. Pseudocode in §4.
- 2026-09-11 — Accept second solve iff portal-spread **RMSE** improves (human option 1 + RMSE): job-level `sqrt(mean(position_spread_m²))` and `sqrt(mean(rotation_spread_deg²))` over portals with ≥2 dets; both ≤ first solve and at least one strictly better; else revert.
- 2026-09-11 — Manifest root locked: **`portal_alignment_metrics`**. Contents: per-portal and per-overlap metrics **before and after** the reweight pass, plus explicit list of downweighted edges. (Option 2 depth; name chosen by human.)
- 2026-09-11 — §4 System design locked (human option 1).
- 2026-09-11 — §5 locked: synthetic A/B on existing dataset (corrupt overlaps) with/without reweight, then **required human job**.
- 2026-09-11 — §6 slices S1–S5 locked; **mass-approve all remaining slices**.
- Bounce (2026-09-11): §1 success wording dropped “flag if downweight would leave scans non-overlapping” — superseded by downweight-only + no connectivity gates.
- Bounce (2026-09-11, live job `8875974d`): **Accept metric invalid.** Full portal-spread RMSE (incl. downweighted / bad detections) rejected a useful reweight: pos improved a lot, rot looked slightly worse because incorrect detections (QR flip / floor-glue tilt on `UW8RNQIJHGX`) diverged further once de-trusted — exactly the outliers we downweighted. Reopen §4 accept rule (+ §5 accept checks); S6 needs a fix slice.
- 2026-09-11 — Accept metric re-locked (human option 3 / clarified): **overlap RMSE excluding the downweighted** — gate on trusted-overlap `Δp`/`Δr` RMSE only; still **log full** portal spreads / all overlaps before+after in `portal_alignment_metrics`. Dual-axis non-worse + ≥1 strictly better on trusted set.
- Bounce (2026-09-11, post-S6 live): Trusted dual-axis still rejects useful reweight when **rot RMSE worsens slightly** (e.g. few ° → ~3°) while absolute value remains healthy and position improves. Reopen §4 accept rule; strict non-worse on both axes too brittle.
- 2026-09-11 — Accept comparison re-locked: on **trusted-overlap** RMSE, **reject only if either axis is catastrophically worse** — `after ≥ 2× before` **or** absolute worsen ≥ **0.20 m** (pos) / ≥ **5°** (rot). Otherwise accept (small worsen OK). No “must improve” requirement.
- 2026-09-12 — Live `b3a19ac2`: S7 accept worked (`accepted=true`) but graph still tilted; hub bad-det rot (UW8 ~21°) still pulls via residual cov×1000. Human option **2**: experiment **cov×1e6** + **median+3σ** (fewer, near-ignored outliers). Not §4 lock until A/B vs baseline.
- 2026-09-12 — Stitch diagnostic: tilt seeded after hub placement; reweight cannot restore up. Human: port **gravity prior** from `origin/feature/gravity-prior` (`90dd0e5`) into global refine — trust local-refine up (COLMAP −X) with soft loss.
- 2026-09-12 — Live knobs: keep **cov×10000** + **median+3σ** (with gravity prior).
- 2026-09-12 — Gravity loss: **Huber(1.0)** not Cauchy(0.1); firm below ~3° tilt.

---

## 1. Intent

- **Customer / internal need:** Operators whose raw portal data is wrong in a few places (duplicate prints, moved portals, wrong physical size, bad detections). Today global Ceres still “succeeds” but smudges the error across the graph so the whole result is tilted/skewed.
- **Framing:** After the normal post-global Ceres optimize, detect outlier **portal overlap constraints** (scanA↔scanB via portalID) via a dynamic residual threshold, heavily downweight them (not remove), re-run the optimizer, keep the new solution only if error metrics improve, and surface which edges were tweaked plus final metrics (per portal and per scan) in the manifest / errors.
- **Why now:** Alignment error metrics (`feature/portal-refinement-metrics`) make the bad cases visible; next step is automatic recovery that does not degrade normal jobs (no global Cauchy).
- **Success look like:** Jobs with a few bad portal-pair constraints recover a better global fit; manifest shows final metrics + a dedicated record of downweighted edges with before/after human-readable errors; healthy jobs (no edges above threshold, or no metric improvement) keep the first solve unchanged.
- **Out of scope (v1):** Applying Cauchy/loss to all constraints; per-portal wholesale drop (prefer per-constraint/pair); removing edges; top-k / max-k caps; connectivity / disconnect gating; DMT UI changes (manifest/error contract only unless decided otherwise); local BA / pose-graph changes.
- **§1 status:** locked (2026-09-11); soft-bounce on disconnect-flag wording (2026-09-11)

## 2. Alternatives

- **Existing ways:** Manual re-scan / fix portals; ignore bad jobs; offline log digging via `print_alignment_comparison`; pre-job app metrics only. Global refine uses hard L2 (`loss=None`) so outliers pull the whole graph. Local BA Huber is unrelated.
- **Comparison:** Manual ops does not scale to 100+ scans. Global Cauchy would change every job and can hide good constraints. Per-portal drop is coarser than per-pair and can nuke all overlaps for a QR. Targeted downweight + accept-if-better matches Intent and keeps normal jobs unchanged when no outliers fire. Top-k / remove / connectivity checks deferred — downweight-only is simpler and cannot hard-disconnect.
- **Decision:** proceed — build recovery pass on global refine; do not ship global robust loss, silent edge deletion, top-k, or connectivity gates in v1.

## 3. Priority

- **Rank vs current backlog:** Follow-on to portal-refinement-metrics (integrating). Pose-graph / MrNeRF / CUDA tracks are separate; this is the natural next alignment-quality step once metrics land.
- **Capacity / opportunity cost:** Contained to global `refine_alignment` + manifest metadata; small Python surface; blocks trust in “successful” global jobs with dirty portal input.
- **Decision:** top-of-queue for alignment-quality work on reconstruction-server (after metrics base branch).

## 4. System design

_(locked 2026-09-11; accept comparison re-locked after S6 live bounce)_

- **Products / codebases impacted:** `reconstruction-server` global path only (`utils/scan_alignment.py`, `global_main.py`, `utils/data_utils.py` manifest writer). No local BA / pose-graph; no DMT UI.
- **Constraints:** Keep `loss=None` globally (no Cauchy). Downweight ≠ remove. No connectivity checks. No top-k / max-k. Gate on **trusted-overlap** RMSE. Prefer additive manifest fields.
- **Score / flag / downweight (live knobs 2026-09-12):** Δp/Δr, **median+3σ**, **cov×10000**, second solve. Gravity prior always on (see below).
- **Gravity prior (2026-09-12, from `feature/gravity-prior`):** Per-scan soft prior in global Ceres: `GravityDirectionPriorSim3DeltaCostFunction(R_stitch, local_g=−X, w=20)` + **`HuberLoss(1.0)`** (~firm below ~3° tilt) on Sim3 delta quat. Cherry-picked local BA `GravityDirectionPriorCostFunction` + flag `use_arkit_gravityprior` (default off). Global prior always on.
- **Accept metric (locked — worsen tolerance):** Population = trusted overlaps (exclude `downweighted_edges`). Compare trusted RMSE before→after:
  ```
  axis_reject(before, after, abs_max):
    if after <= before: return false          # improved or equal → ok
    if before > 0 and after >= 2.0 * before: return true
    if (after - before) >= abs_max: return true
    return false
  reject if axis_reject(pos, 0.20 m) OR axis_reject(rot, 5.0 deg)
  else accept
  ```
  Small worsen (e.g. 1.5°→3°) stays accepted. No separate “≥1 axis must improve” clause. Still log full portal spreads + `rmse_trusted_*`.
- **Decision:** accept = trusted RMSE without catastrophic worsen on either axis.

## 5. Verification design

_(updated after worsen-tolerance bounce)_

1. Unit: accept when trusted rot worsens mildly (e.g. 1.5°→3°, under 2× and &lt;5° Δ); reject when rot ≥2× or Δ≥5°; same for pos with 0.20 m.
2. Prior scoring/flagging/cov + trusted-population unit tests still pass.
3. Re-run S4 A/B script (corrupt path should still accept).
4. Offline / live: prior reject cases with mild rot worsen should accept under new rule.
5. Redeploy + human job confirms `accepted=true` when appropriate.

## 6. Work breakdown (AI-oriented)

| # | Slice | Scope | Human after verify? |
|---|-------|-------|---------------------|
| S1–S5 | _(done / S5 exposed flaw)_ | prior work | — |
| S6 | `accept-trusted-overlap-rmse` | Gate on trusted-overlap RMSE; keep full spreads; trusted RMSE fields | done (gate too strict) |
| **S7** | `accept-worsen-tolerance` | Reject only if trusted axis ≥2× worse **or** Δ≥0.20 m / ≥5°; unit + A/B; redeploy | **yes** (re-job) |

**Approvals:** S1–S6 done earlier; **S7 mass-approved with all remaining** (2026-09-11, human option 2).

## 7. Implementation notes

- Base: `feature/portal-refinement-metrics`. Prefer docker/local fixture loops for Python-only edits.
- Fixture candidate: `_local/job-artifacts/20260904-214520-job_50ca9b6a-dcc2-4bf6-a278-439b1ffea8ff-complete` (4 scans); use larger if available.

## 8. Verification record

### S4 `synthetic-ab-fixture` (2026-09-11)

**Fixture:** `_local/job-artifacts/20260904-214520-job_50ca9b6a-dcc2-4bf6-a278-439b1ffea8ff-complete` (4 scans).

**Command:**

```bash
docker run --rm -v "$PWD:/app" -w /app -e PYTHONPATH=/app \
  --entrypoint python3 reconstruction-node:local \
  tests/validate_portal_constraint_reweight_ab.py
```

**Portal-spread RMSE** (`position_m`, `rotation_deg`):

| Scenario | +reweight | −reweight |
|----------|-----------|-----------|
| clean | 0.00549, 0.122 | 0.00549, 0.122 |
| corrupt | 0.0243, 0.129 | 0.0294, 0.131 |

**Corrupt path setup:** two phantom scans (copies of `2026-04-02_13-42-53`) added so median+2σ has enough low-gap edges; aligned constraint on `2026-04-02_13-41-55` / `HBORRXLUOD6` skewed +2.0 m on X (raw detections unchanged).

**Reweight on corrupt:** `applied=true`, `accepted=true`; downweighted edge `2026-04-02_13-40-15↔2026-04-02_13-41-55` / `HBORRXLUOD6` (Δp≈0.098 m).

**Checks:** healthy unchanged; corrupt −reweight worse than clean; corrupt +reweight better than corrupt −reweight; edges flagged.

**Discovery:** raw `portals.csv`-style translation corruption on this 4-scan / 6-edge graph does not trigger median+2σ after the first Ceres solve (gaps scale uniformly). Phantom low-gap edges + aligned-constraint skew needed for integration A/B; real bad-job validation remains S5.

### S5 live job `job_8875974d-75a3-4429-8e70-2f998be5634b` (2026-09-11 ~17:50Z)

- Domain `da74b7f9-1765-4cfa-9746-86f121866440`; **41 scans**, 47 portals, 401 overlaps; job completed/uploaded.
- `portal_alignment_metrics`: **applied=true**, **accepted=false**, `cov_scale=1000`.
- Flagged **23** edges; **all** involve hub scan `2026-09-10_12-03-25` (portals `94EY6USWYKG`×7, `H31M7RC020E`×5, `UW8RNQIJHGX`×5, `EBHY8R2YDQ8`×4, `AVMTK6M6OGU`×2).
- RMSE before → after (reweight solve, **not kept**): pos **0.097 → 0.056 m** (−43%); rot **2.92 → 3.11°** (+6.7%) → dual-axis accept **reverted**.
- Final kept spreads still bad: `UW8RNQIJHGX` 38.5 cm / 18.0°; `94EY6USWYKG` 28.4 cm / 3.4°.
- Discarded reweight would cut position hard (UW8 38.5→9.0 cm) but worsen UW8 rotation (18→21°) and some overlap Δr (~32→42°).

**Interpretation:** Correctly fingered one bad scan; full-spread accept gate blocked a useful fix (bad dets’ rot inflated after downweight). Accept rule bounced → trusted-overlap gate (option 3).

### S6 `accept-trusted-overlap-rmse` (2026-09-11)

**Code:** Gate on `compute_overlap_rmse(..., exclude_keys=downweighted)`; additive `rmse_trusted_before` / `rmse_trusted_after`; full portal-spread RMSE still logged.

**Checks:**
- Unit (docker `tests/test_portal_constraint_reweight.py`): **23/23** — trusted accept/revert cases pass.
- A/B (docker `validate_portal_constraint_reweight_ab.py`): all 6 checks PASS; corrupt +reweight `accepted=true`.
- Offline `8875974d` overlaps: trusted RMSE pos 0.116→0.096 m, rot 1.53→0.87° → **would accept**; old full-spread gate correctly False.
- Review: approve. Verifier: pass (slice done). Redeploy + live re-job: **pending human**.

### S7 `accept-worsen-tolerance` (2026-09-11)

**Code:** `accept_portal_spread_rmse` rejects only if either trusted axis worsens with `after ≥ 2× before` **or** Δ ≥ 0.20 m / 5°; equal/mild worsen accepts.

**Checks:**
- Unit: **28/28** — mild rot/pos accept; 2× and absolute reject; equal accepts.
- A/B: 6/6 PASS; corrupt +reweight `accepted=true`.
- Offline `8875974d` mild rot worsen (2.92→3.11°) → accept under new rule.
- Review: approve. Verifier: pass. Redeploy + live re-job: **pending human**.

**Redeploy (2026-09-11 ~21:00Z):** `docker cp` of `utils/scan_alignment.py`, `utils/data_utils.py`, `global_main.py` into running `rec-dev` (`reconstruction-node:local`); `docker restart`; DDS `registration.success` on `dds.dev.aukiverse.com`. Container confirms S7 markers (`_PORTAL_ACCEPT_WORSEN_RATIO`, trusted gate at L809).

### Live experiment `job_581372d2` — cov×1e6 + median+3σ (2026-09-12 ~07:05Z)

Vs baseline `b3a19ac2` (cov×1000 + median+2σ), same domain / inputs:

| | b3a19ac2 | 581372d2 |
|--|----------|----------|
| cov_scale | 1000 | **1e6** |
| flagged edges | 23 | **14** (subset of the 23; 0 new) |
| accepted | true | true |
| trusted RMSE after | 0.096 m / 0.87° | 0.096 m / 0.88° |
| full-spread after | 0.056 m / 3.11° | 0.055 m / 3.13° |
| UW8 after | 9.0 cm / **21.1°** | 8.0 cm / **21.2°** |

Hub scan `2026-09-10_12-03-25` still in every flagged edge. 3σ dropped all `H31M7RC020E` / `AVMTK6M6OGU` edges and 2 of 4 `EBHY…` — kept all 7 `94EY…` + 5 `UW8…`. Near-ignore did **not** reduce UW8 rotation disagreement; graph still tilted. Stronger downweight alone is the wrong lever for residual hub-detection rot.

### Stitch-vs-refine tilt diagnostic (2026-09-12, offline on `581372d2` inputs)

Hypothesis (“already tilted at basic stitch; nothing recovers”) — **mostly confirmed for the seed, partially wrong for recovery:**

| Stage | portal-spread RMSE | UW8 | mean Sim3 world-down tilt before hub / after hub | worst scan tilt |
|-------|-------------------|-----|--------------------------------------------------|-----------------|
| **Stitch only** | 0.232 m / 3.45° | 41 cm / **22.0°** | **0.00° / 4.47°** | `12-04-15` **24.4°** |
| First Ceres (manifest before) | 0.097 m / 2.92° | 38 cm / 18.0° | — | — |
| Reweight accepted | 0.055 m / 3.13° | 8 cm / **21.2°** | **2.82° / 3.51°** | `12-04-15` **15.3°** |

- Origin floor QR `FG8WUWBCO4W` on first scan is fine; hub itself stitches at **0°** tilt.
- Tilt enters **immediately after hub** is placed into `placed_portal_poses` (bad hub dets poison subsequent `mean_pose` snaps).
- Ceres **does** recover a lot of position (23→5.5 cm) and cuts the worst scan from 24→15°, but also **smears ~2.8° lean into early scans that were upright** (no absolute gauge / floor prior in the solve — only relative QR pairs).
- Post-solve edge downweight cannot restore world-up once stitch + relative L2 have locked a shared lean; need stitch-time quarantine or detection-level drop.

### Gravity prior port (2026-09-12)

- Cherry-picked `90dd0e5` (`GravityDirectionPriorCostFunction` + local BA flag); resolved `bundle_adjuster.py` import conflict; guarded missing `arkit_precomputed`.
- Added **global** `GravityDirectionPriorSim3DeltaCostFunction` (Sim3 delta ∘ stitch → world gravity); wired in `_run_ceres_alignment_solve` with weight **20** + **Cauchy(0.1)**; rebuilt `.so` in `rec-dev`.
- Reverted experiment knobs temporarily during gravity bring-up; human then locked live knobs to **cov×10000 + median+3σ** (with gravity).
- Offline on `581372d2` inputs (gravity + then-baseline cov): mean Sim3 world-down tilt **3.1° → 0.45°**; before-hub **2.8° → 0.0°**; after-hub mean **3.5° → 1.0°**. Worst scan `12-04-15` still **~18°**. UW8 rot still ~21° (detection disagreement). Reweight accepted.

### Live `job_3fbe173c` — gravity + cov×1e4 + 3σ + Huber(1) (2026-09-12 ~08:06Z)

- `accepted=true`, `cov_scale=10000`, **14** downweighted edges.
- Gravity logs (1st solve → after; then reweight pass):
  - pre: tilt mean **1.96°** max **24.4°** p90 5.8°; huber_cost 42.3 → **0.1%** of Ceres initial (~31k)
  - post 1st: tilt mean **1.05°** max **8.6°** p90 2.1°; huber 13.7 (**0.0%** of ~18.6k)
  - post reweight: tilt mean **0.50°** max **17.8°** p90 **0.08°**; huber 11.4 (**0.2%** of ~2.8k)
- Trusted RMSE after: 0.122 m / 0.88° (similar to prior accepts). UW8 after still ~21° rot.
- Interpretation: gravity is tiny vs portal residual mass but enough to kill the **shared lean** (p90→0.08°); one scan (`12-04-15`) remains badly tilted; UW8 detection disagreement unchanged.

## Gate (stage 11)

- **Decision:** _(pending human visual check)_  
- **Notes:**
