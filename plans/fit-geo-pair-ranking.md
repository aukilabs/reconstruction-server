# Initiative: Fit geo pair ranking

Status: active  
Tier: feature  
Stage: 11-release  
Skips: _(none)_  
Last updated: 2026-09-25

```yaml
h2k:
  initiative: fit-geo-pair-ranking
  phase: integrating
  awaiting_human: true
  plan: plans/fit-geo-pair-ranking.md
```

Living document — update in place when later work changes earlier conclusions.  
**§1 Intent (feature) and §4 System design stay distinct — never merge.**

---

## 1. Intent

- **Customer / internal need:** Cleaner mono-depth fit under loop drift — less wobble on revisited surfaces without a denser SfM pass.
- **Framing:** Geo depth-consistency loss currently optimizes **temporal neighbors only**; wide-baseline / revisit pairs (where drift shows) are under-weighted.
- **Why now:** Human chose pair ranking as first improvement (before denser post-triangulation features).
- **Success look like:** Cost pairs include temporal seed **plus** ranked covis pairs preferring mid triangulation angle and larger time gaps; unit tests lock pair selection behavior; no SfM densify in this initiative.

## 2. Alternatives

| Option | Pros | Cons |
|--------|------|------|
| **A. Ranked temporal+wide (chosen)** | Cheap; targets drift; no SfM change | Needs angle/time heuristics |
| B. Full union (legacy `temporal_only=False`) | Simple | No preference for hard pairs |
| C. Dense second SfM pass | Better anchors | Deferred by human |

- **Decision:** A.

## 3. Priority

- **Decision:** top-of-queue (human directed after mesh UX accept).

## 4. System design

- **Codebase:** `third_party/colmap-monodepth` (`fit_geometry.build_geo_pairs`, `FitConfig`, `fit.py` call site); rec-server only if it overrides FitConfig (it does not today).
- **Behavior:** Default cost mode `temporal_plus_ranked`: keep all temporal pairs; add up to a budget of covis pairs scored by triangulation-angle band × time-gap × shared-count; still respect `geo_max_baseline_m` / `covis_min_shared` / `max_geo_pairs`.
- **No floor plane snap; no denser SfM.**
- **Decision:** design change in monodepth fit pair builder only.

## 5. Verification design

- Unit: `build_geo_pairs` with synthetic centers/obs — temporal-only still works when flagged; ranked mode includes a distant-in-index covis pair over a near temporal-only set when scores warrant; stats expose `cost_mode`.
- Existing `test_fit` / `run_fit` still pass (update expected `cost_mode` for defaults).
- Out of scope: GPU E1 metric re-run; denser SfM.

## 6. Work breakdown

| ID | Slice | Done when |
|----|-------|-----------|
| **A** | FitConfig + `build_geo_pairs` ranking + tests | Ranked mode default; backcompat temporal/union; pytest green |

**Approvals:** Human directed implement (2026-09-25) = mass-approve slice A.

## 7. Implementation notes

- Files: `types.py` FitConfig knobs; `fit_geometry.py`; `fit.py` (pass forwards / optional timestamps if easy); `tests/test_fit.py`.
- Angle via camera centers + forward axes and a proxy scene point (no extra COLMAP I/O).
- Time via optional timestamps else `|i-j| * geo_index_dt_s`.

## 8. Verification record

- **Slice A review:** approve (design §4 met; legacy union now needs explicit `geo_cost_mode="union"`).
- **Slice A verify (§5):** pass — `uv run --extra mesh --extra fit --extra dev pytest tests/test_fit.py` → **5 passed**; temporal-only flag; ranked prefers `(0,7)` over `(0,2)` at budget 1; default `cost_mode=temporal_plus_ranked` in `run_fit` meta.
- **Out of scope skipped:** GPU E1 re-run; denser SfM.

## Gate (stage 11)

- **Decision:** integrate in progress (2026-09-25)  
- **Notes:** Upstream `aukilabs/colmap-monodepth@9d54bc8`; pin + vendored tree updated; rebuild `rec-dev` as `0.4.3-monodepth.6` for human smoke.
