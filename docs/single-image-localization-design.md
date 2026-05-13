# Single-image localization — design criteria

This note captures product and engineering criteria for localizing individual frames against an existing COLMAP reconstruction (approximate pose in, refined pose out), including downstream use in **pose refinement**.

## Trust and coverage (primary criterion)

**We do not require every localized frame to achieve high geometric accuracy.**

The bar that matters for practical use—especially when feeding poses into refinement pipelines—is **selective trust**:

- It must be possible to **confidently identify** queries whose localization is weak, ambiguous, or inconsistent.
- Those queries should be **ignorable or down-weighted** (skipped, flagged, or excluded from fusion) without breaking the overall workflow.
- **End quality** is preserved when the system can avoid *trusting* bad localizations, even if a minority of frames never get a tight solve.

Stated plainly: **median pose error across all queries is less important than calibrated uncertainty and clear pass/fail signals** so consumers can apply thresholds and checks.

## Observability to support thresholds

To design those thresholds and checks, operators and automation need **basic, structured diagnostics per query** (and where feasible, **per database image pair**), for example:

- How many retrieval / covisibility pairs were considered, and **which database image names** were matched against.
- Match and correspondence counts at useful granularities (per pair and aggregated).
- RANSAC or robust estimation outputs that indicate consensus versus outlier contamination (e.g. inlier counts, error statistics), **broken down when the pipeline allows** so bad pairs can be distinguished from a globally good solve.

The exact fields exported to JSON or logs may evolve; the **design requirement** is that exports expose enough signal to **separate “don’t trust this localization” from “good enough to refine”** without manual forensics on every failure.

Holdout tooling: ``tests/test_localize_holdout.py`` writes ``correlation_vs_pose`` into ``holdout_results.json`` (Pearson/Spearman of pose error vs ``num_inliers``, ``num_matches``, ``num_2d3d``) when enough successful rows exist, saves ``holdout_correlation_scatter.png`` (matplotlib) beside the JSON by default, and can recompute with ``--analyze_only`` without re-running localization.

## QC thresholds (holdout-derived)

These numbers come from one 60-image holdout JSON (``tests/localize_holdout_results/holdout_results.json``). **BAD** means ``pos_error_m > --qc_good_max_pos_m`` or ``rot_error_deg > --qc_good_max_rot_deg`` (defaults ``0.1`` m and ``5`` deg); **GOOD** is all other successful rows. **Accept** only rows that pass the rule; the goal is **zero BAD accepted** while keeping as many GOOD as possible.

- **Counts:** 6 BAD, 54 GOOD (60 successes).
- **Best single-metric rules** (accept iff ``metric >= T`` for integers, with the smallest ``T`` that rejects every BAD; for ``ratio = num_inliers / max(num_2d3d, 1)`` use ``ratio >= nextafter(max_BAD_ratio)``, equivalent in practice to strict ``ratio > max_BAD_ratio``):
  - ``num_inliers >= 44`` (same as ``num_inliers > 43``): **49** GOOD accepted, **5** GOOD rejected.
  - ``num_matches >= 1308`` (``> 1307``): **37** GOOD accepted.
  - ``num_2d3d >= 397`` (``> 396``): **30** GOOD accepted.
  - ``ratio > 5/29`` (max BAD ratio is from 5 inliers / 29 correspondences): **51** GOOD accepted, **3** GOOD rejected.
- **Strict improvement with a simple AND:** ``num_inliers >= 6`` **and** a strict inlier purity floor. On this file the tightest ratio bound that still rejects every BAD with ``inl >= 6`` is ``ratio > 9/68`` (i.e. ``27/204``, the worst BAD among those with enough inliers to matter). **Integer form (recommended for code):** ``num_inliers >= 6`` and ``68 * num_inliers > 9 * max(num_2d3d, 1)``. **52** GOOD accepted (**2** GOOD false rejects) — one more GOOD than the best single-metric rule. Equivalent ANDs on this file: ``num_matches >= 88`` with the same ratio floor, or ``num_2d3d >= 30`` with the same ratio floor.
- **Rejected under the recommended AND** (8 rows = all BAD + 2 GOOD false rejects): ``000232``, ``000282``, ``000292``, ``000302``, ``000312``, ``000362``, ``000402``, ``000462`` (full names in JSON / sweep output).

Recompute or refresh the printed sweep with::

    python tests/test_localize_holdout.py --threshold_sweep tests/localize_holdout_results/holdout_results.json
    python tests/test_localize_holdout.py --threshold_sweep tests/localize_holdout_results/holdout_results.json --qc_good_max_pos_m 0.15 --qc_good_max_rot_deg 3

(Uses the same Python environment as the holdout test: ``numpy`` / ``scipy`` imports at startup.)

## Operational gate: inlier / 2D–3D ratio

**Starting-point threshold:** treat a localization as **passing** a simple correspondence-quality gate when

``num_inliers / max(num_2d3d, 1) >= 0.3``.

**Findings (single holdout, 60 successes, default BAD labels):** the worst BAD row on that file had ratio ``5/29 ≈ 0.17``. A floor at **0.3** is therefore above every BAD ratio observed there, rejects all six BAD poses under the default QC labels, and is a readable single knob for callers (less tight than the sweep-optimal ratio ``> 9/68`` but easier to justify as a margin). **Caveat:** this is still one scan and one holdout split; **next step** is to re-run holdout + ``--analyze_only`` / ``--threshold_sweep`` on additional refinements and scenes before locking server defaults.

The holdout harness defaults ``--min_inliers_2d3d_ratio`` to **0.3** for ``--analyze_only`` precision/recall tables so local runs match this recommendation unless overridden.

## Relationship to pose refinement

Pose refinement consumes trajectories or frame poses over time. A localization layer that reports **confidence and per-pair health** lets refinement:

- Use strong localizations as anchors.
- Drop or isolate weak frames instead of letting them distort the refined solution.

This document should stay aligned with implementation in `localize_image.py`, `localize_main.py`, `pose_tracking_server.py` (HTTP / streaming entry), and holdout or integration tests under `tests/`.
