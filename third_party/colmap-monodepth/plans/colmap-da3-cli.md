# Initiative: colmap-da3-cli

Status: done  
Tier: feature  
Stage: 11-gate  
Skips: _(none — §§1–6 filled in-chat before implement; Plan mode skipped per user request)_  
Last updated: 2026-08-03

Living document — update in place when later work changes earlier conclusions.  
**§1 Intent (feature) and §4 System design stay distinct — never merge.**

---

## 1. Intent

- **Customer / internal need:** Reconstruct metric-consistent dense depth from an existing COLMAP sparse reconstruction without inventing camera poses.
- **Framing:** Users already have COLMAP extrinsics/intrinsics; they need pose-conditioned monocular/multi-view depth (Depth Anything 3) and simple exports (per-image depth PNGs + a PLY point cloud).
- **Why now:** DA3 supports pose conditioning and Apache-licensed DA3-BASE; wiring COLMAP priors into that API is the missing thin CLI.
- **Success look like:** One CLI invocation on a COLMAP dir writes depth PNGs + PLY; unit round-trip proves the COLMAP parser; e2e runs on a public 7Scenes scene.

## 2. Alternatives

- **Existing ways:** Official `da3 colmap` CLI (exports glb/depth_vis by default, not our exact PNG+PLY contract); hand-rolled scripts per project.
- **Comparison:** Official CLI is close but not the deliverable shape (custom modules, PNG+PLY, tests). Must build a thin wrapper around documented DA3 APIs.
- **Decision:** proceed.

## 3. Priority

- **Rank:** top-of-queue (greenfield repo; this is the product).
- **Capacity:** single feature slice; no competing backlog.
- **Decision:** top-of-queue.

## 4. System design

- **Products / codebases impacted:** new `colmap_monodepth` package in this repo; depends on `depth-anything-3` (ByteDance-Seed) + torch.
- **Constraints:**
  - Use documented DA3 API only (`DepthAnything3.from_pretrained` / `inference` with `extrinsics` `(N,4,4)` w2c and `intrinsics` `(N,3,3)`). Sources: [docs/API.md](https://github.com/ByteDance-Seed/Depth-Anything-3/blob/main/docs/API.md), [ColmapHandler](https://github.com/ByteDance-Seed/Depth-Anything-3/blob/main/src/depth_anything_3/services/input_handlers.py).
  - Default model: `depth-anything/DA3-BASE` (Apache 2.0 pose-conditioning).
  - Default device: `cuda`.
  - COLMAP layout: `images/` + `sparse/` (optional `sparse/0/`), text or binary model.
- **Interfaces / boundaries:**
  - `colmap_io`: read COLMAP → image paths, extrinsics, intrinsics (same conversion as DA3 ColmapHandler).
  - `inference`: load DA3, call pose-conditioned `inference`.
  - `export`: write 16-bit depth PNGs + colored PLY (DA3 has no first-class depth-PNG / plain-PLY export; we derive from `prediction.depth` / cameras — same back-projection idea as DA3 `export/glb.py`).
  - `cli`: `--colmap-dir`, `--output-dir`, `--model`, `--device`.
- **Risks:** GPU/CUDA required for practical e2e; Microsoft 7Scenes has no `dining_table` scene (use `chess`); HF model download size/network.
- **Decision:** design change needed (new greenfield CLI package).

## 5. Verification design

- **End-to-end acceptance:**
  1. `pytest tests/test_colmap_roundtrip.py` — write synthetic COLMAP text model, read back, assert cameras/images/extrinsics/intrinsics match.
  2. Prepare 7Scenes `chess` (public MSR download) as COLMAP dir from GT poses; run CLI; assert depth PNGs exist per image and `pointcloud.ply` is non-empty.
- **Integration / regression:** CLI `--help` exits 0; import of package modules without GPU optional for unit tests.
- **CI / delivery:** local pytest for round-trip; e2e marked and runnable when CUDA + data available.
- **Out of scope for v1:** GS export, backend server, binary-only write path, metric accuracy vs GT depth.

## 6. Work breakdown (AI-oriented)

- **Slice A:** `colmap_io` + synthetic round-trip test.
- **Slice B:** `inference` + `export` + `cli` + requirements/README.
- **Slice C:** 7Scenes prepare script + e2e runner; record §8 evidence.
- **Human:** CUDA machine / HF access if agent env lacks GPU.
- **Stop / escalate:** If DA3 API differs from docs at install time → bounce §4; if 7Scenes download blocked → document and use minimal local fixture.

## 7. Implementation notes

- Mirror DA3 ColmapHandler pose math: `qvec2rotmat` + t → 4×4 w2c; PINHOLE / SIMPLE_PINHOLE / fallback fx.
- HuggingFace id: `depth-anything/DA3-BASE` (model card).
- Challenge: user said `dining_table`; 7Scenes scenes are chess/fire/heads/office/pumpkin/redkitchen/stairs — e2e uses **chess**.

## 8. Verification record

- **2026-08-03 unit:** `.venv\Scripts\pytest.exe tests/test_colmap_roundtrip.py -q` → **3 passed**
- **2026-08-03 e2e:** prepared Microsoft 7Scenes **chess** (no `dining_table` in 7Scenes) as COLMAP via GT poses; ran CLI with `depth-anything/DA3-BASE` on CUDA (RTX 5070 Ti). Log: `Using camera conditions provided by the user`. Outputs: 6× `*_depth.png`, `pointcloud.ply` (~685k points). Command: `python scripts/run_e2e_7scenes.py --skip-prepare --device cuda`.

## Gate (stage 11)

- **Decision:** accept  
- **Notes:** Intent met; dining_table substituted with chess with rationale recorded in §4/§7.
