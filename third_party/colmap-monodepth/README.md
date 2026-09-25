# COLMAP → Depth Anything 3 + mesh refine

CLI and Python API that reads a COLMAP sparse reconstruction, runs [Depth Anything 3](https://github.com/ByteDance-Seed/Depth-Anything-3) pose-conditioned depth, then optional mesh-refine stages (fit → carve → TSDF).

Default model: **`depth-anything/DA3-BASE`** (Apache 2.0, pose conditioning supported).

## Staged pipeline

| Stage | Module | Role |
|-------|--------|------|
| `infer` | `pipeline.run_infer` | COLMAP → DA3 depth PNGs + `pointcloud.ply` |
| `fit` | `pipeline.run_fit` | Joint scale + residual fit → `fitted/` + `confident/` |
| `carve` | `pipeline.run_carve` | E1 subtractive carve → masked depths |
| `tsdf` | `pipeline.run_tsdf` | Open3D ScalableTSDF → `tsdf_mesh.ply` |
| `mesh` | `pipeline.run_mesh` | **E1 composition:** infer → fit → carve(**confident/**) → tsdf |

Supporting modules: `colmap_io`, `inference`, `export`, `depth_io`, `fit`, `carve`, `tsdf`, `types`.

## Install extras

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
pip install -U pip
# CUDA torch for your GPU first, e.g.:
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu128
pip install -e .
```

| Extra | Installs | Needed for |
|-------|----------|------------|
| `[da3]` | `depth-anything-3`, torch | `infer`, `mesh` |
| `[fit]` | torch, opencv | `fit`, `mesh` |
| `[mesh]` | open3d, numba, opencv | `carve`, `tsdf`, `mesh` |
| `[dev]` | pytest, numba, opencv | unit tests |

Full mesh pipeline: `pip install -e ".[da3,fit,mesh,dev]"`

Use **Python 3.10–3.12** (`depth-anything-3` declares `<=3.13`).

## CLI

Subcommands: `infer | fit | carve | tsdf | mesh`. Legacy invocations without a subcommand still run `infer` (backward compatible).

```bash
# Depth inference only
colmap-monodepth infer \
  --colmap-dir /path/to/colmap_dataset \
  --output-dir /path/to/output \
  --model depth-anything/DA3-BASE \
  --device cuda

# Legacy (same as infer)
colmap-monodepth --colmap-dir /path/to/dataset --output-dir /path/to/output

# Fit scaled depths (input: infer/depth)
colmap-monodepth fit \
  --colmap-dir /path/to/dataset \
  --depth-dir /path/to/output/depth \
  --output-dir /path/to/fit_out

# Carve from confident depths (not unmasked fitted/)
colmap-monodepth carve \
  --colmap-dir /path/to/dataset \
  --depth-dir /path/to/fit_out/confident \
  --output-dir /path/to/carve_out

# TSDF mesh from carved depths
colmap-monodepth tsdf \
  --colmap-dir /path/to/dataset \
  --depth-dir /path/to/carve_out \
  --output-dir /path/to/mesh_out

# E1 end-to-end (infer → fit → carve(confident) → tsdf)
colmap-monodepth mesh \
  --colmap-dir /path/to/dataset \
  --output-dir /path/to/mesh_pipeline
```

Expected COLMAP layout: `images/` + model under `sparse/`, `sparse/0/`, or directly in the dataset root.

### E1 defaults (locked experiment)

- **Fit:** `mode=residual_icp`, `steps_affine=50` + `steps_res=100` (`icp_rounds=1` → residual→rematch→residual), residual grid `12×16`, geo cost = temporal neighbors only on a full-res strided UV grid (`geo_stride=8`), batched Torch geo/track losses, scale clamp `[0.85, 1.15]`, ICP rematch + temporal confident mask (`agree_max_abs_m=0.05`)
- **Carve:** `voxel_size=0.07`, subtractive carve (`subtractive_value=-0.2`), multi-view voxel gate (`min_points=3`, `min_confidence=2.0`)
- **TSDF:** `voxel_length=0.04`, `sdf_trunc=0.16`, `depth_trunc=10.0`
- **Mesh composition:** carve reads **`fit/confident/`**, not `fit/fitted/`

## Python API

```python
from colmap_monodepth.pipeline import run_infer, run_fit, run_carve, run_tsdf, run_mesh

infer = run_infer("colmap_dir", "out/infer", device="cuda")
fit = run_fit("colmap_dir", infer.depth_dir, "out/fit")
carve = run_carve("colmap_dir", fit.confident_depth_dir, "out/carve")
tsdf = run_tsdf("colmap_dir", carve.output_depth_dir, "out/mesh", infer.image_paths)

# Or one call:
result = run_mesh("colmap_dir", "out/pipeline", device="cuda")
```

## Outputs

**infer:** `depth/*_depth.png` (16-bit mm), `pointcloud.ply`

**fit:** `fitted/`, `confident/`, `params.csv`, `meta.json`

**carve:** masked depth PNGs

**tsdf:** `tsdf_mesh.ply`, `tsdf_points.ply`

**mesh:** `infer/`, `fit/`, `carve/`, `mesh/` subdirectories

## Tests

```bash
# Unit tests (no GPU)
pytest tests/ -q

# CLI help smoke
pytest tests/test_cli.py -q

# E2E on public 7Scenes (CUDA + HF weights)
python scripts/run_e2e_7scenes.py --scene chess --device cuda
```

## Praxis plan

See `plans/colmap-da3-cli.md`.
