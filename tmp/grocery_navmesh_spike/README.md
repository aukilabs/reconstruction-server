# Grocery navmesh spike (offline)

Top-down floor evidence + wall blockers + trajectory-seeded flood fill on the locked global mesh.

## Run

From the reconstruction-server repo root (or any cwd):

```bash
python tmp/grocery_navmesh_spike/run_navmesh_spike.py
```

Defaults use the locked inputs:

- Mesh: `posemesh-harness/tmp/mono_depth_ux_v2/meshes/da3_global_stronger_carve_colored_nofloaters.ply`
- Trajectory: `.../traj_global_aligned.json`
- Outputs: this directory (`tmp/grocery_navmesh_spike/`)

## Useful flags

- `--grid-cell 0.05` — XZ cell size (meters)
- `--seed-dilate 0.5` — seed corridor half-width (meters)
- `--max-invent 1.75` — max fill distance from floor evidence (meters)
- `--floor-y-min / --floor-y-max` — floor triangle centroid band (Y-up)

## Floor mesh (pre-Recast)

1. **Thin enclosed holes** in the walkable mask: fill background CCs that do not
   touch the grid border, have **&lt; 4 cells**, and are **≤ 1 cell wide in either
   axis** (so 1×1 / 1×2 / 1×3 close; 2×2 does not).
2. **Same-surface median** flatten (default radius 0.55 m, outlier 6 cm, 2 iters).

That mesh is `nav_floor.ply` (Recast input).

## Recast bake

After `nav_floor.ply` exists:

```bash
node tmp/grocery_navmesh_spike/bake_recast.mjs
```

Uses [`recast-navigation`](https://github.com/isaac-mason/recast-navigation-js) (WASM Recast/Detour). Defaults: cell **0.05 m**, agent radius **0.10 m**, height 1.8 m, climb 0.25 m, tiled.

**After bake**, the viewer PLY (`navmesh_recast.ply`) gets a **median plane snap**:
hard project within **5 cm**, smoothstep pull 1→0 out to **15 cm**
(`plane_snap_ply.py`; Detour `.bin` is left unsnapped).

Viewer: `nav_floor.ply` + `navmesh_recast.ply` (+ optional TSDF overlay).

## Outputs

| File | Description |
|------|-------------|
| `summary.json` | Counts, knobs, paths (+ `recast` block after bake) |
| `nav_floor.ply` | Preprocess walkable surface (Recast input) |
| `navmesh_recast.ply` | Recast debug navmesh for the 3D viewer |
| `navmesh_recast.bin` | Detour binary (`exportNavMesh`) |
| `overlay_*.png` | Top-down layers + `overlay_all.png` |
| `viewer.html` | 3D viewer (dropdown: Recast / preprocess floor) |

Spike only — not wired into recon-server refine or upload.
