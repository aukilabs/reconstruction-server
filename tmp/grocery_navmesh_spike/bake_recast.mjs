/**
 * Bake a Recast/Detour navmesh from nav_floor.ply and export a viewable PLY.
 * Uses recast-navigation (WASM port of recastnavigation).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  init,
  exportNavMesh,
  getNavMeshPositionsAndIndices,
} from "recast-navigation";
import { generateTiledNavMesh } from "recast-navigation/generators";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
  const out = {
    input: path.join(__dirname, "nav_floor.ply"),
    outDir: __dirname,
    // Grocery person / cart-friendly defaults (world meters → voxels below)
    cs: 0.05,
    ch: 0.05,
    agentRadius: 0.10,
    agentHeight: 1.8,
    agentMaxClimb: 0.25,
    agentMaxSlope: 35,
    tileSize: 32,
    // Post-Recast debug-mesh plane snap (viewer PLY only; Detour .bin unchanged)
    planeSnap: 0.05,
    planeOuter: 0.15,
    planeSnapEnable: true,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a === "--input") out.input = path.resolve(next());
    else if (a === "--out-dir") out.outDir = path.resolve(next());
    else if (a === "--cs") out.cs = Number(next());
    else if (a === "--ch") out.ch = Number(next());
    else if (a === "--agent-radius") out.agentRadius = Number(next());
    else if (a === "--agent-height") out.agentHeight = Number(next());
    else if (a === "--agent-max-climb") out.agentMaxClimb = Number(next());
    else if (a === "--agent-max-slope") out.agentMaxSlope = Number(next());
    else if (a === "--tile-size") out.tileSize = Number(next());
    else if (a === "--plane-snap") out.planeSnap = Number(next());
    else if (a === "--plane-outer") out.planeOuter = Number(next());
    else if (a === "--no-plane-snap") out.planeSnapEnable = false;
  }
  return out;
}

function loadPlyAsArrays(plyPath) {
  const py = `
import json, sys
import open3d as o3d
import numpy as np
m = o3d.io.read_triangle_mesh(sys.argv[1])
if not m.has_triangles():
    raise SystemExit("no triangles")
v = np.asarray(m.vertices, dtype=np.float64)
t = np.asarray(m.triangles, dtype=np.int32)
# Ensure CCW for Recast (OpenGL convention): flip if majority CW when viewed from +Y
areas = []
for a,b,c in t[: min(len(t), 2000)]:
    n = np.cross(v[b]-v[a], v[c]-v[a])
    areas.append(n[1])
flip = (np.mean(areas) < 0)
if flip:
    t = t[:, ::-1].copy()
print(json.dumps({
  "positions": v.reshape(-1).tolist(),
  "indices": t.reshape(-1).tolist(),
  "flipped_winding": bool(flip),
  "n_verts": int(len(v)),
  "n_tris": int(len(t)),
}))
`;
  const tmpPy = path.join(__dirname, "_dump_ply_geom.py");
  fs.writeFileSync(tmpPy, py);
  const r = spawnSync("python", [tmpPy, plyPath], {
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
  });
  try {
    fs.unlinkSync(tmpPy);
  } catch {}
  if (r.status !== 0) {
    throw new Error(`PLY load failed: ${r.stderr || r.stdout}`);
  }
  return JSON.parse(r.stdout);
}

function writeAsciiPly(outPath, positions, indices) {
  const nV = positions.length / 3;
  const nF = indices.length / 3;
  const lines = [];
  lines.push("ply");
  lines.push("format ascii 1.0");
  lines.push(`element vertex ${nV}`);
  lines.push("property float x");
  lines.push("property float y");
  lines.push("property float z");
  lines.push(`element face ${nF}`);
  lines.push("property list uchar int vertex_indices");
  lines.push("end_header");
  for (let i = 0; i < nV; i++) {
    const o = i * 3;
    lines.push(
      `${positions[o]} ${positions[o + 1]} ${positions[o + 2]}`
    );
  }
  for (let i = 0; i < nF; i++) {
    const o = i * 3;
    lines.push(
      `3 ${indices[o]} ${indices[o + 1]} ${indices[o + 2]}`
    );
  }
  fs.writeFileSync(outPath, lines.join("\n") + "\n");
}

function voxelsFromMeters(meters, cell, { min = 1 } = {}) {
  return Math.max(min, Math.ceil(meters / cell - 1e-12));
}

/** Agent radius may be 0 voxels when below ~half a cell (intentional minimal erosion). */
function radiusVoxels(meters, cell) {
  if (meters <= 0) return 0;
  return Math.max(0, Math.round(meters / cell));
}

async function main() {
  const args = parseArgs(process.argv);
  fs.mkdirSync(args.outDir, { recursive: true });

  console.log(`Loading ${args.input}…`);
  const geom = loadPlyAsArrays(args.input);
  console.log(
    `Input: ${geom.n_verts} verts, ${geom.n_tris} tris` +
      (geom.flipped_winding ? " (winding flipped to CCW)" : "")
  );

  const walkableRadius = radiusVoxels(args.agentRadius, args.cs);
  const walkableHeight = voxelsFromMeters(args.agentHeight, args.ch);
  const walkableClimb = Math.max(
    1,
    Math.floor(args.agentMaxClimb / args.ch)
  );

  const config = {
    cs: args.cs,
    ch: args.ch,
    walkableRadius,
    walkableHeight,
    walkableClimb,
    walkableSlopeAngle: args.agentMaxSlope,
    tileSize: args.tileSize,
    // Prefer larger polys for aisle floors
    maxEdgeLen: 24,
    maxSimplificationError: 1.3,
    minRegionArea: 8,
    mergeRegionArea: 20,
    maxVertsPerPoly: 6,
    detailSampleDist: 6,
    detailSampleMaxError: 1,
  };

  console.log("Recast config (voxel units where noted):", {
    ...config,
    agentRadius_m: args.agentRadius,
    agentHeight_m: args.agentHeight,
    agentMaxClimb_m: args.agentMaxClimb,
  });

  await init();
  const t0 = Date.now();
  const result = generateTiledNavMesh(
    Float32Array.from(geom.positions),
    Uint32Array.from(geom.indices),
    config
  );
  const ms = Date.now() - t0;

  if (!result.success || !result.navMesh) {
    console.error("Recast bake failed:", result.error || result);
    process.exit(1);
  }

  const [dbgPos, dbgIdx] = getNavMeshPositionsAndIndices(result.navMesh);
  const nDbgV = dbgPos.length / 3;
  const nDbgT = dbgIdx.length / 3;
  console.log(
    `Baked in ${ms}ms · debug mesh ${nDbgV} verts / ${nDbgT} tris`
  );

  const plyPath = path.join(args.outDir, "navmesh_recast.ply");
  const binPath = path.join(args.outDir, "navmesh_recast.bin");
  writeAsciiPly(plyPath, dbgPos, dbgIdx);

  let planeSnapStats = null;
  if (args.planeSnapEnable) {
    const snapScript = path.join(__dirname, "plane_snap_ply.py");
    const snap = spawnSync(
      "python",
      [
        snapScript,
        plyPath,
        "--snap",
        String(args.planeSnap),
        "--outer",
        String(args.planeOuter),
        "--stats-json",
      ],
      { encoding: "utf8" }
    );
    if (snap.status !== 0) {
      console.error("Post-Recast plane snap failed:", snap.stderr || snap.stdout);
      process.exit(1);
    }
    planeSnapStats = JSON.parse(snap.stdout.trim().split("\n").pop());
    console.log(
      `Plane-snap navmesh PLY: hard=${planeSnapStats.vertices_hard_snap} ` +
        `ramp=${planeSnapStats.vertices_ramped} untouched=${planeSnapStats.vertices_untouched}`
    );
  }

  const bin = exportNavMesh(result.navMesh);
  fs.writeFileSync(binPath, Buffer.from(bin));

  const summaryPath = path.join(args.outDir, "summary.json");
  let summary = {};
  if (fs.existsSync(summaryPath)) {
    summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  }
  summary.recast = {
    library: "recast-navigation@0.43.1",
    input: path.resolve(args.input),
    bake_ms: ms,
    config_world: {
      cs: args.cs,
      ch: args.ch,
      agentRadius: args.agentRadius,
      agentHeight: args.agentHeight,
      agentMaxClimb: args.agentMaxClimb,
      agentMaxSlope: args.agentMaxSlope,
    },
    config_voxels: {
      walkableRadius,
      walkableHeight,
      walkableClimb,
      tileSize: args.tileSize,
    },
    input_mesh: {
      vertices: geom.n_verts,
      triangles: geom.n_tris,
      flipped_winding: geom.flipped_winding,
    },
    output_mesh: {
      vertices: nDbgV,
      triangles: nDbgT,
    },
    plane_snap: planeSnapStats,
    outputs: {
      navmesh_recast_ply: plyPath,
      navmesh_recast_bin: binPath,
    },
  };
  if (!summary.outputs) summary.outputs = {};
  summary.outputs.navmesh_recast_ply = plyPath;
  summary.outputs.navmesh_recast_bin = binPath;
  // Floor mesh no longer carries plane-snap stats
  delete summary.mesh_plane_snap;
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2) + "\n");

  result.navMesh.destroy();
  console.log(`Wrote ${plyPath}`);
  console.log(`Wrote ${binPath} (${bin.byteLength} bytes)`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
