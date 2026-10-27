# Coordinate conventions

This document lists the coordinate convention used at each pipeline step and for each output file. It also lists the places where two conventions meet without a conversion between them.

Frames are named the way the Auki SDK names them: each one says which way its x, y and z axes point, its handedness and its units. All frames here are right-handed and in meters. `reconstruction/geometry/frames.py` declares the frames that code converts between. Every conversion goes through `auki_geometry`, the SDK's convention library (`labs/auki-geometry` in [auki-sdk](https://github.com/aukilabs/auki-sdk), pinned with the SDK rev the node uses).

## Frames

| Frame | x | y | z | Declared as | Used by |
|---|---|---|---|---|---|
| OpenGL | right | up | backward | `frames.OPENGL` (the SDK's `opengl` preset) | ARKit world (`ARposes.csv`, `PortalDetections.csv`); Domain portal poses (`portals.json`); `refined_manifest.json`; the OpenGL point clouds and topology meshes |
| COLMAP world | up | right | forward | `frames.COLMAP_WORLD` | The world of every COLMAP model the pipeline writes (`colmap_rec`, `sfm`, `refined_sfm_combined`) and of `portals.csv`. Gravity is -x. |
| COLMAP camera | right | down | forward | (the SDK's `ros_optical` preset) | Every camera in a COLMAP model: x right and y down in the image, z along the optical axis |
| DMT camera | down | right | backward | | The camera side of `ARposes.csv`, relative to the stored landscape frame. It is not ARKit's own camera frame (right, up, backward); the recorder app rotates it before writing. |
| QR marker | right | up | backward | | A marker's own frame as the DMT recorder and pnp-lab report it: x and y in the marker plane, z out of the printed face toward the viewer |
| Auki robot world | forward | left | up | Read from each session's registry | `world` and `base_link` in an Auki capture (the SDK's `ros_body` preset; every session checked so far) |
| Auki camera | right | down | forward | Read from the registry | Auki sensor optical frames. Same as the COLMAP camera. |
| Auki anchor | forward | up | right | | `qr_anchor_poses.csv` and `point_cloud_qr_anchored.ply` (Auki-session flow) |

The DMT camera row was measured, not read from code. On the golden scan `2026-03-18_17-51-58`, the `ARposes.csv` camera poses, converted as the local stage converts them, agree with the refined COLMAP cameras to 0.59° median and 3.2° max over 601 frames. That agreement holds only if the camera axes in the file are (down, right, backward).

## Conversions

The SDK has three ways to change a pose's convention. They differ in which side of the pose they relabel. In pycolmap's `A_from_B`, B is the side the pose reads points from (the SDK calls it the source) and A the side it writes them to (the target).

| SDK function | Relabels | Example |
|---|---|---|
| `convert_pose_convention(pose, from, to)` | Both sides | A DMT pose: the world and the camera or marker change convention together |
| `convert_transform_target_convention(pose, from, to)` | The target (A) only | An Auki `world_from_camera`: the world changes convention, the camera keeps its own axes. Also pnp-lab's `camera_from_marker`: the camera changes, the marker keeps its own |
| `convert_transform_source_convention(pose, from, to)` | The source (B) only | A `world_from_camera` whose camera axes change while the world stays |

`convention_matrix(from, to)` gives the 4×4 matrix for points.

The DMT flows use one conversion, `frames.convert_pose_convention(position, quaternion, OPENGL, COLMAP_WORLD)` and its reverse. It swaps x and y and negates z on both sides of the pose, and applying it twice gives back the original pose. The quaternion it returns can be the negated one, which is the same rotation.

Relabelling both sides is right for DMT cameras because the DMT camera frame and the COLMAP camera frame differ by the same swap. For markers it relabels the marker's own axes too: in `portals.csv` a marker's x is the marker's up, y its right, and z points into the printed face. So a floor marker's z points down, which is why the DMT flat-portal check compares against `[-1, 0, 0]` (down in COLMAP world).

## Local refinement (DMT scan)

| Step | In | Code | Out |
|---|---|---|---|
| Read camera poses | `ARposes.csv`: `world_from_camera`, OpenGL world, DMT camera | `pipeline/local/refine.py:102` `convert_pose_convention(…, OPENGL, COLMAP_WORLD)` | COLMAP world, COLMAP camera |
| Read portal detections | `PortalDetections.csv`: `world_from_marker`, OpenGL world, QR marker | `io/portals.py:46`, same conversion | COLMAP world, relabelled marker |
| Triangulate, bundle-adjust | COLMAP world | `pipeline/local/triangulation.py`, `bundle_adjuster.py` | COLMAP world. The gauge stays the ARKit world's: within 0.2° of it on the golden scan |
| Portal world poses | Detections in camera space plus the refined cameras | `pipeline/local/qr.py:42` | COLMAP world |
| Snap near-floor portals flat | Portal z within 20° of `[-1, 0, 0]` | `pipeline/local/refine.py:207` → `geometry/pose.py:71` `rectify_portal_pose` | COLMAP world |
| Write | | `pipeline/local/refine.py:210`, `io/model.py` | `sfm/*.bin` and `portals.csv` in COLMAP world |

## Global refinement

| Step | In | Code | Out |
|---|---|---|---|
| Read scans | `sfm/` and `portals.csv` of each scan, each in its own COLMAP world | `pipeline/global_map/alignment.py:129` | |
| Domain origin | A floor marker at the origin, given in OpenGL | `pipeline/global_map/alignment.py:34` `convert_pose_convention(…, OPENGL, COLMAP_WORLD)` | `floor_origin_portal_pose`, COLMAP world |
| Place the first scan | Its first floor portal (`is_portal_almost_flat`), turned only about up (x) | `pipeline/global_map/alignment.py:75-92` | Scan in the domain frame, COLMAP world axes |
| Place later scans | `placed * scanned⁻¹` per shared portal, averaged | `pipeline/global_map/alignment.py` `calculate_alignment_transform` | Same |
| Refine and merge | COLMAP world | `pipeline/global_map/refine.py`, `merge.py` | `refined_sfm_combined`, COLMAP world |
| Manifest portals | COLMAP world, rectified | `output/manifest.py:188` `convert_pose_convention(…, COLMAP_WORLD, OPENGL)` | `refined_manifest.json`, OpenGL |
| Point cloud | `export_PLY` in COLMAP world | `entrypoints/global_map.py:153-156` → `output/pointcloud.py:71` `convention_matrix(COLMAP_WORLD, OPENGL)` | `RefinedPointCloud*.ply` and `.drc`, OpenGL |
| Topology | `RefinedPointCloud.ply` | `pipeline/topology/mesh.py:39` | `topology/*.obj`, `*.glb`, OpenGL |

Every alignment in global is computed and applied in COLMAP world.

## Update refinement

| Step | In | Code | Out |
|---|---|---|---|
| Read the reference map | `refined_sfm_combined` (COLMAP world) and `refined_manifest.json` portals (OpenGL, read as they are) | `pipeline/update/update.py:194` | Models in COLMAP world, portals in OpenGL |
| Read each new scan's portals | `portals.csv`, COLMAP world | `pipeline/update/update.py:126` `convert_pose_convention(…, COLMAP_WORLD, OPENGL)` | OpenGL |
| Alignment | New scan's portals and the reference portals, both OpenGL | `pipeline/update/update.py:209` | An OpenGL-frame transform |
| Apply the alignment | The new scan's model, **COLMAP world** | `pipeline/update/update.py:211` | See finding 1 |
| Prune, merge | COLMAP world | `pipeline/update/carving.py`, `merge.py` | `refined_sfm_combined`, COLMAP world |
| Manifest portals | Reference portals, OpenGL | `pipeline/update/update.py:263` converts them as if they were COLMAP world, then `save_manifest_json` converts again | `refined_manifest.json`, OpenGL (see finding 2) |
| Point cloud | COLMAP world | `pipeline/update/update.py:280` → `output/pointcloud.py:71` | `RefinedPointCloud*.ply`, OpenGL |

## Local refinement (Auki session)

This is the flow from `experiment/auki-session-input` (66a5279). It moves into this package in a later change; until then the references are to that commit.

| Step | In | Code (66a5279) | Out |
|---|---|---|---|
| Robot world convention | The session's registry (`world` → `base_link` pose log) | `auki_data_utils.py` `_read_from_frame_registry_entry` | Auki robot world |
| Camera poses | `world_from_camera`: Auki robot world and Auki camera | `convert_to_colmap_world`: `convert_transform_target_convention(…, robot world, COLMAP_WORLD)` | COLMAP world, camera axes unchanged (already the COLMAP camera) |
| QR poses from pnp-lab | `camera_from_marker` with the camera in OpenGL axes | `detect_qr_codes`: `convert_transform_target_convention(…, OpenGL camera, COLMAP camera)` | COLMAP camera, marker axes unchanged (QR marker) |
| Snap near-floor portals flat | Marker z within 20° of `[1, 0, 0]` (`_QR_UP_AXIS`): pnp-lab's marker z points out of the face, so up for a floor marker | `auki_refinement_util.py` `process_auki_qr` | COLMAP world, QR marker |
| `portals.csv` | Right-multiplied by `_DOMAIN_PORTAL_FRAME_RELABEL`, so the marker axes match the DMT `portals.csv` | `auki_refinement_util.py` | COLMAP world, relabelled marker, as in the DMT flow |
| QR-anchored outputs | Re-origined at one marker | `convert_colmap_pose_to_qr_origin_auki` → `convert_qr0_to_auki` | Auki anchor (`qr_anchor_poses.csv`, `point_cloud_qr_anchored.ply`) |

## Output files

| File | Frame |
|---|---|
| `refined/local/<scan>/sfm/*.bin`, `colmap_rec/*.bin` | COLMAP world, COLMAP cameras |
| `refined/local/<scan>/sfm/portals.csv` | COLMAP world, relabelled marker axes |
| `refined/global/refined_sfm_combined/*.bin`, `refined/update/refined_sfm_combined/*.bin` | COLMAP world (the domain frame) |
| `refined_manifest.json` portals | OpenGL, QR marker axes |
| `RefinedPointCloud.ply`, `RefinedPointCloudReduced.ply`, `RefinedPointCloudFloat.ply`, `RefinedPointCloud.ply.drc` | OpenGL |
| `topology/*.obj`, `topology/*.glb` | OpenGL |
| `qr_anchor_poses.csv`, `point_cloud_qr_anchored.ply` (Auki session) | Auki anchor, origin at one marker |

```mermaid
flowchart LR
  AR["ARposes.csv / PortalDetections.csv<br/>OpenGL"] -- "convert_pose_convention<br/>OPENGL to COLMAP_WORLD" --> L["local: sfm, portals.csv<br/>COLMAP world"]
  L --> G["global: align, refine, merge<br/>COLMAP world"]
  G -- "COLMAP_WORLD to OPENGL" --> M["refined_manifest.json<br/>OpenGL"]
  G -- "convention_matrix" --> P["point clouds, topology<br/>OpenGL"]
  L -- "portals.csv to OpenGL" --> U["update alignment<br/>OpenGL"]
  M --> U
  U -. "applied without conversion" .-> UM["update model<br/>COLMAP world"]
```

## Findings

These are recorded here and not yet fixed.

1. **Update computes its alignment in OpenGL and applies it in COLMAP world.** `pipeline/update/update.py:209` derives the transform from OpenGL portals; `:211` applies it to COLMAP-world points and cameras. The result is right only when the transform is close to identity, which is the case when the new scan already sits in the domain frame. DMT scans usually do. Otherwise both the turn and the shift land on the wrong axes; for example, a yaw comes out as a tilt. Commit 3ae4431 measured 10.85 m of portal error on a scan that was not near the domain frame. Global does not have this problem: it aligns in COLMAP world. Either of these would fix it:
   - Keep the portals in COLMAP world: don't convert `portals.csv` at `:126`, convert the manifest portals to COLMAP world once at `:194`, and drop the conversion at `:263`. This is what 3ae4431 does; it also logs each portal's residual after alignment and warns above 0.5 m.
   - Or relabel the transform once before applying it: `convert_pose_convention(alignment, OPENGL, COLMAP_WORLD)`.
2. **Update's manifest output converts twice.** `pipeline/update/update.py:263` converts the OpenGL reference portals "from COLMAP world to OpenGL", which actually puts them in COLMAP world because the swap is its own inverse. `save_manifest_json` then converts them to OpenGL. The file comes out right, but the code says the opposite of what it does. Fixing finding 1 the first way removes this.
3. **Update's first-chunk fallback mixes frames.** `pipeline/update/update.py:78` combines `floor_origin_portal_pose` (COLMAP world) with OpenGL portal poses. The comment above it says update never reaches that branch.
4. **The flat-portal axis differs between the two flows:** `[-1, 0, 0]` for DMT and `[1, 0, 0]` for Auki sessions. Both mean "the marker faces up", because the DMT flow has relabelled the marker axes by then and the Auki flow has not. Declaring the marker frames would make this explicit.
5. **The Auki anchor conversion's source frame is a placeholder.** `convert_qr0_to_auki` (66a5279) declares the marker's own axes as (forward, left, up), the robot body's convention, while pnp-lab's marker axes are (right, up, backward). Both give the same result for floor markers, the only kind tested. They differ for wall and ceiling markers.
6. **The occlusion box swaps y and z** (`pipeline/occlusion/occlusion_box.py:72`). That is a reflection, not a convention change, so no right-handed frame pair expresses it. The step is disabled.
