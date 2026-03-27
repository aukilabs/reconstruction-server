import numpy as np
from typing import Dict, Tuple
from utils.io import Image, Point3D, qvec2rotmat, cam_center_from_extrinsics
from collections import defaultdict
import logging

def carve_outdated_reference_geometry(
    ref_imgs: Dict[int, 'Image'],
    ref_pts: Dict[int, 'Point3D'],
    new_imgs: Dict[int, 'Image'],
    new_pts: Dict[int, 'Point3D'],
    voxel_size: float = 0.1,
    clearance_margin: float = 0.3,
    min_surviving_points: int = 15,
    logger: logging.Logger = None
) -> Tuple[Dict[int, 'Image'], Dict[int, 'Point3D']]:
    """
    Uses new free-space rays to delete conflicting points from the reference model.
    """
    if logger is None:
        logger = logging.getLogger()

    # 1. Map voxels to reference point IDs
    voxel_to_ref_pids = defaultdict(list)
    for pid, p in ref_pts.items():
        vox = tuple(np.floor(p.xyz / voxel_size).astype(int))
        voxel_to_ref_pids[vox].append(pid)
        
    violated_ref_pids = set()
    step_size = voxel_size / 2.0
    
    # 2. Cast rays from trusted new cameras
    for img in new_imgs.values():
        R = qvec2rotmat(img.qvec)
        C = cam_center_from_extrinsics(R, img.tvec)

        for pid in img.point3D_ids:
            if pid < 0 or pid not in new_pts:
                continue
                
            X = new_pts[pid].xyz
            ray_vec = X - C
            dist = np.linalg.norm(ray_vec)
            
            if dist <= clearance_margin:
                continue
                
            ray_dir = ray_vec / dist
            t_max = dist - clearance_margin 
            t_vals = np.arange(clearance_margin, t_max, step_size)
            
            if len(t_vals) == 0:
                continue
                
            ray_pts = C + t_vals[:, np.newaxis] * ray_dir
            ray_voxels = np.floor(ray_pts / voxel_size).astype(int)
            
            # 3. Collect old points that violate the new free space
            for vox in ray_voxels:
                vox_tuple = tuple(vox)
                if vox_tuple in voxel_to_ref_pids:
                    violated_ref_pids.update(voxel_to_ref_pids[vox_tuple])
                    # Remove from dict so we don't process it multiple times
                    del voxel_to_ref_pids[vox_tuple] 

    logger.info(f"[prune] Vaporizing {len(violated_ref_pids)} outdated reference points.")

    # 4. Rebuild the reference model without the violated points
    pruned_ref_pts = {pid: p for pid, p in ref_pts.items() if pid not in violated_ref_pids}
    
    pruned_ref_imgs = {}
    violated_array = np.array(list(violated_ref_pids))

    dropped_img_names = []
    
    for iid, img in ref_imgs.items():
        new_pids = img.point3D_ids.copy()
        
        # Set deleted points to -1
        mask = np.isin(new_pids, violated_array)
        new_pids[mask] = -1
        
        # Check if the old image still sees enough valid points to survive
        valid_count = np.sum(new_pids >= 0)
        if valid_count >= min_surviving_points:
            img = img._replace(point3D_ids=new_pids)
            pruned_ref_imgs[iid] = img
        else:
            # If it doesn't have enough points, we drop it and record its name
            dropped_img_names.append((img.name, valid_count))
    # Print the summary of dropped images
    if dropped_img_names:
        logger.info(f"[prune] Dropped {len(dropped_img_names)} old reference images (fell below {min_surviving_points} valid points):")
        for name, remaining_pts in dropped_img_names:
            logger.debug(f"  -> {name} (Only {remaining_pts} points left)")
    else:
        logger.info(f"[prune] No reference images were completely dropped.")        
    
    logger.info(f"[prune] Kept {len(pruned_ref_imgs)}/{len(ref_imgs)} reference images.")
    
    final_ref_pts = {}
    valid_pids = set()
    kept_img_ids = set(pruned_ref_imgs.keys())

    # Filter the tracks of the surviving points
    for pid, p in pruned_ref_pts.items():
        # Keep only track observations from images that survived the prune
        new_image_ids = []
        new_point2D_idxs = []
        for (iid, idx) in zip(p.image_ids, p.point2D_idxs):
            if iid in kept_img_ids:
                new_image_ids.append(iid)
                new_point2D_idxs.append(idx)


        # A 3D point must be seen by at least 2 cameras to physically exist in COLMAP
        if len(new_image_ids) >= 2:
            final_ref_pts[pid] = Point3D(pid, p.xyz, p.rgb, p.error, np.array(new_image_ids), np.array(new_point2D_idxs))
            valid_pids.add(pid)

    # Final sweep: disconnect the surviving images from any points we just deleted
    valid_pids_array = np.array(list(valid_pids))
    for iid, img in list(pruned_ref_imgs.items()):
        # Find point IDs that are >= 0 BUT are no longer in our valid_pids set
        mask = ~np.isin(img.point3D_ids, valid_pids_array) & (img.point3D_ids >= 0)
        if mask.any():
            updated_pids = img.point3D_ids.copy()
            updated_pids[mask] = -1
            pruned_ref_imgs[iid] = img._replace(point3D_ids=updated_pids)

    pts_removed = len(pruned_ref_pts) - len(final_ref_pts)
    logger.info(f"[prune] Final cleanup: Removed {pts_removed} ghost points that lost camera support.")
    
    return pruned_ref_imgs, final_ref_pts