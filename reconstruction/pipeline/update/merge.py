"""Merging a new, aligned model into the reference model."""
import numpy as np
from scipy.spatial.transform import Rotation as R
from typing import Dict, Tuple, Optional
from reconstruction.geometry.pose import cam_center_from_extrinsics
from reconstruction.io.colmap import BaseImage, Camera, Image, Point3D, qvec2rotmat


def apply_similarity_to_new_model(cams: Dict[int, 'Camera'], imgs: Dict[int, 'BaseImage'], pts: Dict[int, 'Point3D'],
                                  T_a: np.ndarray) -> Tuple[Dict[int, 'Camera'], Dict[int, 'BaseImage'], Dict[int, 'Point3D']]:
    """
    Transform the entire new model (cameras & points) into the reference frame 
    using a 4x4 similarity transformation matrix.
    """
    
    # Extract scale and pure rotation from the 4x4 matrix
    # T_a[:3, :3] contains s * R_a. We can find 's' by taking the norm of the first column.
    s = np.linalg.norm(T_a[:3, 0])
    R_a = T_a[:3, :3] / s

    # transform images: update rotations and translations via centers
    new_imgs = {}
    for iid, im in imgs.items():
        Rn = qvec2rotmat(im.qvec)
        tn = im.tvec
        Cn = cam_center_from_extrinsics(Rn, tn)
        
        # Apply 4x4 transformation to the camera center using homogeneous coordinates
        Cn_hom = np.append(Cn, 1.0)
        C_ref = (T_a @ Cn_hom)[:3]
        
        R_ref = Rn @ R_a.T
        t_ref = -R_ref @ C_ref

        rot_ref = R.from_matrix(R_ref)
        q_ref2 = rot_ref.as_quat()
        # SciPy returns [x, y, z, w], but COLMAP uses [w, x, y, z]
        q_ref2 = np.array([q_ref2[3], q_ref2[0], q_ref2[1], q_ref2[2]])
        new_imgs[iid] = BaseImage(iid, q_ref2, t_ref, im.camera_id, im.name, im.xys, im.point3D_ids)
        
    # transform points
    new_pts = {}
    for pid, p in pts.items():
        # Apply 4x4 transformation to 3D points
        p_hom = np.append(p.xyz, 1.0)
        X_ref = (T_a @ p_hom)[:3]
        new_pts[pid] = Point3D(pid, X_ref, p.rgb, p.error, p.image_ids, p.point2D_idxs)
        
    # cameras unchanged (intrinsics)
    return cams, new_imgs, new_pts


def merge_models(reference_model: Tuple[Dict[int,Camera], Dict[int,Image], Dict[int,Point3D]],
                 new_model: Tuple[Dict[int,Camera], Dict[int,Image], Dict[int,Point3D]],
                 new_name_prefix: Optional[str] = "new/") -> Tuple[Dict[int,Camera], Dict[int,Image], Dict[int,Point3D], Dict[int,int]]:
    cams_r, imgs_r, pts_r = reference_model
    cams_n, imgs_n, pts_n = new_model

    next_cam_id = (max(cams_r.keys()) + 1) if cams_r else 1
    next_img_id = (max(imgs_r.keys()) + 1) if imgs_r else 1
    next_pt_id  = (max(pts_r.keys())  + 1) if pts_r  else 1

    # 1) Cameras: append and map ids
    cam_map = {}
    for cid, c in cams_n.items():
        cam_map[cid] = next_cam_id
        cams_r[next_cam_id] = Camera(next_cam_id, c.model, c.width, c.height, list(c.params))
        next_cam_id += 1

    # 2) Images: append, avoid name clashes, remember image id remap
    existing_names = set([im.name for im in imgs_r.values()])
    img_map = {}
    for iid, im in imgs_n.items():
        new_name = im.name
        if new_name in existing_names:
            new_name = (new_name_prefix or "new/") + new_name
        img_map[iid] = next_img_id
        # Temporarily copy point3D_ids; we'll fix them after adding points (once we know pid remap)
        imgs_r[next_img_id] = BaseImage(
            next_img_id, im.qvec, im.tvec, cam_map[im.camera_id],
            new_name, im.xys.copy(), im.point3D_ids.copy()
        )
        existing_names.add(new_name)
        next_img_id += 1

    # 3) Points: append with new ids, rewrite tracks with remapped image ids
    oldpid_to_newpid: Dict[int, int] = {}
    for old_pid, p in pts_n.items():
        new_img_ids = []
        new_pt2ds = []
        for (old_iid, j) in zip(p.image_ids, p.point2D_idxs):
            if old_iid in img_map:
                new_img_ids.append(img_map[old_iid])
                new_pt2ds.append(j)

        if len(new_img_ids) >= 2:
            new_pid = next_pt_id
            oldpid_to_newpid[old_pid] = new_pid
            pts_r[new_pid] = Point3D(new_pid, p.xyz, p.rgb, p.error, np.array(new_img_ids), np.array(new_pt2ds))
            next_pt_id += 1
        # else: drop point (insufficient observations)

    # Build inverse image map once
    inv_img_map = {new_id: old_id for old_id, new_id in img_map.items()}

    for new_iid, im_r in imgs_r.items():
        if new_iid not in inv_img_map:
            continue  # this is an original reference image; its links stay unchanged
        old_iid = inv_img_map[new_iid]
        # Fetch the original new image to see original point3D_ids (old pids)
        im_old = imgs_n[old_iid]
        old_pids = im_old.point3D_ids
        if old_pids.size == 0:
            continue
        # Map old pids to new pids where available; else set to -1
        mapped = old_pids.copy()
        mask = mapped >= 0
        if mask.any():
            # vectorized map: for speed, use a dict lookup with fallback -1
            mapped_ids = []
            for pid in mapped[mask]:
                mapped_ids.append(oldpid_to_newpid.get(int(pid), -1))
            mapped[mask] = np.array(mapped_ids, dtype=np.int64)
        # namedtuple-based Image is immutable; replace with updated copy
        imgs_r[new_iid] = im_r._replace(point3D_ids=mapped)
    
    return cams_r, imgs_r, pts_r, oldpid_to_newpid
