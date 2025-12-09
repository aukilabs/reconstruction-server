import argparse
from collections import defaultdict
from pathlib import Path

import numpy as np
from tqdm import tqdm

from hloc import logger
from hloc.utils.read_write_model import read_model


def compute_voxel_hash(xyz, voxel_size):
    """Compute integer hash from voxel coordinates."""
    vx = int(np.floor(xyz[0] / voxel_size))
    vy = int(np.floor(xyz[1] / voxel_size))
    vz = int(np.floor(xyz[2] / voxel_size))
    # Use spatial hash with primes to avoid collisions
    return vx * 73856093 + vy * 19349663 + vz * 83492791


def main(model, output, num_matched, voxel_size=0.2, image_names=None):
    logger.info("Reading the COLMAP model...")
    cameras, images, points3D = read_model(model)

    logger.info("Precomputing voxel hashes per image...")
    image_voxel_hashes = {}
    for image_id, image in tqdm(images.items(), desc="Computing voxel hashes"):
        if image_names and image.name not in image_names:
            continue

        matched = image.point3D_ids != -1
        points3D_covis = image.point3D_ids[matched]
        
        voxel_hashes = set()
        for point_id in points3D_covis:
            xyz = points3D[point_id].xyz
            voxel_hash = compute_voxel_hash(xyz, voxel_size)
            voxel_hashes.add(voxel_hash)
        
        image_voxel_hashes[image_id] = voxel_hashes

    logger.info("Extracting image pairs from voxelized covisibility info...")
    pairs = []
    for image_id, image in tqdm(images.items(), desc="Finding pairs"):
        if image_names and image.name not in image_names:
            continue
        
        voxel_hashes = image_voxel_hashes[image_id]
        
        if len(voxel_hashes) == 0:
            logger.info(f"Image {image_id} does not have any voxels.")
            continue

        covis = defaultdict(int)
        for other_image_id, other_voxel_hashes in image_voxel_hashes.items():
            if other_image_id == image_id:
                continue
            # Count common voxels
            common_voxels = voxel_hashes & other_voxel_hashes
            if len(common_voxels) > 0:
                covis[other_image_id] = len(common_voxels)

        if len(covis) == 0:
            logger.info(f"Image {image_id} does not have any covisibility.")
            continue

        covis_ids = np.array(list(covis.keys()))
        covis_num = np.array([covis[i] for i in covis_ids])

        if len(covis_ids) <= num_matched:
            top_covis_ids = covis_ids[np.argsort(-covis_num)]
        else:
            # get covisible image ids with top k number of common voxels
            ind_top = np.argpartition(covis_num, -num_matched)
            ind_top = ind_top[-num_matched:]  # unsorted top k
            ind_top = ind_top[np.argsort(-covis_num[ind_top])]
            top_covis_ids = [covis_ids[i] for i in ind_top]
            assert covis_num[ind_top[0]] == np.max(covis_num)

        for i in top_covis_ids:
            pair = (image.name, images[i].name)
            pairs.append(pair)

    logger.info(f"Found {len(pairs)} pairs.")
    with open(output, "w") as f:
        f.write("\n".join(" ".join([i, j]) for i, j in pairs))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--num_matched", required=True, type=int)
    parser.add_argument("--voxel_size", default=0.2, type=float)
    args = parser.parse_args()
    main(**args.__dict__)
