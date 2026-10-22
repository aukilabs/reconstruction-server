"""Merging the aligned scans into one reconstruction."""
import pycolmap
import sys
import logging
from pathlib import Path
from typing import Optional
from reconstruction.native.reconstruction_merge import append_reconstruction
from reconstruction.pipeline.global_.alignment import AlignedScans
from reconstruction.layout import JobLayout


def merge_aligned_scans(
    aligned_scans: AlignedScans,
    job_root_path: Path,
    logger: Optional[logging.Logger] = None
) -> pycolmap.Reconstruction:
    if logger is None:
        logger = logging.getLogger("scan_alignment")
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            logger.addHandler(logging.StreamHandler(sys.stdout))

    combined_rec = pycolmap.Reconstruction()

    for i, scan_id in enumerate(aligned_scans.scan_ids):
        scan_path = JobLayout(job_root_path).refined_sfm(scan_id)
        rec = pycolmap.Reconstruction()
        logger.info(f"Align and merge scan {i} / {len(aligned_scans.scan_ids)}: {scan_id}")
        logger.debug(f"-- Reading ...")
        rec.read(scan_path)
        logger.debug(f"-- Aligning ...")
        rec.transform(aligned_scans.alignment_transforms[scan_id])
        logger.debug(f"-- Merging ...")
        append_reconstruction(combined_rec, rec)

        logger.debug(f"-- MERGED! Combined size: {combined_rec.num_images()} images, "
            f"{combined_rec.num_frames()} frames, {combined_rec.num_rigs()} rigs, "
            f"{combined_rec.num_cameras()} cameras, {combined_rec.num_points3D()} points3D")

    logger.info(f"Merged {len(aligned_scans.scan_ids)} scans successfully: {combined_rec}")
    return combined_rec
