"""Merge per-scan features.h5 files into a single combined features file.

Used during global stitching so the localizer has one h5 covering all
database images across all scans.
"""

import h5py
import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


def merge_features_h5(
    scan_feature_paths: List[Path],
    output_path: Path,
) -> Path:
    """Merge multiple hloc features.h5 files into one.

    Each scan's features.h5 contains groups keyed by image filename.
    Since scan IDs are unique timestamps baked into the filenames,
    there should be no collisions.

    Args:
        scan_feature_paths: Paths to per-scan features.h5 files.
        output_path: Where to write the merged features.h5.

    Returns:
        Path to the merged file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_images = 0
    skipped_missing = 0
    with h5py.File(output_path, "w") as out_f:
        for feat_path in scan_feature_paths:
            if not feat_path.exists():
                logger.warning(f"Features file not found, skipping: {feat_path}")
                skipped_missing += 1
                continue
            with h5py.File(feat_path, "r") as in_f:
                for image_name in in_f.keys():
                    if image_name in out_f:
                        logger.warning(
                            f"Duplicate image name '{image_name}' across scans, "
                            f"skipping from {feat_path}"
                        )
                        continue
                    in_f.copy(image_name, out_f)
                    total_images += 1

    logger.info(
        f"Merged features from {len(scan_feature_paths) - skipped_missing} scans: "
        f"{total_images} images -> {output_path}"
    )
    return output_path
