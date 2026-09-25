"""Depth Anything 3 pose-conditioned inference wrapper (documented API only)."""

from __future__ import annotations

from typing import Any, Sequence

import numpy as np


def load_model(model_id: str = "depth-anything/DA3-BASE", device: str = "cuda") -> Any:
    """Load DA3 via the public Hugging Face / API entry points.

    Docs: https://github.com/ByteDance-Seed/Depth-Anything-3/blob/main/docs/API.md
    Model card: depth-anything/DA3-BASE (Apache 2.0, pose conditioning supported).
    """
    import torch
    from depth_anything_3.api import DepthAnything3

    model = DepthAnything3.from_pretrained(model_id)
    model = model.to(device=torch.device(device))
    model.eval()
    return model


def run_pose_conditioned(
    model: Any,
    image_paths: Sequence[str],
    extrinsics: np.ndarray,
    intrinsics: np.ndarray,
    *,
    #align_to_input_ext_scale: bool = True,
    process_res: int = 720,
    #process_res_method: str = "upper_bound_resize",
    #use_ray_pose: bool = False,
    #ref_view_strategy: str = "saddle_balanced",
) -> Any:
    """Run pose-conditioned depth estimation.

    Official signature (docs/API.md):
      model.inference(image=..., extrinsics=(N,4,4), intrinsics=(N,3,3), ...)
    Extrinsics are world-to-camera; when provided, DA3 runs pose-conditioned mode.
    """
    if extrinsics.ndim != 3 or extrinsics.shape[1:] not in {(4, 4), (3, 4)}:
        raise ValueError(f"extrinsics must be (N,4,4) or (N,3,4), got {extrinsics.shape}")
    if intrinsics.ndim != 3 or intrinsics.shape[1:] != (3, 3):
        raise ValueError(f"intrinsics must be (N,3,3), got {intrinsics.shape}")
    if len(image_paths) != extrinsics.shape[0] or len(image_paths) != intrinsics.shape[0]:
        raise ValueError("image_paths, extrinsics, and intrinsics length/N must match")

    # DA3 docs show (N,4,4); pad (N,3,4) if needed.
    if extrinsics.shape[1:] == (3, 4):
        padded = np.zeros((extrinsics.shape[0], 4, 4), dtype=np.float64)
        padded[:, :3, :4] = extrinsics
        padded[:, 3, 3] = 1.0
        extrinsics = padded

    prediction = model.inference(
        image=list(image_paths),
        extrinsics=extrinsics.astype(np.float32),
        intrinsics=intrinsics.astype(np.float32),
        align_to_input_ext_scale=True,
        process_res=process_res,
        process_res_method="upper_bound_resize",
        use_ray_pose=False,
        #ref_view_strategy="middle",
        infer_gs=False,
    )
    return prediction
