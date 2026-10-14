#!/usr/bin/env python3
"""Smoke test for hloc device selection & basic torch functionality."""

from __future__ import annotations

import sys


def main() -> int:
    import torch

    import hloc
    from hloc.utils.inference_device import select_inference_device

    print("hloc:", getattr(hloc, "__file__", "?"))
    print("hloc version:", getattr(hloc, "__version__", "?"))
    print("torch:", torch.__version__)

    name = select_inference_device()
    dev = torch.device(name)
    print("inference device:", name)

    # Same shape class as descriptor similarity (e.g. pairs_from_retrieval matmul); no weights / I/O.
    a = torch.randn(32, 128, device=dev, dtype=torch.float32)
    b = torch.randn(128, 64, device=dev, dtype=torch.float32)
    c = a @ b
    if c.device.type != dev.type:
        print("FAIL: result on wrong device", file=sys.stderr)
        return 1
    if tuple(c.shape) != (32, 64):
        print("FAIL: bad shape", c.shape, file=sys.stderr)
        return 1
    # Touch result on host (sync for MPS/CUDA).
    _ = float(c.cpu().sum())
    print("OK: matmul on", name)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ModuleNotFoundError as e:
        print("FAIL:", e, file=sys.stderr)
        raise SystemExit(1) from e
