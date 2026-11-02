"""Exercise the Spark image's GPU and native bindings without node credentials.

Run from /app: python3 -m scripts.smoke_test_spark
"""

import os
import platform
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import open3d as o3d
import pyceres
import pycolmap
import pycolmap.cost_functions
import torch
import torchvision

from src.cost_functions import PoseCenterConstraintCostFunction
from src.ply_export import export_ply_text
from src.reconstruction_merge import append_reconstruction
from utils.point_cloud_utils import reduce_decimals_ply


def main():
    assert platform.machine() == "aarch64"
    assert os.getuid() == 10001
    assert torch.cuda.is_available()
    assert torch.cuda.get_device_capability() == (12, 1)
    assert pycolmap.has_cuda

    # Match the scripts invoked by the Rust runners, including transitive imports.
    for entrypoint in ("main.py", "update_main.py"):
        subprocess.run(
            [sys.executable, f"/app/{entrypoint}", "--help"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    print("Local/global/update task entrypoints passed")

    # Execute cuBLAS, cuDNN, and a compiled torchvision GPU operator.
    matrix = torch.ones((128, 128), device="cuda")
    assert torch.allclose(matrix @ matrix, torch.full_like(matrix, 128))
    conv = torch.nn.Conv2d(3, 8, 3).cuda()
    assert torch.isfinite(conv(torch.randn(1, 3, 32, 32, device="cuda"))).all()
    kept = torchvision.ops.nms(
        torch.tensor([[0, 0, 10, 10], [1, 1, 9, 9]], device="cuda", dtype=torch.float32),
        torch.tensor([0.9, 0.8], device="cuda"),
        0.5,
    )
    assert kept.tolist() == [0]
    torch.cuda.synchronize()
    print(f"GPU operations passed: {torch.cuda.get_device_name()}, PyTorch {torch.__version__}")

    sift = pycolmap.Sift(pycolmap.FeatureExtractionOptions(), device=pycolmap.Device.cuda)
    pixels = np.random.default_rng(42).integers(0, 256, (256, 256), dtype=np.uint8)
    keypoints, descriptors = sift.extract(pixels)
    assert len(keypoints) > 0
    assert descriptors.shape == (len(keypoints), 128)
    print(f"COLMAP CUDA SIFT passed: {len(keypoints)} features")

    # A real solve catches incompatible Ceres/pybind ABIs between extensions.
    rotation = np.array([0.0, 0.0, 0.0, 1.0])
    translation = np.array([1.0, 2.0, 3.0])
    cost = PoseCenterConstraintCostFunction(np.zeros(3), np.ones(3))
    problem = pyceres.Problem()
    problem.add_residual_block(cost, None, [rotation, translation])
    problem.set_parameter_block_constant(rotation)
    options = pyceres.SolverOptions()
    options.linear_solver_type = pyceres.LinearSolverType.DENSE_QR
    options.logging_type = pyceres.LoggingType.SILENT
    summary = pyceres.SolverSummary()
    pyceres.solve(options, problem, summary)
    assert np.linalg.norm(translation) < 1e-6, translation
    print("Custom Ceres binding solve passed")

    reprojection = pycolmap.cost_functions.ReprojErrorCost(
        pycolmap.CameraModelId.PINHOLE, np.zeros(2), pycolmap.Rigid3d()
    )
    reprojection_problem = pyceres.Problem()
    point = np.array([0.0, 0.0, 1.0])
    intrinsics = np.array([1.0, 1.0, 0.0, 0.0])
    reprojection_problem.add_residual_block(reprojection, None, [point, intrinsics])
    print(f"COLMAP/Ceres binding interoperability passed: COLMAP {pycolmap.__version__}")

    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory) / "points.ply"
        float_source = Path(directory) / "points-float.ply"
        output = Path(directory) / "points.drc"
        reconstruction = pycolmap.Reconstruction()
        append_reconstruction(reconstruction, pycolmap.Reconstruction())
        export_ply_text(reconstruction, str(Path(directory) / "empty.ply"))
        cloud = o3d.geometry.PointCloud()
        cloud.points = o3d.utility.Vector3dVector(np.random.default_rng(42).random((100, 3)))
        assert o3d.io.write_point_cloud(str(source), cloud, write_ascii=True)
        assert len(o3d.io.read_point_cloud(str(source)).points) == 100
        reduce_decimals_ply(source, float_source, decimals=3)
        subprocess.run(
            ["/usr/local/bin/draco_encoder", "-i", str(float_source), "-o", str(output), "-point_cloud"],
            check=True,
            capture_output=True,
            text=True,
        )
        assert output.stat().st_size > 0
    print("Open3D point-cloud I/O and Draco encoding passed")

    for directory in (Path("/app/jobs"), Path.home() / ".cache"):
        with tempfile.TemporaryFile(dir=directory) as handle:
            handle.write(b"writable")
    print("Non-root job and model-cache directories are writable")


if __name__ == "__main__":
    main()
