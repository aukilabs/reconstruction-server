"""Topology: extract floor and mesh files from a point cloud or COLMAP model."""
from pathlib import Path
import argparse

from reconstruction.log import configure
from reconstruction.pipeline.topology.mesh import main as topology_main


def parse_args(argv, prog=None):
    parser = argparse.ArgumentParser(prog=prog, description="Topology mesh extraction")
    parser.add_argument('--input_path', type=Path, required=True)
    parser.add_argument('--output_dir', type=Path, required=True)
    parser.add_argument('--floor_height', type=float, default=0.0)
    parser.add_argument('--floor_height_threshold', type=float, default=0.2, help='Max height offset from [floor_height] to count as floor, in meters')
    parser.add_argument('--voxel_size', type=float, default=0.05, help='Voxel size for clustering, in meters')

    return parser.parse_args(argv)


def main(args):
    configure()
    topology_main(args)
