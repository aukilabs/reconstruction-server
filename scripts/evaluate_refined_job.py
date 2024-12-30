#!/usr/bin/env python3

import argparse
from pathlib import Path
import pycolmap
import numpy as np
import csv
import matplotlib.pyplot as plt
import os
import sys
import json

# Add the parent directory to Python path so we can import utils
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
sys.path.append(parent_dir)

from utils.dataset_utils import compare_portals
from utils.data_utils import save_qr_poses_csv, load_portals_json, convert_pose_opengl_to_colmap

def load_portal_poses_csv(csv_path):
    """Load portal poses from CSV file"""
    portal_poses = {}
    
    with open(csv_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            short_id = row[0]
            pos = np.array([float(row[1]), float(row[2]), float(row[3])])
            quat = np.array([float(row[4]), float(row[5]), float(row[6]), float(row[7])])
            pos, quat = convert_pose_opengl_to_colmap(pos, quat)
            portal_poses[short_id] = pycolmap.Rigid3d(pycolmap.Rotation3d(quat), pos)
    
    return portal_poses

def evaluate_job(job_folder, truth_portals_path=None, verbose=False):
    """Evaluate accuracy of a refined job by comparing portal poses"""
    job_path = Path(job_folder)
    
    # Find all local refined scans
    local_refined_dir = job_path / "refined" / "local"
    if not local_refined_dir.exists():
        print(f"No local refined directory found in {job_folder}/refined/")
        return
        
    # Get ground truth portal poses if provided
    truth_portal_poses = None
    if truth_portals_path:
        truth_portals_path = Path(truth_portals_path)
        print(f"Loading truth portals from {truth_portals_path}")
        if truth_portals_path.exists():
            truth_portal_poses = load_portals_json(truth_portals_path)
            print(f"Loaded {len(truth_portal_poses)} truth portal poses")
        else:
            print(f"Warning: Truth portals file not found at {truth_portals_path}")
        
    # Process each refined scan
    for scan_dir in local_refined_dir.iterdir():
        if not scan_dir.is_dir():
            continue
            
        print(f"\nEvaluating scan: {scan_dir.name}")
        
        # Check for unrefined poses
        unrefined_poses_path = scan_dir / "UnrefinedPortalPoses.csv"
        refined_poses_path = scan_dir / "RefinedPortalPoses.csv"
        
        # Load unrefined poses if they exist
        unrefined_poses = None
        if unrefined_poses_path.exists():
            unrefined_poses = load_portal_poses_csv(unrefined_poses_path)
            print(f"Loaded {len(unrefined_poses)} unrefined portal poses")
        
        # Load refined poses if they exist
        refined_poses = None
        if refined_poses_path.exists():
            refined_poses = load_portal_poses_csv(refined_poses_path)
            print(f"Loaded {len(refined_poses)} refined portal poses")
        
        if not refined_poses and not unrefined_poses:
            print("No portal poses found to compare")
            continue

        compare_portals(unrefined_poses, refined_poses, truth_portal_poses, align=True, verbose=verbose, correct_scale=False)
    
    # Compare globally refined pose

    print("\nComparing globally refined poses accuracy")

    global_refined_dir = job_path / "refined" / "global"
    refined_manifest_path = global_refined_dir / "refined_manifest.json"
    if refined_manifest_path.exists():
        with open(refined_manifest_path) as f:
            refined_manifest = json.load(f)
        global_refined_poses = {}
        for portal in refined_manifest["portals"]:
            pose = portal["pose"]
            position = pose["position"]
            rotation = pose["rotation"]
            quat = np.array([rotation["x"], rotation["y"], rotation["z"], rotation["w"]])
            pos = np.array([position["x"], position["y"], position["z"]])
            pos, quat = convert_pose_opengl_to_colmap(pos, quat)
            pose = pycolmap.Rigid3d(pycolmap.Rotation3d(quat), pos)
            global_refined_poses[portal["shortId"]] = pose

        print(f"Compare accuracy of {len(global_refined_poses)} portals.")
        compare_portals(None, global_refined_poses, truth_portal_poses, align=True, verbose=True, correct_scale=False, flatten=False)
    else:
        print(f"No globally refined manifest found at {refined_manifest_path}")

def main():
    parser = argparse.ArgumentParser(description="Evaluate accuracy of refined jobs")
    parser.add_argument("job_folder", help="Path to the job output folder")
    parser.add_argument("--truth", "-t", help="Path to the ground truth portals.json file", default=None)
    parser.add_argument("--verbose", "-v", help="Show detailed comparison information", action="store_true")
    args = parser.parse_args()
    
    evaluate_job(args.job_folder, args.truth, args.verbose)

if __name__ == "__main__":
    main()