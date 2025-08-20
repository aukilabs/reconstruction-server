"""
Note: the functions in this file depend on the 'evo' package for evaluation utilities.
It is intended for local development and is not required in deployed Docker images.
To use these functions locally, install evo manually:

    pip install evo

"""

import pycolmap
import numpy as np
from numpy.linalg import norm

from evo.main_ape import ape as evo_ape
from evo.core.trajectory import PosePath3D
from evo.core.trajectory import geometry
from evo.core.metrics import PoseRelation
from evo.core import lie_algebra as evo_lie
import matplotlib.pyplot as plt

def portals_to_evo_path(pose_per_qr, flatten=False):
    positions_xyz = []
    quats_wxyz = []
    for qr_id, pose in pose_per_qr.items():
        if not isinstance(pose, pycolmap.Rigid3d):
            raise Exception(f"Wrong value type for pose of QR {qr_id}, in portals_to_evo_path. Must be pycolmap.Rigid3d, got: {pose}")

        positions_xyz.append(np.array([
            0.0 if flatten else pose.translation[0],
            pose.translation[1],
            pose.translation[2]
        ]))

        quat = np.array([
            pose.rotation.quat[3], # Evo library uses WXYZ !!!
            pose.rotation.quat[0],
            0.0 if flatten else pose.rotation.quat[1],
            0.0 if flatten else pose.rotation.quat[2]
        ])
        if flatten:
            quat /= norm(quat)

        quats_wxyz.append(quat)

    return PosePath3D(positions_xyz, quats_wxyz)


def compare_portals(initial, estimate, reference, align=False, correct_scale=False, verbose=False):

    filtered_reference = {qr_id: reference[qr_id] for qr_id in estimate.keys()}

    ini_pose_path = portals_to_evo_path(initial, flatten=True)
    est_pose_path = portals_to_evo_path(estimate, flatten=True)
    ref_pose_path = portals_to_evo_path(filtered_reference, flatten=True)

    if verbose:
        print("Initial:", ini_pose_path)
        print(", ".join(f"{qr_id}: {initial[qr_id].rotation.quat}" for qr_id in initial))
        print("Estimate:", est_pose_path)
        print(", ".join(f"{qr_id}: {estimate[qr_id].rotation.quat}" for qr_id in estimate))
        print("Reference:", ref_pose_path)
        print(", ".join(f"{qr_id}: {filtered_reference[qr_id].rotation.quat}" for qr_id in filtered_reference))
        print("")

    if align or correct_scale:
        # ONLY rotate around world up (don't rely on alignment to fix height drift)
        # Load again temporarily to flatten and compute alignment.
        # Then apply alignment on original paths which we DON'T flatten.
        # This gives a more fair measurement and also works with wall portals
        """
        def flatten(points):
            return np.array([np.array([0.0, p[1], p[2]]) for p in points])

        rotation, translation, scaling = geometry.umeyama_alignment(flatten(est_pose_path.positions_xyz).T,
                                                                    flatten(ref_pose_path.positions_xyz).T,
                                                                    correct_scale)


        #print(f"Umeyama: translation={translation},\nrotation=\n{rotation},\nscaling={scaling}")

        if correct_scale:
            est_pose_path.scale(scaling)
        if align:
            est_pose_path.transform(evo_lie.se3(rotation, translation))

            # Align again without flattening, to get also the height right (but not rotating again)
            _, translation_2, scaling_2 = geometry.umeyama_alignment(est_pose_path.positions_xyz.T,
                                                                     ref_pose_path.positions_xyz.T,
                                                                     correct_scale)

            #print(f"Umeyama 2: translation={translation_2},\nscaling={scaling_2}")
            if correct_scale:
                est_pose_path.scale(scaling_2)
            if align:
                est_pose_path.transform(evo_lie.se3(np.identity(3), translation_2))
        """


        rotation, translation, scaling = geometry.umeyama_alignment(est_pose_path.positions_xyz.T,
                                                                    ref_pose_path.positions_xyz.T,
                                                                    correct_scale)
        ini_pose_path.scale(scaling)
        ini_pose_path.transform(evo_lie.se3(rotation, translation))


    pos_comparison = evo_ape(ref_pose_path, est_pose_path, PoseRelation.point_distance,
                             align=align, correct_scale=correct_scale)

    rot_comparison = evo_ape(ref_pose_path, est_pose_path, PoseRelation.rotation_angle_deg,
                             align=align, correct_scale=correct_scale)

    if verbose:
        print(pos_comparison.pretty_str())
        print(rot_comparison.pretty_str())
        
        # fig = plt.figure()
        # traj_by_label = {
        #     "estimate": est_pose_path,
        #     "reference": ref_pose_path
        # }
        # evo_plot.trajectories(fig, traj_by_label, evo_plot.PlotMode.yz)
        
        # Scatter plot to compare portal poses
        fig = plt.figure(figsize=(14, 9))
        ax = fig.add_subplot(111)

        colors = plt.get_cmap('tab10').colors
        color_0 = np.array(colors[0]).reshape(1,-1)
        color_1 = np.array(colors[1]).reshape(1,-1)
        color_2 = np.array(colors[2]).reshape(1,-1)

        ax.scatter(ini_pose_path.positions_xyz[:, 1], ini_pose_path.positions_xyz[:, 2], label='initial',
                   c=color_0, marker="x", s=30)

        ax.scatter(est_pose_path.positions_xyz[:, 1], est_pose_path.positions_xyz[:, 2], label='optimized',
                   c=color_1, marker="x", s=15)

        ax.scatter(ref_pose_path.positions_xyz[:, 1], ref_pose_path.positions_xyz[:, 2], label='measured truth',
                   c=color_2, marker="x", s=15)

        ax.set_xlabel('Y axis')
        ax.set_ylabel('Z axis')
        ax.legend()
        plt.show()

    if verbose:
        print()
        print("Absolute Position Error (m):", pos_comparison.stats)
        print("Absolute Rotation Error (°):", rot_comparison.stats)

    print(f"Portal Accuracy (APE):",
          f" RMSE: {pos_comparison.stats['rmse']:.5f} m, {rot_comparison.stats['rmse']:.5f}°",
          f"  Max: {pos_comparison.stats['max']:.5f} m, {rot_comparison.stats['max']:.5f}°")

    return pos_comparison, rot_comparison
