import matplotlib.pyplot as plt
from hloc.utils import viz_3d
import numpy as np
from numpy.linalg import norm
from plotly import graph_objects as go


from utils.data_utils import get_sorted_images


def plot_loss_breakdown_total(breakdown):
    
    fig, ax = plt.subplots()

    ax.bar(breakdown.keys(),
           [data["sum"] for data in breakdown.values()])
    ax.set_xticks(ax.get_xticks(), ax.get_xticklabels(), rotation=40, ha='right')
    plt.show()


# Plot how each loss category is distributed over the trajectory
# Useful to balance loss weights, or understand if certain frames have a much higher loss (e.g. due to ARKit loop closures)
def plot_loss_breakdown_per_image_id(breakdown_per_image_id):

    all_image_ids = breakdown_per_image_id.keys()
    first_image_id = list(all_image_ids)[0]
    categories = breakdown_per_image_id[first_image_id]
    series = {c: [] for c in categories}

    for image_id, breakdown in breakdown_per_image_id.items():
        for category, data in breakdown.items():
            series[category].append(data["sum"])

    fig, ax = plt.subplots()
    ax.stackplot(all_image_ids, series.values(),
                labels=series.keys(), alpha=0.8)
    ax.legend(loc='upper left', reverse=True)
    ax.set_title('Loss over Trajectory')
    ax.set_xlabel('Image ID')
    ax.set_ylabel('Loss')

    plt.show()


def plot_loss_breakdown(breakdown, breakdown_per_image_id):
    plot_loss_breakdown_total(breakdown)
    plot_loss_breakdown_per_image_id(breakdown_per_image_id)


def plot_loss_details_history(loss_details_history):
    initial_breakdown, initial_breakdown_per_img, _, _ = loss_details_history[0]
    plot_loss_breakdown(initial_breakdown, initial_breakdown_per_img)

    for loss_details in loss_details_history:
        _, _, final_breakdown, final_breakdown_per_img = loss_details
        plot_loss_breakdown(final_breakdown, final_breakdown_per_img)


def plot_trajectory(fig, sorted_images, color, alignment_transform=None, width=4):
    positions = [img.cam_from_world.inverse().translation for img in sorted_images]
    if(alignment_transform is not None):
        positions = [alignment_transform * np.array(pos) for pos in positions]
    trajectory = go.Scatter3d(
        x=[pos[0] for pos in positions],
        y=[pos[1] for pos in positions],
        z=[pos[2] for pos in positions],
        mode="lines",
        name="trajectory",
        line=dict(color=color, width=width),
        showlegend=False
    )
    fig.add_trace(trajectory)


def plot_helper(reconstruction, reference_reconstruction=None, show_points=True):
    fig = viz_3d.init_figure()

    sorted_images = get_sorted_images(reconstruction.images.values())

    viz_3d.plot_reconstruction(fig, reconstruction, color='rgba(255,0,0,0.5)', name="mapping", points_rgb=True, points=show_points)

    plot_trajectory(fig, sorted_images, 'rgba(255,0,255,1.0)')

    if(reference_reconstruction is not None):
        reference_sorted_images = get_sorted_images(reference_reconstruction.images.values())
        #from_pose = reference_sorted_images[0].cam_from_world.inverse()
        to_pose = sorted_images[0].cam_from_world.inverse()
        alignment = sorted_images[0].cam_from_world.inverse() *  reference_sorted_images[0].cam_from_world
        plot_trajectory(fig, reference_sorted_images, 'rgba(255,255,1.0,0.8)', alignment_transform=alignment, width=3)

    fig.update_scenes(camera_projection_type="perspective")

    fig.show()


def evaluate_scanned_qr_codes(qr_world_detections, measure_pairs=None, truth_pairs=None):
    
    print()
    for short_id, poses in qr_world_detections.items():
        #print("poses", poses)
        positions = [pose.translation for pose in poses]
        up_vecs = [pose.rotation * np.array([1.0, 0.0, 0.0]) for pose in poses]
        right_vecs = [pose.rotation * np.array([0.0, 1.0, 0.0]) for pose in poses]

        pos_deviation = np.mean(np.std(np.array(positions), axis=0))
        up_deviation = np.mean(np.std(np.array(up_vecs), axis=0))
        right_deviation = np.mean(np.std(np.array(right_vecs), axis=0))
        #print(up_vecs)
        print(f"{short_id}: pos_deviation {pos_deviation}, up_deviation {up_deviation}, right_deviation {right_deviation}")
        #print(positions)
        #print("STD DEV:", std_deviation)

    all_heights = []
    for qr_id, poses in qr_world_detections.items():
        for pose in poses:
            all_heights.append(pose.translation[0])
    print(all_heights)
    print("Average height:", np.mean(all_heights))
    print("Height deviation:", np.std(all_heights))

    if measure_pairs is not None:

        for i, pair in enumerate(measure_pairs):
            a, b = measure_pairs[i]
            pos1 = qr_world_detections[a][0].translation
            pos2 = qr_world_detections[b][0].translation
            offset = pos1 - pos2
            offset[0] = 0 # Snap floor height
            distances = []
            for pose_a in qr_world_detections[a]:
                for pose_b in qr_world_detections[b]:
                    distances.append(norm(pose_a.translation - pose_b.translation))
            percent_vs_truth = (norm(offset) / truth_pairs[i] - 1) * 100
            print(f"{a} - {b}: {norm(offset):.4f},"
                  f"{'+' if percent_vs_truth > 0 else ''}{percent_vs_truth:.2f}%,",
                  f"{'+' if percent_vs_truth > 0 else ''}{(norm(offset) - truth_pairs[i]) * 100.0:.2f} cm,",
                  f"(truth:{truth_pairs[i]:.5f}). (spread {np.std(distances):.4f})")