import os
import cv2
import csv
import argparse

def get_image_names(folder_path, valid_extensions=None):
    """
    Get a list of image file names in the given folder.

    :param folder_path: Path to the folder containing images.
    :param valid_extensions: List of valid file extensions (e.g., ['.jpg', '.png']). If None, all files are included.
    :return: List of image file names.
    """
    if valid_extensions is None:
        valid_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.tiff', '.webp']

    image_names = [
        file for file in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, file)) and os.path.splitext(file)[1].lower() in valid_extensions
    ]
    return image_names

def plot_coordinates_on_image(portal_file, frames_ts_file, images_folder, output_folder):
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    images = get_image_names(images_folder, ".jpg")
    images = sorted(images)

    timestamps = []
    with open(frames_ts_file, 'r') as ffile:
        csv_reader = csv.reader(ffile)
        for row in csv_reader:
            timestamps.append(row[0])
    
    # Read the CSV file
    with open(portal_file, 'r') as pfile:
        csv_reader = csv.reader(pfile)
        for row in csv_reader:
            if len(row) != 17:
                print(f"Invalid row: {row}")
                continue

            # Extract image name and coordinates
            frame_timestamp = row[0]
            try:
                coordinates = [float(coord) for coord in row[9:]]
            except ValueError:
                print(f"Invalid coordinates in row: {row}")
                continue

            # Group coordinates into (x, y) pairs
            points = [(coordinates[i], coordinates[i + 1]) for i in range(0, len(coordinates), 2)]

            # Load the image
            image_name = images[timestamps.index(frame_timestamp)]
            image_path = os.path.join(images_folder, image_name)
            if not os.path.exists(image_path):
                print(f"Image not found: {image_name}")
                continue
            image = cv2.imread(image_path)
            height, width = image.shape[:2]
            # Plot the points on the image
            for point in points:
                x, y = int(point[0]), int(point[1])
                x = width - x
                y = height - y
                cv2.circle(image, (x, y), radius=5, color=(0, 0, 255), thickness=-1)  # Red dot

            # Save the output image
            output_path = os.path.join(output_folder, image_name)
            cv2.imwrite(output_path, image)
            print(f"frame_timestamp: {frame_timestamp}   Index: {timestamps.index(frame_timestamp)}    filename: {image_name}")
            print(f"Processed and saved: {output_path}")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Display SFM and Portal results")
    parser.add_argument('--dataset', type=str, help='Path to dataset folder', required=True)
    return parser.parse_args()   


if __name__ == "__main__":
    args = parse_arguments()

    portal_file = os.path.join(args.dataset, "PortalDetections.csv")
    frames_ts_file = os.path.join(args.dataset, "Frames.csv")
    images_folder = os.path.join(args.dataset, "Frames")
    output_folder = os.path.join(args.dataset, "DrawnFrames")


    plot_coordinates_on_image(portal_file, frames_ts_file, images_folder, output_folder)
