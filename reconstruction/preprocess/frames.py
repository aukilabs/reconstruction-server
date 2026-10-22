"""Frame extraction from Frames.mp4 and frame selection."""
import os
import cv2


def mp4_to_frames(mp4_path, frames_path, filename_prefix=""):
    capture = cv2.VideoCapture(mp4_path)
    frame_count = 0
    print("Unpacking mp4 to frames:", mp4_path, "->", frames_path)
    while capture.isOpened():
        ret, frame = capture.read()
        if not ret:
            break
        img_path = f"{frames_path}/{filename_prefix}{frame_count:06d}.jpg"
        if not os.path.exists(img_path):
            cv2.imwrite(img_path, frame)
        frame_count += 1
    print(f"Unpacked {frame_count} frames from mp4")
    capture.release()


def process_frames(
    paths,
    every_nth_image,
    logger
):
    """
    Process and extract frames from video if necessary.

    Returns:
        Tuple of (reference_list, use_frames_from_video)
    """
    frames_mp4 = paths.scan_folder / 'Frames.mp4'
    logger.info(f"Looking for mp4 encoded frames: {frames_mp4}")

    use_frames_from_video = False
    if frames_mp4.exists():
        logger.info(f"Frames mp4 found, unpacking into {paths.images}")
        if not paths.images.exists():
            paths.images.mkdir()
        mp4_to_frames(frames_mp4, paths.images, filename_prefix=f"{paths.scan_folder.name}_")
        use_frames_from_video = True

    references = [str(p.relative_to(paths.images)) for p in paths.images.iterdir()]

    original_image_count = len(references)
    references = references[::every_nth_image]
    logger.info(f'{len(references)}, frames selected, out of, {original_image_count}')
    return sorted(references), use_frames_from_video, original_image_count
