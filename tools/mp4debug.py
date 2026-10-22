import cv2
from pathlib import Path

def mp4_count_frames(mp4_paths):
    for mp4_path in mp4_paths:
        try:
            mp4_path = str(mp4_path)
            capture = cv2.VideoCapture(mp4_path)
            frame_count = 0
            while capture.isOpened():
                ret, frame = capture.read()
                if not ret:
                    break
                frame_count += 1
            
            frames_csv_path = mp4_path.replace(".mp4", ".csv")
            with open(frames_csv_path, "r") as f:
                expected_frame_count = len([line for line in f.readlines() if line.strip() and ',' in line])
            print(f"{mp4_path}: {frame_count} frames, expected {expected_frame_count}")
            if frame_count != expected_frame_count:
                raise Exception(f"Frame count mismatch: {frame_count} != {expected_frame_count}")

        except Exception as e:
            print(f"{mp4_path}: Failed to count frames, {e}")
        finally:
            if capture:
                capture.release()


if __name__ == "__main__":
    paths = Path("/app/mp4dbg/").glob("**/Frames.mp4")
    mp4_count_frames(paths)
