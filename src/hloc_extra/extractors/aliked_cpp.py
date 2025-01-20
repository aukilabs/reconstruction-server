from ..utils.base_model import BaseModel
import torch
import sys
from pathlib import Path

# Import the C++ implementation
lib_path = "/app/Light_Glue_CPP/build/lib/release"
sys.path.append(lib_path)
import lightglue_cpp

class ALIKED_CPP(BaseModel):
    default_conf = {
        "model_name": "aliked-n16",
        "max_num_keypoints": -1,
        "detection_threshold": 0.2,
        "nms_radius": 2,
    }
    required_inputs = ["image"]

    def _init(self, conf):
        conf.pop("name")
        self.model = lightglue_cpp.ALIKED(**conf)  # Use C++ implementation

    def _forward(self, data):
        # Get image tensor directly
        image = data["image"]
        
        # Call C++ implementation with tensor
        features = self.model(image)
        
        # Features are already torch tensors, just return them
        return {
            "keypoints": features["keypoints"],
            "keypoint_scores": features["keypoint_scores"],
            "descriptors": features["descriptors"],
        } 