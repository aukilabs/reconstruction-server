from ..utils.base_model import BaseModel
import torch
import sys
from pathlib import Path

# Import the C++ implementation
lib_path = "/app/Light_Glue_CPP/build/lib/release"
sys.path.append(lib_path)
try:
    import lightglue_cpp
except ImportError:
    raise ImportError(f"Could not import lightglue_cpp. Make sure to build the C++ implementation first. Looking in: {lib_path}")

class LightGlue_CPP(BaseModel):
    default_conf = {
        "features": "superpoint",
        "depth_confidence": 0.95,
        "width_confidence": 0.99,
    }
    required_inputs = [
        "image0",
        "keypoints0",
        "descriptors0",
        "image1",
        "keypoints1",
        "descriptors1",
    ]

    def _init(self, conf):
        self.net = lightglue_cpp.LightGlue(conf.pop("features"), **conf)  # Use C++ implementation

    def _forward(self, data):
        # Convert data to format expected by C++ implementation
        data0 = {
            "image": data["image0"],
            "keypoints": data["keypoints0"].cpu().numpy(),
            "descriptors": data["descriptors0"].transpose(-1, -2).cpu().numpy(),
        }
        data1 = {
            "image": data["image1"],
            "keypoints": data["keypoints1"].cpu().numpy(),
            "descriptors": data["descriptors1"].transpose(-1, -2).cpu().numpy(),
        }

        # Call C++ implementation
        matches = self.net(data0, data1)

        # Convert back to expected format
        return {
            "matches0": torch.from_numpy(matches["matches0"]) if matches["matches0"] is not None else None,
            "matches1": torch.from_numpy(matches["matches1"]) if matches["matches1"] is not None else None,
            "matching_scores0": torch.from_numpy(matches["matching_scores0"]) if matches["matching_scores0"] is not None else None,
            "matching_scores1": torch.from_numpy(matches["matching_scores1"]) if matches["matching_scores1"] is not None else None,
        } 