from lightglue import LightGlue as LightGlue_

from ..utils.base_model import BaseModel


class LightGlue(BaseModel):
    compiled = None
    default_conf = {
        "features": "superpoint",
        "depth_confidence": 0.95,
        "width_confidence": 0.99,
        "compile_network": True,
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
        if conf.get("compile_network", False):
            if not LightGlue.compiled:
                LightGlue.compiled = LightGlue_(conf.pop("features"), **conf)
                LightGlue.compiled = LightGlue.compiled.eval().cuda()
                LightGlue.compiled.compile()
            self.net = LightGlue.compiled
        else:
            self.net = LightGlue_(conf.pop("features"), **conf)

    def _forward(self, data):
        data["descriptors0"] = data["descriptors0"].transpose(-1, -2)
        data["descriptors1"] = data["descriptors1"].transpose(-1, -2)

        return self.net(
            {
                "image0": {k[:-1]: v for k, v in data.items() if k[-1] == "0"},
                "image1": {k[:-1]: v for k, v in data.items() if k[-1] == "1"},
            }
        )
