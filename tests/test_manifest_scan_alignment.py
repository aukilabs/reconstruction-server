"""Emit scanAlignmentTransforms.localToDomain for splatter 0.2 tip global splat."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


def _fake_sim3(scale, tx, ty, tz, qx=0.0, qy=0.0, qz=0.0, qw=1.0):
    return SimpleNamespace(
        scale=scale,
        translation=(tx, ty, tz),
        rotation=SimpleNamespace(quat=(qx, qy, qz, qw)),
    )


def _stub_heavy_deps():
    torch = mock.MagicMock()
    torch.cuda.is_available.return_value = False
    torch.version.cuda = None
    return {
        "pycolmap": mock.MagicMock(),
        "torch": torch,
        "GPUtil": mock.MagicMock(getGPUs=mock.MagicMock(return_value=[])),
        "psutil": mock.MagicMock(
            cpu_count=mock.MagicMock(return_value=1),
            cpu_percent=mock.MagicMock(return_value=0.0),
            virtual_memory=mock.MagicMock(
                return_value=SimpleNamespace(total=1, available=1, used=0, percent=0.0)
            ),
        ),
    }


class SaveManifestScanAlignmentTests(unittest.TestCase):
    def test_writes_local_to_domain_sim3(self):
        sim3 = _fake_sim3(1.25, 1.0, 2.0, 3.0)

        with tempfile.TemporaryDirectory() as tmpdir:
            job_root = Path(tmpdir)
            manifest_path = job_root / "refined_manifest.json"
            with mock.patch.dict(sys.modules, _stub_heavy_deps()):
                sys.modules.pop("reconstruction.output.manifest", None)
                from reconstruction.output.manifest import save_manifest_json

                save_manifest_json(
                    {},
                    manifest_path,
                    job_root,
                    job_status="refined",
                    scan_alignment_transforms={"scan_a": sim3},
                )
            manifest = json.loads(manifest_path.read_text())

        self.assertIn("scanAlignmentTransforms", manifest)
        entry = manifest["scanAlignmentTransforms"]["scan_a"]
        self.assertIn("localToDomain", entry)
        self.assertNotIn("localToGlobalColmap", entry)

        xform = entry["localToDomain"]
        self.assertEqual(xform["scale"], "1.25")
        self.assertEqual(xform["position"]["x"], "1.0")
        self.assertEqual(xform["position"]["y"], "2.0")
        self.assertEqual(xform["position"]["z"], "3.0")
        self.assertEqual(xform["rotation"]["x"], "0.0")
        self.assertEqual(xform["rotation"]["y"], "0.0")
        self.assertEqual(xform["rotation"]["z"], "0.0")
        self.assertEqual(xform["rotation"]["w"], "1.0")

    def test_omits_block_when_transforms_absent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job_root = Path(tmpdir)
            manifest_path = job_root / "refined_manifest.json"
            with mock.patch.dict(sys.modules, _stub_heavy_deps()):
                sys.modules.pop("reconstruction.output.manifest", None)
                from reconstruction.output.manifest import save_manifest_json

                save_manifest_json({}, manifest_path, job_root, job_status="refined")
            manifest = json.loads(manifest_path.read_text())

        self.assertNotIn("scanAlignmentTransforms", manifest)


if __name__ == "__main__":
    unittest.main()
