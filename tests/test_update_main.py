import argparse
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import update_main

REPO_ROOT = Path(__file__).resolve().parents[1]


class UpdateMainFailureTests(unittest.TestCase):
    """A failed update must exit non-zero and leave fail_reason.txt and log.txt in the job root."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.job_root = Path(self._tmp.name)
        # The update runner passes --data_path <job_root>/refined/local --output_path <job_root>/refined/update.
        self.data_path = self.job_root / "refined" / "local"
        self.output_path = self.job_root / "refined" / "update"
        self.data_path.mkdir(parents=True)

    def tearDown(self):
        self._tmp.cleanup()

    def test_no_refined_scans(self):
        args = argparse.Namespace(
            data_path=self.data_path, output_path=self.output_path, domain_id="d1", job_id="job_1", log_level="INFO"
        )
        with self.assertRaises(RuntimeError):
            update_main.main(args)
        self.assertIn("no refined scans found", (self.job_root / "fail_reason.txt").read_text())
        self.assertIn("Update refinement failed", (self.job_root / "log.txt").read_text())

    def test_pipeline_error_exits_non_zero(self):
        # A refined scan but no reference map under refined/global: update_helper raises.
        (self.data_path / "2026-01-01_00-00-00" / "sfm").mkdir(parents=True)
        proc = subprocess.run(
            [sys.executable, "update_main.py", "--data_path", str(self.data_path), "--output_path", str(self.output_path)],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        self.assertNotEqual(proc.returncode, 0, proc.stderr[-2000:])
        self.assertTrue((self.job_root / "fail_reason.txt").read_text().strip())
        self.assertIn("Update refinement failed", (self.job_root / "log.txt").read_text())


if __name__ == "__main__":
    unittest.main()
