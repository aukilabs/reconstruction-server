import logging
import tempfile
import unittest
import zipfile
from pathlib import Path

from reconstruction.pipeline.update.update import _get_refined_rec_dir

SCAN = "2026-01-01_00-00-00"


class RefinedRecDirTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.job_root = Path(self._tmp.name)
        self.scan_dir = self.job_root / "refined" / "local" / SCAN
        self.scan_dir.mkdir(parents=True)
        self.logger = logging.getLogger("test_dataset_utils")

    def tearDown(self):
        self._tmp.cleanup()

    def test_sfm_folder_by_default(self):
        rec_dir = _get_refined_rec_dir(True, self.job_root / "refined", SCAN, self.logger)
        self.assertEqual(rec_dir, self.scan_dir / "sfm")

    def test_zipped_reconstruction_is_extracted_under_the_job_root(self):
        with zipfile.ZipFile(self.scan_dir / "reconstruction_refined_x1.zip", "w") as zf:
            zf.writestr("images.bin", b"\x00")
        rec_dir = _get_refined_rec_dir(True, self.job_root / "refined", SCAN, self.logger)
        self.assertTrue(rec_dir.is_relative_to(self.job_root), rec_dir)
        self.assertEqual((rec_dir / "images.bin").read_bytes(), b"\x00")


if __name__ == "__main__":
    unittest.main()
