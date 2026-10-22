import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(*args):
    return subprocess.run([sys.executable, *args], cwd=REPO_ROOT, capture_output=True, text=True)


class GlobalMainCliTests(unittest.TestCase):
    def test_runs_directly(self):
        # global_main used to read args.log_format without defining --log_format.
        with tempfile.TemporaryDirectory() as tmp:
            datasets = Path(tmp) / "datasets"
            datasets.mkdir()
            proc = _run("global_main.py", "--data_dir", str(datasets), "--output_path", str(Path(tmp) / "out"))
            self.assertNotIn("has no attribute 'log_format'", proc.stderr)
            self.assertTrue((Path(tmp) / "out" / "global_logs").exists(), proc.stderr[-2000:])


if __name__ == "__main__":
    unittest.main()
