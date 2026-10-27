import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(*args, cwd=REPO_ROOT):
    # Like the image: the package is found through PYTHONPATH, whatever the cwd.
    pythonpath = os.pathsep.join(filter(None, [str(REPO_ROOT), os.environ.get("PYTHONPATH")]))
    env = {**os.environ, "PYTHONPATH": pythonpath}
    return subprocess.run([sys.executable, *args], cwd=cwd, env=env, capture_output=True, text=True)


class EntryPointTests(unittest.TestCase):
    def test_stages(self):
        proc = _run("-m", "reconstruction", "--help")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        for stage in ("local", "global", "update", "topology"):
            self.assertIn(stage, proc.stdout)

    def test_unknown_stage(self):
        proc = _run("-m", "reconstruction", "stitch")
        self.assertEqual(proc.returncode, 2)
        self.assertIn("invalid choice", proc.stderr)

    def test_runs_from_another_cwd(self):
        # The runner starts Python from the node's cwd; nothing may depend on it being the repo root.
        with tempfile.TemporaryDirectory() as tmp:
            proc = _run("-m", "reconstruction", "update", "--help", cwd=tmp)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("--data_path", proc.stdout)

    def test_mode_must_match_the_subcommand(self):
        proc = _run("-m", "reconstruction", "local", "--mode", "global_refinement",
                    "--domain_id", "d1", "--job_id", "job_1")
        self.assertEqual(proc.returncode, 2)
        self.assertIn("invalid choice: 'global_refinement'", proc.stderr)

    def test_disabled_occlusion_box_is_not_imported(self):
        # occlusion_box_wrapper is disabled; its dependencies should not load on every run.
        # (shapely is not listed: trimesh imports it, and the io module uses trimesh.)
        heavy = ["reconstruction.pipeline.occlusion.occlusion_box", "sklearn", "alphashape", "matplotlib"]
        proc = _run("-c", "import sys, reconstruction.entrypoints.global_map, reconstruction.entrypoints.local; "
                          f"print([m for m in {heavy!r} if m in sys.modules])")
        self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
        self.assertEqual(proc.stdout.strip().splitlines()[-1], "[]")


class GlobalEntryPointTests(unittest.TestCase):
    def test_failure_writes_reason_and_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_root = Path(tmp)
            (job_root / "datasets").mkdir()
            proc = _run("-m", "reconstruction", "global", "--mode", "global_refinement",
                        "--job_root_path", str(job_root), "--output_path", str(job_root / "refined"),
                        "--domain_id", "d1", "--job_id", "job_1")
            self.assertNotEqual(proc.returncode, 0)
            self.assertTrue((job_root / "fail_reason.txt").read_text().strip(), proc.stderr[-2000:])
            self.assertIn("Refinement failed", (job_root / "log.txt").read_text())
            self.assertTrue((job_root / "job_manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
