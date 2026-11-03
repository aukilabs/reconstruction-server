import importlib
import pkgutil
import subprocess
import sys
import unittest
from pathlib import Path

import reconstruction

REPO_ROOT = Path(__file__).resolve().parents[1]


class ImportTests(unittest.TestCase):
    def test_every_module_imports_on_its_own(self):
        # A fresh interpreter per module, so an import cycle that another import order hides
        # still fails here.
        modules = [m.name for m in pkgutil.walk_packages(reconstruction.__path__, "reconstruction.")
                   if m.name != "reconstruction.__main__"]
        self.assertGreater(len(modules), 40)
        script = "import importlib, sys\nfor m in sys.argv[1:]:\n    importlib.import_module(m)\n"
        for module in modules:
            with self.subTest(module=module):
                proc = subprocess.run([sys.executable, "-c", script, module], cwd=REPO_ROOT,
                                      capture_output=True, text=True)
                self.assertEqual(proc.returncode, 0, proc.stderr[-1500:])

    def test_pipeline_does_not_import_entrypoints(self):
        # Dependencies point from __main__ and entrypoints/ to the rest, never back.
        for module in pkgutil.walk_packages(reconstruction.__path__, "reconstruction."):
            if module.name.startswith(("reconstruction.entrypoints", "reconstruction.__main__")) or module.ispkg:
                continue
            origin = Path(importlib.util.find_spec(module.name).origin)
            if origin.suffix != ".py":  # native modules
                continue
            source = origin.read_text()
            self.assertNotIn("reconstruction.entrypoints", source, module.name)


if __name__ == "__main__":
    unittest.main()
