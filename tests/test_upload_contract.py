"""Upload contract between the Python pipeline and the Rust runners.

After Python exits, the Rust runners upload a fixed set of files from the job root:
- server/rust/runner-reconstruction-local/src/refined.rs: one RefinedScan.zip per scan
- server/rust/runner-reconstruction-common/src/output.rs: the final global and update outputs
- server/rust/runner-reconstruction-*/src/lib.rs: the task log

reconstruction/output/uploads.py holds the Python-side copy of those tables (UPLOAD_TABLE).
These tests parse the Rust sources, so changing the paths, names, data types or required flags
on either side fails here first. They only read files and run without the Docker image.
"""
import re
import unittest
from pathlib import Path

from reconstruction.output.uploads import (
    REFINED_SCAN_REQUIRED_FILES,
    REFINED_SCAN_ZIP_EXTENSIONS,
    UPLOAD_TABLE,
    UploadSpec,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
RUST_ROOT = REPO_ROOT / "server" / "rust"


def _read(rel_path):
    return (RUST_ROOT / rel_path).read_text(encoding="utf-8")


def _rust_str_array(source, const_name):
    match = re.search(rf"const {const_name}: &\[&str\] = &\[(.*?)\];", source, re.S)
    if match is None:
        raise AssertionError(f"{const_name} not found")
    return tuple(re.findall(r'"([^"]*)"', match.group(1)))


def _rust_final_outputs(runner):
    """Rebuild this runner's part of UPLOAD_TABLE from the common crate's output.rs and strategy.rs."""
    output_rs = _read("runner-reconstruction-common/src/output.rs")
    strategy_rs = _read("runner-reconstruction-common/src/strategy.rs")

    table_rs = re.search(rf"const {runner.upper()}_OUTPUTS: &\[OutputSpec\] = &\[(.*?)\n\];", output_rs, re.S)
    if table_rs is None:
        raise AssertionError(f"{runner.upper()}_OUTPUTS not found")
    specs = re.findall(
        r'OutputSpec \{\s*relative_path: "([^"]+)",\s*display_name: "([^"]+)",\s*mandatory: (true|false),\s*\}',
        table_rs.group(1),
    )
    type_fn = re.search(r"fn data_type_for_display\(.*?\n\}", output_rs, re.S).group(0)
    data_types = dict(re.findall(r'"([^"]+)" => "([^"]+)"', type_fn))
    fallback = re.search(r'_ => "([^"]+)"', type_fn).group(1)
    table = [
        UploadSpec(runner, path, name, data_types.get(name, fallback), mandatory == "true")
        for path, name, mandatory in specs
    ]

    json_files = re.findall(r'upload_json_if_exists\(\s*sink,\s*"([^"]+)"', output_rs)
    json_types = {
        path: (name, data_type)
        for path, name, data_type in re.findall(
            r'"([^"]+\.json)" => Some\(\(name\("([^"]+)"\), "([^"]+)"\.into\(\)\)\)', strategy_rs
        )
    }
    for path in json_files:
        name, data_type = json_types[path]
        table.append(UploadSpec(runner, path, name, data_type, False))
    return table


class UploadContractTests(unittest.TestCase):
    def _table(self, runner, exclude_paths=()):
        return [s for s in UPLOAD_TABLE if s.runner == runner and s.path not in exclude_paths]

    def test_global_and_update_outputs_match_rust(self):
        for runner in ("global", "update"):
            with self.subTest(runner=runner):
                self.assertEqual(_rust_final_outputs(runner), self._table(runner, exclude_paths=("log.txt",)))

    def test_refined_scan_zip_matches_rust(self):
        refined_rs = _read("runner-reconstruction-local/src/refined.rs")
        self.assertEqual(_rust_str_array(refined_rs, "REQUIRED_SFM_FILES"), REFINED_SCAN_REQUIRED_FILES)
        self.assertEqual(_rust_str_array(refined_rs, "ZIP_ALLOWED_EXTENSIONS"), REFINED_SCAN_ZIP_EXTENSIONS)

        (spec,) = self._table("local", exclude_paths=("log.txt",))
        rust_path = spec.path.replace("{scan}", "{}")
        rust_name = spec.name.replace("{scan}", "{}")
        self.assertIn(f'format!("{rust_path}", scan_id)', refined_rs)
        self.assertIn(f'name: &format!("{rust_name}", scan_id)', refined_rs)
        self.assertIn(f'data_type: "{spec.data_type}"', refined_rs)
        # Missing sfm files skip the scan instead of failing the task.
        self.assertIn("if !upload_local_zips || !has_required_sfm_files(&sfm_path) {\n                continue;", refined_rs)

    def test_task_log_matches_rust(self):
        for runner in ("local", "global", "update"):
            with self.subTest(runner=runner):
                (spec,) = [s for s in self._table(runner) if s.path == "log.txt"]
                lib_rs = _read(f"runner-reconstruction-{runner}/src/lib.rs")
                self.assertIn(f'workspace.root().join("{spec.path}")', lib_rs)
                self.assertIn('format!("logs/{task_id}.txt")', lib_rs)
                self.assertIn(f'format!("{spec.name}")', lib_rs)
                self.assertIn(f'data_type: "{spec.data_type}"', lib_rs)
                # A missing log is a warning, not a task failure.
                self.assertIn('warn!(error = %err, task_id = %task_id, "failed to upload task log");', lib_rs)


if __name__ == "__main__":
    unittest.main()
