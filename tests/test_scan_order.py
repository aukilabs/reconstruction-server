"""Scan folder discovery and oldest-first ordering for global and update refinement."""
import json
import tempfile
import unittest
from pathlib import Path

from reconstruction.io.scan import get_data_paths, sort_scan_paths
from reconstruction.layout import JobLayout


class DiscoveryTests(unittest.TestCase):
    def test_accepts_timestamp_dmt_scan_and_uuid_ignores_other(self):
        with tempfile.TemporaryDirectory() as tmp:
            group = Path(tmp)
            uuid = "8c4869c0-99c5-4515-8545-e3674d383338"
            for name in ["2025-03-19_10-57-00", "dmt_scan_abc", uuid, "other"]:
                (group / name).mkdir()
            (group / "not_a_dir").write_text("")
            _, paths = get_data_paths(group)
            self.assertEqual({p.name for p in paths}, {"2025-03-19_10-57-00", "dmt_scan_abc", uuid})


class SortScanPathsTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)

    def _global(self, names):
        """Global: scans under datasets/, refined sfm under refined/local/<scan>/sfm."""
        datasets = self.root / "datasets"
        for name in names:
            (datasets / name).mkdir(parents=True)
        _, paths = get_data_paths(datasets)
        return JobLayout(self.root), paths

    def _update(self, names):
        """Update: scans are the refined/local/<scan> folders themselves."""
        refined_local = self.root / "refined" / "local"
        for name in names:
            (refined_local / name).mkdir(parents=True)
        _, paths = get_data_paths(refined_local)
        return JobLayout.from_refined_local(refined_local), paths

    def _write_metadata(self, layout, scan, content):
        path = layout.refined_sfm(scan) / "capture_metadata.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content if isinstance(content, str) else json.dumps(content))
        return path

    def test_timestamp_scans_keep_name_order_and_precede_uuid_scans(self):
        early = "f0000000-0000-4000-8000-000000000001"
        late = "10000000-0000-4000-8000-000000000002"
        layout, paths = self._global([late, "2025-01-02_00-00-00", "dmt_scan_x", early, "2025-01-01_00-00-00"])
        self._write_metadata(layout, early, {"scanStartTime": "2020-01-01T12:00:00Z"})
        self._write_metadata(layout, late, {"scanStartTime": "2026-01-01T12:00:00Z"})

        ordered = sort_scan_paths(paths, layout)

        self.assertEqual(
            [p.name for p in ordered],
            ["2025-01-01_00-00-00", "2025-01-02_00-00-00", "dmt_scan_x", early, late],
        )

    def test_uuid_scans_order_by_instant_not_by_name_or_text(self):
        # 11:00+02:00 is 09:00Z, earlier than 10:00Z even though its text and name sort later.
        utc = "aaaaaaaa-0000-4000-8000-000000000001"
        offset = "bbbbbbbb-0000-4000-8000-000000000002"
        layout, paths = self._update([utc, offset])
        self._write_metadata(layout, utc, {"scanStartTime": "2026-01-01T10:00:00.1234567Z"})
        self._write_metadata(layout, offset, {"scanStartTime": "2026-01-01T11:00:00+02:00"})

        self.assertEqual([p.name for p in sort_scan_paths(paths, layout)], [offset, utc])

    def test_same_start_time_breaks_tie_by_name(self):
        first = "10000000-0000-4000-8000-0000000000aa"
        second = "10000000-0000-4000-8000-0000000000bb"
        layout, paths = self._global([second, first])
        for scan in (first, second):
            self._write_metadata(layout, scan, {"scanStartTime": "2025-01-01T00:00:00Z"})

        self.assertEqual([p.name for p in sort_scan_paths(paths, layout)], [first, second])

    def test_uuid_scan_without_valid_time_fails(self):
        scan = "30000000-0000-4000-8000-000000000003"
        cases = {
            "missing file": None,
            "missing field": {},
            "not a time": {"scanStartTime": "not-a-time"},
            "no timezone": {"scanStartTime": "2025-01-01T12:00:00"},
            "invalid json": "{",
        }
        for case, content in cases.items():
            with self.subTest(case), tempfile.TemporaryDirectory() as tmp:
                self.root = Path(tmp)
                layout, paths = self._global([scan])
                if content is not None:
                    self._write_metadata(layout, scan, content)
                with self.assertRaisesRegex(ValueError, scan):
                    sort_scan_paths(paths, layout)


if __name__ == "__main__":
    unittest.main()
