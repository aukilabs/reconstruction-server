import json
import tempfile
import unittest
from pathlib import Path

from utils.capture_identity import capture_folder_id, discover_capture_paths, sort_capture_paths

A = "8c4869c0-99c5-4515-8545-e3674d383338"
B = "7d909fbe-20fa-4a13-8f88-aa5d8b91ceef"
LEGACY = "2025-03-19_10-57-00"


class CaptureIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "datasets").mkdir()

    def scan(self, identity, time=None, refined=False):
        dataset = self.root / "datasets" / identity
        dataset.mkdir(exist_ok=True)
        if refined:
            folder = self.root / "refined" / "local" / identity / "sfm"
            folder.mkdir(parents=True, exist_ok=True)
            manifest = folder / "capture_metadata.json"
            data = {"captureId": identity, "scanStartTime": time}
        else:
            manifest = dataset / "Manifest.json"
            data = {"id": identity, "scanStartTime": time}
        if time is not None:
            manifest.write_text(json.dumps(data))
        return dataset

    def test_discovery_accepts_both_formats_and_excludes_other_directories(self):
        for name in [A, B, LEGACY, "dmt_scan_" + LEGACY, "dmt_scan_" + A, "20_not_a_scan", "other"]:
            self.scan(name)
        found = {path.name for path in discover_capture_paths(self.root / "datasets")}
        self.assertEqual(found, {A, B, LEGACY, "dmt_scan_" + LEGACY, "dmt_scan_" + A})
        self.assertIsNone(capture_folder_id("../" + A))

    def test_same_start_time_keeps_both_scans_and_breaks_ties_by_id(self):
        scans = [self.scan(id, "2026-10-13T12:34:56.1234567Z") for id in [A, B]]
        self.assertEqual([p.name for p in sort_capture_paths(scans, self.root)], [B, A])

    def test_global_and_update_use_metadata_time_not_uuid_order(self):
        scans = [self.scan(B, "2026-10-13T12:00:00Z", True), self.scan(A, "2026-10-13T13:00:00+02:00", True)]
        self.assertEqual([p.name for p in sort_capture_paths(scans, self.root)], [A, B])
        update_paths = [self.root / "refined" / "local" / p.name for p in scans]
        self.assertEqual([p.name for p in sort_capture_paths(update_paths, self.root)], [A, B])

    def test_legacy_cache_without_metadata_is_still_usable(self):
        dated = self.scan(A, "2026-10-13T12:00:00Z", True)
        old = self.scan(LEGACY)
        older = self.scan("2024-01-01_00-00-00")
        with self.assertLogs(level="WARNING"):
            self.assertEqual(sort_capture_paths([old, dated, older], self.root), [dated, older, old])

    def test_uuid_missing_time_or_mismatched_metadata_fails(self):
        scan = self.scan(A)
        with self.assertRaisesRegex(ValueError, "start time"):
            sort_capture_paths([scan], self.root)
        (scan / "Manifest.json").write_text(json.dumps({"id": B, "scanStartTime": "2026-10-13T12:00:00Z"}))
        with self.assertRaisesRegex(ValueError, "identity"):
            sort_capture_paths([scan], self.root)

    def test_legacy_prefixed_manifest_and_offset_time(self):
        scan = self.scan(LEGACY, "2026-10-13T12:00:00Z")
        (scan / "Manifest.json").write_text(json.dumps({"id": "dmt_scan_" + LEGACY, "scanStartTime": "2026-10-13T12:00:00Z"}))
        uuid = self.scan(A, "2026-10-13T14:00:01+02:00")
        self.assertEqual(sort_capture_paths([uuid, scan], self.root), [scan, uuid])


if __name__ == "__main__":
    unittest.main()
