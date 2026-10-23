"""The job folder: every path the Rust runners create, read or upload.

The runners lay out a job root like this (runner-reconstruction-*/src/workspace.rs and input.rs)
and pass its paths as arguments:

    <job root>/
    ├── job_metadata.json        written by the runner before Python starts
    ├── scan_data_summary.json   optional; uploaded by global and update when present
    ├── datasets/<scan>/         local: the scan's inputs; global and update: RefinedScan.zip
    ├── refined/                 --output_path of local and global
    │   ├── local/<scan>/sfm/    local's output, zipped and uploaded as RefinedScan.zip;
    │   │                        global's and update's input
    │   ├── global/              global's output, and update's reference map
    │   └── update/              update's output
    ├── log.txt                  uploaded as the task log
    ├── fail_reason.txt          read by the runner after a non-zero exit
    └── job_manifest.json        failed-job manifest (not uploaded)

Files inside the stage output folders are named by the code that writes them; the uploaded
ones are listed in reconstruction/output/uploads.py.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class JobLayout:
    job_root: Path
    # Defaults to <job root>/refined; the local and global runners pass it as --output_path.
    refined: Optional[Path] = field(default=None)

    def __post_init__(self):
        object.__setattr__(self, "job_root", Path(self.job_root))
        refined = self.job_root / "refined" if self.refined is None else Path(self.refined)
        object.__setattr__(self, "refined", refined)

    @classmethod
    def from_refined_local(cls, refined_local: Path) -> "JobLayout":
        """The update runner passes --data_path <job root>/refined/local."""
        refined_local = Path(refined_local)
        return cls(refined_local.parent.parent, refined_local.parent)

    @property
    def datasets(self) -> Path:
        return self.job_root / "datasets"

    def dataset(self, scan: str) -> Path:
        return self.datasets / scan

    @property
    def refined_local(self) -> Path:
        return self.refined / "local"

    def refined_scan(self, scan: str) -> Path:
        return self.refined_local / scan

    def refined_sfm(self, scan: str) -> Path:
        return self.refined_scan(scan) / "sfm"

    @property
    def refined_global(self) -> Path:
        return self.refined / "global"

    @property
    def refined_update(self) -> Path:
        return self.refined / "update"

    @property
    def log_txt(self) -> Path:
        return self.job_root / "log.txt"

    @property
    def fail_reason_txt(self) -> Path:
        return self.job_root / "fail_reason.txt"

    @property
    def job_metadata_json(self) -> Path:
        return self.job_root / "job_metadata.json"

    @property
    def job_manifest_json(self) -> Path:
        return self.job_root / "job_manifest.json"

    @property
    def scan_data_summary_json(self) -> Path:
        return self.job_root / "scan_data_summary.json"

    def partial_rec(self, scan: str) -> Path:
        """Where a legacy zipped refined scan (reconstruction_refined_x1.zip) is extracted."""
        return self.job_root / "partial_rec" / scan
