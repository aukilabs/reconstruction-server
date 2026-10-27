"""Files the runner writes into the job root before Python starts: job_metadata.json, and
scan_data_summary.json when the job has one."""
import json

from reconstruction.layout import JobLayout


def load_job_metadata(job_root_path):
    """job_metadata.json as a dict, or None if it does not exist."""
    path = JobLayout(job_root_path).job_metadata_json
    if not path.exists():
        return None
    return json.load(open(path))


def load_scan_data_summary(job_root_path):
    """scan_data_summary.json as a dict, or None if it does not exist."""
    path = JobLayout(job_root_path).scan_data_summary_json
    if not path.exists():
        return None
    return json.load(open(path))
