"""Failure reporting: fail_reason.txt, which the runner turns into the DMS failure reason."""
from reconstruction.layout import JobLayout


def write_fail_reason(job_root_path, error, logger):
    # Write error to fail_reason.txt for the Rust runner to read
    fail_reason_path = JobLayout(job_root_path).fail_reason_txt
    try:
        fail_reason_path.write_text(str(error), encoding="utf-8")
        logger.info(f"Saved fail reason to: {fail_reason_path}")
    except Exception as write_err:
        logger.warning(f"Failed to write fail_reason.txt: {write_err}")
