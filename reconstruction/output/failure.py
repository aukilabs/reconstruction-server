"""Failure reporting: what the runner reads after a stage fails."""
from reconstruction.layout import JobLayout
from reconstruction.output.manifest import save_failed_manifest_json


def write_fail_reason(job_root_path, error, logger):
    # Write error to fail_reason.txt for the Rust runner to read
    fail_reason_path = JobLayout(job_root_path).fail_reason_txt
    try:
        fail_reason_path.write_text(str(error), encoding="utf-8")
        logger.info(f"Saved fail reason to: {fail_reason_path}")
    except Exception as write_err:
        logger.warning(f"Failed to write fail_reason.txt: {write_err}")


def report_failure(layout, error, logger):
    """Write fail_reason.txt, which the runner turns into the DMS failure reason, and the failed
    job manifest (job_manifest.json; not uploaded). Every entry point calls this on failure."""
    write_fail_reason(layout.job_root, error, logger)

    manifest_out_path = layout.job_manifest_json
    logger.error(f"Saving 'failed' manifest to: {manifest_out_path}")
    save_failed_manifest_json(manifest_out_path, layout.job_root, str(error))
