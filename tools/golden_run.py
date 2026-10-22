"""Golden run: run local, global and update the way the Rust runners do, then summarize the outputs.

Each stage gets a fresh workspace, staged the way the runner leaves it after downloading inputs,
and Python is started with the runner's exact command line from the repository root (the node's
cwd, /app in the image). The summary holds only what should survive a refactor: the output file
set, reconstruction stats, PLY sizes, manifest keys, upload-table paths and exit codes.
Compare two summaries with tools/golden_compare.py. See tools/README.md for the source layout.

Run inside the reconstruction image, from the repository root:
    python3 tools/golden_run.py _local/golden/<job> --out _local/golden/<name>.json
    python3 tools/golden_run.py --summarize-only <dir> --out <name>.json
"""
import argparse
import datetime
import importlib.util
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import time
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
STAGES = ("local", "global", "update")
SFM_FILES = ("cameras.bin", "images.bin", "points3D.bin")
GLOBAL_SFM_FILES = ("cameras.bin", "frames.bin", "images.bin", "points3D.bin", "rigs.bin")


def _load_upload_contract():
    path = REPO_ROOT / "tests" / "test_upload_contract.py"
    spec = importlib.util.spec_from_file_location("upload_contract", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


UPLOAD_CONTRACT = _load_upload_contract()


# ---------------------------------------------------------------------------
# Staging: lay out each workspace the way the runner does before starting Python
# ---------------------------------------------------------------------------

def _write_job_metadata(workspace, stage, index, domain_id, data_ids):
    # Same fields as JobMetadataRecord in runner-reconstruction-*/src/lib.rs.
    job_id = f"00000000-0000-0000-0000-{index:012d}"
    metadata = {
        "id": job_id,
        "name": f"job_{job_id}",
        "domain_id": domain_id,
        "processing_type": f"{stage}_refinement",
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "domain_server_url": "https://domain.invalid",
        "reconstruction_server_url": None,
        "data_ids": data_ids,
    }
    (workspace / "job_metadata.json").write_text(json.dumps(metadata, indent=2))
    return metadata["name"]


def _refined_scan_files(sfm_dir):
    """Files the local runner zips into RefinedScan.zip, as (relative name, path)."""
    files = []
    for path in sorted(sfm_dir.rglob("*")):
        if path.is_file() and path.suffix in UPLOAD_CONTRACT.REFINED_SCAN_ZIP_EXTENSIONS:
            files.append((path.relative_to(sfm_dir).as_posix(), path))
    return files


def _stage_refined_scan(workspace, scan, sfm_source):
    """Mirror input::materialize_refined_scans: keep the zip under datasets, extract into refined/local."""
    files = _refined_scan_files(sfm_source)
    dataset_dir = workspace / "datasets" / scan
    dataset_dir.mkdir(parents=True)
    with zipfile.ZipFile(dataset_dir / "RefinedScan.zip", "w", zipfile.ZIP_STORED) as zf:
        for name, path in files:
            # Fixed timestamp, like the Rust zip writer; source files may predate 1980.
            zf.writestr(zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0)), path.read_bytes())
    sfm_dir = workspace / "refined" / "local" / scan / "sfm"
    sfm_dir.mkdir(parents=True)
    for name, path in files:
        (sfm_dir / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, sfm_dir / name)
    missing = [f for f in UPLOAD_CONTRACT.REFINED_SCAN_REQUIRED_FILES if not (sfm_dir / f).exists()]
    if missing:
        raise RuntimeError(f"refined scan {scan} is missing {missing} in {sfm_source}")


def _refined_scan_source(scan, source, work_dir):
    """Prefer this run's local output, so the stages chain the way they do in production."""
    produced = work_dir / "local" / "refined" / "local" / scan / "sfm"
    if produced.is_dir():
        return produced
    provided = source / "refined" / "local" / scan / "sfm"
    if provided.is_dir():
        return provided
    raise RuntimeError(f"no refined scan for {scan}: neither {produced} nor {provided} exists")


def stage_workspace(stage, scans, source, work_dir, domain_id):
    workspace = work_dir / stage
    workspace.mkdir(parents=True)
    index = STAGES.index(stage) + 1
    if stage == "local":
        for scan in scans:
            shutil.copytree(source / "datasets" / scan, workspace / "datasets" / scan)
        job_name = _write_job_metadata(workspace, stage, index, domain_id, [f"dmt_scan_{s}" for s in scans])
        return workspace, job_name

    for scan in scans:
        _stage_refined_scan(workspace, scan, _refined_scan_source(scan, source, work_dir))
    if stage == "update":
        # Mirror materialize_global_colmap and materialize_refine_manifest.
        reference = source / "refined" / "global"
        target = workspace / "refined" / "global"
        (target / "refined_sfm_combined").mkdir(parents=True)
        for name in GLOBAL_SFM_FILES:
            shutil.copy2(reference / "refined_sfm_combined" / name, target / "refined_sfm_combined" / name)
        shutil.copy2(reference / "refined_manifest.json", target / "refined_manifest.json")
    job_name = _write_job_metadata(workspace, stage, index, domain_id, [f"refined_scan_{s}" for s in scans])
    return workspace, job_name


def stage_command(stage, workspace, job_name, domain_id, scans, python, workers):
    """The runner's argv: `python -m reconstruction <stage>` (python.rs), then build_python_args
    in runner-reconstruction-<stage>/src/lib.rs."""
    entry = [python, "-m", "reconstruction", stage]
    if stage in ("local", "global"):
        args = [
            "--mode", f"{stage}_refinement",
            "--job_root_path", str(workspace),
            "--output_path", str(workspace / "refined"),
            "--domain_id", domain_id,
            "--job_id", job_name,
            "--local_refinement_workers", str(workers),
        ]
        if scans:
            args += ["--scans", *scans]
        return [*entry, *args]
    return [
        *entry,
        "--data_path", str(workspace / "refined" / "local"),
        "--output_path", str(workspace / "refined" / "update"),
        "--domain_id", domain_id,
        "--job_id", job_name,
    ]


def run_stage(cmd, logs_dir, stage, timeout):
    """Start Python in its own process group, like python.rs, and wait for it."""
    stdout_path = logs_dir / f"{stage}.stdout.log"
    stderr_path = logs_dir / f"{stage}.stderr.log"
    start = time.monotonic()
    with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
        proc = subprocess.Popen(cmd, cwd=REPO_ROOT, stdout=out, stderr=err, start_new_session=True)
        try:
            exit_code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
            exit_code = "timeout"
    run_info = {"exit_code": exit_code, "duration_s": round(time.monotonic() - start, 1), "argv": cmd[1:]}
    (logs_dir / f"{stage}.run.json").write_text(json.dumps(run_info, indent=2))
    return run_info


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def size_bucket(size):
    """Order of magnitude in bytes; -1 for an empty file."""
    return -1 if size == 0 else int(math.log10(size))


def ply_vertex_count(path):
    with open(path, "rb") as f:
        for raw in f:
            line = raw.decode("ascii", errors="replace").strip()
            if line.startswith("element vertex"):
                return int(line.split()[2])
            if line == "end_header":
                break
    return None


def key_structure(value):
    """Keys only, recursively. List elements are merged into one structure."""
    if isinstance(value, dict):
        return {k: key_structure(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        merged = {}
        for item in value:
            structure = key_structure(item)
            if isinstance(structure, dict):
                merged.update(structure)
        return [dict(sorted(merged.items()))] if merged else []
    return None


def sfm_stats(sfm_dir):
    import pycolmap

    rec = pycolmap.Reconstruction(str(sfm_dir))
    return {
        "num_cameras": rec.num_cameras(),
        "num_reg_images": rec.num_reg_images(),
        "num_points3D": rec.num_points3D(),
        "mean_reproj_error": round(rec.compute_mean_reprojection_error(), 6),
    }


def log_line_stats(logs_dir, stage):
    """Rust parses each line as JSON {level, message}; anything else is logged at INFO."""
    stats = {"json_lines": 0, "json_lines_missing_level_or_message": 0, "non_json_lines": 0}
    for stream in ("stdout", "stderr"):
        path = logs_dir / f"{stage}.{stream}.log"
        if not path.exists():
            continue
        for line in path.read_text(errors="replace").splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                stats["non_json_lines"] += 1
                continue
            if not isinstance(record, dict):
                stats["non_json_lines"] += 1
            elif "level" in record and "message" in record:
                stats["json_lines"] += 1
            else:
                stats["json_lines_missing_level_or_message"] += 1
    return stats


def upload_paths(stage, workspace):
    """Which upload-table paths exist, keyed by path as the runner sees it."""
    uploads = {}
    for spec in UPLOAD_CONTRACT.UPLOAD_TABLE:
        if spec.runner != stage:
            continue
        if "{scan}" in spec.path:
            # The local runner zips each refined/local/<scan>/sfm that has every required file.
            local_dir = workspace / "refined" / "local"
            for scan_dir in sorted(p for p in local_dir.glob("*") if p.is_dir()):
                sfm = scan_dir / "sfm"
                complete = all((sfm / f).exists() for f in UPLOAD_CONTRACT.REFINED_SCAN_REQUIRED_FILES)
                uploads[spec.path.format(scan=scan_dir.name)] = complete
        else:
            uploads[spec.path] = (workspace / spec.path).exists()
    return uploads


def summarize_workspace(stage, workspace, logs_dir=None):
    refined = workspace / "refined"
    files = {}
    if refined.is_dir():
        for path in sorted(refined.rglob("*")):
            if path.is_file():
                files[path.relative_to(workspace).as_posix()] = size_bucket(path.stat().st_size)

    sfm, ply, manifests = {}, {}, {}
    for rel in files:
        path = workspace / rel
        if path.name == "images.bin" and all((path.parent / f).exists() for f in SFM_FILES):
            sfm[path.parent.relative_to(workspace).as_posix()] = sfm_stats(path.parent)
        elif path.suffix == ".ply":
            ply[rel] = ply_vertex_count(path)
    for path in sorted(list(refined.rglob("refined_manifest.json")) + list(workspace.glob("job_manifest.json"))):
        data = json.loads(path.read_text())
        manifests[path.relative_to(workspace).as_posix()] = {
            "keys": key_structure(data),
            "portal_ids": sorted(p.get("shortId") for p in data.get("portals", [])),
        }

    fail_reason = workspace / "fail_reason.txt"
    run_info = {}
    if logs_dir is not None and (logs_dir / f"{stage}.run.json").exists():
        run_info = json.loads((logs_dir / f"{stage}.run.json").read_text())
    summary = {
        "exit_code": run_info.get("exit_code"),
        "fail_reason": fail_reason.read_text().strip() if fail_reason.exists() else None,
        "root_files": sorted(p.name for p in workspace.iterdir() if p.is_file()),
        "files": files,
        "sfm": sfm,
        "ply_vertices": ply,
        "manifests": manifests,
        "uploads": upload_paths(stage, workspace),
        # Not compared: timing and log volume vary between runs.
        "info": {"duration_s": run_info.get("duration_s")},
    }
    if logs_dir is not None:
        log_stats = log_line_stats(logs_dir, stage)
        summary["json_lines_missing_level_or_message"] = log_stats.pop("json_lines_missing_level_or_message")
        summary["info"].update(log_stats)
    return summary


def summarize_dir(directory, stage=None):
    directory = Path(directory)
    logs_dir = directory / "logs"
    found = [s for s in STAGES if (directory / s / "job_metadata.json").exists()]
    if found:
        return {s: summarize_workspace(s, directory / s, logs_dir if logs_dir.is_dir() else None) for s in found}
    if stage is None:
        raise SystemExit(f"{directory} has no local/global/update workspaces; pass --stage to summarize it as one")
    return {stage: summarize_workspace(stage, directory, logs_dir if logs_dir.is_dir() else None)}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def golden_run(source, work_dir, stages, python, workers, timeout):
    spec = json.loads((source / "golden.json").read_text())
    domain_id = spec["domain_id"]
    logs_dir = work_dir / "logs"
    logs_dir.mkdir(parents=True)
    for stage in stages:
        scans = sorted(spec["stages"][stage]["scans"])
        try:
            workspace, job_name = stage_workspace(stage, scans, source, work_dir, domain_id)
        except Exception as e:
            print(f"[{stage}] staging failed: {e}", flush=True)
            (logs_dir / f"{stage}.run.json").write_text(json.dumps({"exit_code": "staging_failed", "error": str(e)}))
            continue
        cmd = stage_command(stage, workspace, job_name, domain_id, scans, python, workers)
        print(f"[{stage}] {' '.join(cmd[1:])}", flush=True)
        run_info = run_stage(cmd, logs_dir, stage, timeout)
        print(f"[{stage}] exit={run_info['exit_code']} in {run_info['duration_s']}s", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source", nargs="?", type=Path, help="golden job folder holding golden.json")
    parser.add_argument("--out", type=Path, required=True, help="summary JSON to write")
    parser.add_argument("--work", type=Path, help="where to create the stage workspaces (default: <out>_work)")
    parser.add_argument("--stages", default=",".join(STAGES), help="comma-separated subset of local,global,update")
    parser.add_argument("--python", default="python3")
    parser.add_argument("--workers", type=int, default=2, help="--local_refinement_workers (LOCAL_RUNNER_CPU_WORKERS default)")
    parser.add_argument("--timeout", type=int, default=3 * 3600, help="per-stage timeout in seconds")
    parser.add_argument("--summarize-only", type=Path, metavar="DIR", help="summarize an existing run or job root, run nothing")
    parser.add_argument("--stage", choices=STAGES, help="with --summarize-only on a single job root: which runner produced it")
    args = parser.parse_args()

    if args.summarize_only:
        summary = summarize_dir(args.summarize_only, args.stage)
    else:
        if args.source is None:
            parser.error("source is required unless --summarize-only is given")
        work_dir = args.work or args.out.with_name(args.out.stem + "_work")
        if work_dir.exists() and any(work_dir.iterdir()):
            parser.error(f"{work_dir} already exists and is not empty")
        stages = [s.strip() for s in args.stages.split(",") if s.strip()]
        for s in stages:
            if s not in STAGES:
                parser.error(f"unknown stage {s}")
        golden_run(args.source.resolve(), work_dir.resolve(), stages, args.python, args.workers, args.timeout)
        summary = summarize_dir(work_dir)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(f"summary written to {args.out}")
    failed = [s for s, v in summary.items() if v["exit_code"] not in (0, None)]
    if failed:
        print(f"stages with a non-zero exit: {', '.join(failed)}", file=sys.stderr)


if __name__ == "__main__":
    main()
