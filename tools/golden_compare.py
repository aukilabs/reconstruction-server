"""Compare two golden-run summaries written by tools/golden_run.py.

Exact: stages, exit codes, fail reasons, the output file set, job-root files, manifest keys and
portal ids, upload-table paths, and JSON log lines missing level/message.
Within tolerance: sfm stats and PLY vertex counts. tools/golden_tolerances.json keys are
"<stage>:<metric>" or "<metric>"; a metric with no entry must match exactly.
File size buckets (orders of magnitude) may differ by one.

    python3 tools/golden_compare.py _local/golden/baseline.json _local/golden/<name>.json

Exits 1 when anything differs beyond tolerance. The "max observed difference" table is what
tools/README.md uses to set the tolerances.
"""
import argparse
import json
import sys
from pathlib import Path

DEFAULT_TOLERANCES = Path(__file__).resolve().parent / "golden_tolerances.json"
EXACT_FIELDS = ("exit_code", "fail_reason", "root_files", "uploads", "json_lines_missing_level_or_message")


def within(a, b, tol):
    if a is None or b is None:
        return a == b
    return abs(a - b) <= max(tol.get("abs", 0), tol.get("rel", 0) * max(abs(a), abs(b)))


def rel_diff(a, b):
    if a is None or b is None or a == b:
        return 0.0
    return abs(a - b) / max(abs(a), abs(b))


def compare(a, b, tolerances):
    problems, max_seen = [], {}

    def check(stage, metric, x, y):
        key = f"{stage}:{metric}"
        max_seen[key] = max(max_seen.get(key, 0.0), rel_diff(x, y))
        return within(x, y, tolerances.get(key, tolerances.get(metric, {})))

    if sorted(a) != sorted(b):
        problems.append(f"stages differ: {sorted(a)} vs {sorted(b)}")
    for stage in sorted(set(a) & set(b)):
        sa, sb = a[stage], b[stage]
        for field in EXACT_FIELDS:
            fa, fb = sa.get(field), sb.get(field)
            if fa == fb:
                continue
            if isinstance(fa, dict) and isinstance(fb, dict):
                for key in sorted(set(fa) | set(fb)):
                    if fa.get(key) != fb.get(key):
                        problems.append(f"[{stage}] {field} {key}: {fa.get(key)!r} vs {fb.get(key)!r}")
            elif isinstance(fa, list) and isinstance(fb, list):
                problems.append(f"[{stage}] {field}: only in A {sorted(set(fa) - set(fb))}, only in B {sorted(set(fb) - set(fa))}")
            else:
                problems.append(f"[{stage}] {field}: {fa!r} vs {fb!r}")

        files_a, files_b = sa["files"], sb["files"]
        for path in sorted(set(files_a) - set(files_b)):
            problems.append(f"[{stage}] only in A: {path}")
        for path in sorted(set(files_b) - set(files_a)):
            problems.append(f"[{stage}] only in B: {path}")
        for path in sorted(set(files_a) & set(files_b)):
            if abs(files_a[path] - files_b[path]) > 1:
                problems.append(f"[{stage}] size bucket {path}: {files_a[path]} vs {files_b[path]}")

        if sa["manifests"] != sb["manifests"]:
            for path in sorted(set(sa["manifests"]) | set(sb["manifests"])):
                ma, mb = sa["manifests"].get(path), sb["manifests"].get(path)
                if ma is None or mb is None:
                    problems.append(f"[{stage}] manifest {path} only in {'B' if ma is None else 'A'}")
                    continue
                if ma["keys"] != mb["keys"]:
                    problems.append(f"[{stage}] manifest keys differ in {path}")
                if ma["portal_ids"] != mb["portal_ids"]:
                    problems.append(f"[{stage}] portal ids in {path}: {ma['portal_ids']} vs {mb['portal_ids']}")

        if sorted(sa["sfm"]) != sorted(sb["sfm"]):
            problems.append(f"[{stage}] sfm folders differ: {sorted(sa['sfm'])} vs {sorted(sb['sfm'])}")
        for sfm in sorted(set(sa["sfm"]) & set(sb["sfm"])):
            for key, x in sa["sfm"][sfm].items():
                y = sb["sfm"][sfm].get(key)
                if not check(stage, f"sfm.{key}", x, y):
                    problems.append(f"[{stage}] {sfm} {key}: {x} vs {y}")

        for ply in sorted(set(sa["ply_vertices"]) & set(sb["ply_vertices"])):
            x, y = sa["ply_vertices"][ply], sb["ply_vertices"][ply]
            if not check(stage, "ply.vertices", x, y):
                problems.append(f"[{stage}] {ply} vertices: {x} vs {y}")
    return problems, max_seen


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("a", type=Path)
    parser.add_argument("b", type=Path)
    parser.add_argument("--tolerances", type=Path, default=DEFAULT_TOLERANCES)
    args = parser.parse_args()

    tolerances = json.loads(args.tolerances.read_text())
    a, b = json.loads(args.a.read_text()), json.loads(args.b.read_text())
    problems, max_seen = compare(a, b, tolerances)

    print(f"A: {args.a}\nB: {args.b}\n")
    for stage in sorted(set(a) | set(b)):
        da = a.get(stage, {}).get("info", {}).get("duration_s")
        db = b.get(stage, {}).get("info", {}).get("duration_s")
        files = len(a.get(stage, {}).get("files", {}))
        print(f"  {stage:<7} exit {a.get(stage, {}).get('exit_code')!s:>4} / {b.get(stage, {}).get('exit_code')!s:<4}"
              f" files {files:>5}   duration {da}s / {db}s")
    print("\nmax observed relative difference (tolerance; {} = exact):")
    for key in sorted(max_seen):
        metric = key.split(":", 1)[1]
        print(f"  {key:<32} {max_seen[key]:.4%}   ({tolerances.get(key, tolerances.get(metric, {}))})")
    if problems:
        print(f"\nFAIL: {len(problems)} difference(s)")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    print("\nOK: file sets identical, stats within tolerance")


if __name__ == "__main__":
    main()
