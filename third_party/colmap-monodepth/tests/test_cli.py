"""CLI help and pipeline import smoke tests (no GPU)."""

from __future__ import annotations

import subprocess
import sys

import pytest


def _run_cli(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "colmap_monodepth.cli", *argv],
        capture_output=True,
        text=True,
        check=False,
    )


def test_cli_top_level_help_exits_zero():
    proc = _run_cli(["--help"])
    assert proc.returncode == 0
    assert "infer" in proc.stdout
    assert "mesh" in proc.stdout


@pytest.mark.parametrize(
    "subcmd",
    ["infer", "fit", "carve", "tsdf", "mesh"],
)
def test_cli_subcommand_help_exits_zero(subcmd: str):
    proc = _run_cli([subcmd, "--help"])
    assert proc.returncode == 0
    assert "--colmap-dir" in proc.stdout


def test_pipeline_import_smoke():
    from colmap_monodepth.pipeline import (
        run_carve,
        run_fit,
        run_infer,
        run_mesh,
        run_tsdf,
    )

    for fn in (run_infer, run_fit, run_carve, run_tsdf, run_mesh):
        assert callable(fn)
