"""CLI shim for L5 dispatch replay.

Operator workflow:

    uv run python -m genie_space_optimizer.scripts.replay_l5_dispatch \\
        --fixture packages/genie-space-optimizer/tests/replay/fixtures/forced_synthesis/label_divergence_minimal.json

The shim prints a one-line JSON summary to stdout:

    {"fixture_id": "...", "iterations": N,
     "total_attempted_dispatches": M,
     "total_appended_proposals": K,
     "total_emitted_decision_records": L}
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path


def test_cli_emits_one_line_summary(tmp_path: Path) -> None:
    fixture_path = (
        Path(__file__).resolve().parents[2]
        / "tests/replay/fixtures/forced_synthesis/label_divergence_minimal.json"
    )
    assert fixture_path.exists(), f"fixture not found: {fixture_path}"
    proc = subprocess.run(
        [
            "uv", "run", "python", "-m",
            "genie_space_optimizer.scripts.replay_l5_dispatch",
            "--fixture", str(fixture_path),
        ],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[2]),
        check=False,
    )
    assert proc.returncode == 0, (
        f"CLI failed: stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
    payload = json.loads(proc.stdout.strip())
    assert payload["fixture_id"] == "label_divergence_minimal"
    assert payload["iterations"] == 1
    assert payload["total_attempted_dispatches"] == 0
    assert payload["total_appended_proposals"] == 0
