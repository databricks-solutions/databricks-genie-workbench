"""Pin that every _StageCtx in the lever loop carries
_phase_h_anchor_run_id, not None.

Run 11110002's MLflow audit showed only stages 04 and 05 captured.
Stages 01 (clustering), 03 (full eval), 06 (safety_gates), 07 (applied
patches) were skipped because their _StageCtx hard-coded
mlflow_anchor_run_id=None. This test scans the source for any
_StageCtx initialisation that still hard-codes None inside the
lever loop body.
"""

from __future__ import annotations

import re
from pathlib import Path

HARNESS = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "genie_space_optimizer"
    / "optimization"
    / "harness.py"
)


def test_no_hardcoded_none_anchor_in_stage_ctx_init() -> None:
    src = HARNESS.read_text()
    # Find every line of the form `mlflow_anchor_run_id=None` inside a
    # _StageCtx(...) call. The regex matches across-newline arg lists.
    matches = re.findall(
        r"_StageCtx[^()]*\([^)]*?mlflow_anchor_run_id\s*=\s*None",
        src,
        re.DOTALL,
    )
    assert matches == [], (
        "Found _StageCtx initialisation(s) hard-coding "
        "mlflow_anchor_run_id=None — Phase H Task 8 requires "
        "_phase_h_anchor_run_id to be threaded through every "
        "lever-loop _StageCtx so wrap_with_io_capture actually logs "
        "to MLflow. Offending matches: " + repr(matches)
    )
