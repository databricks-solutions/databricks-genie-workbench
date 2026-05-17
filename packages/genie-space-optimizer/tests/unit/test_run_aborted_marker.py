"""Phase 6.2 — GSO_RUN_ABORTED_V1 marker contract."""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    run_aborted_marker,
)


def test_marker_carries_required_fields():
    line = run_aborted_marker(
        optimization_run_id="run-abc",
        iteration=4,
        terminal_reason="invariant_violation",
        next_step="abort_run",
        reason="iteration_budget_exhausted",
    )
    assert line.startswith("GSO_RUN_ABORTED_V1 ")
    payload = json.loads(line[len("GSO_RUN_ABORTED_V1 "):])
    assert payload == {
        "optimization_run_id": "run-abc",
        "iteration": 4,
        "terminal_reason": "invariant_violation",
        "next_step": "abort_run",
        "reason": "iteration_budget_exhausted",
    }


def test_marker_iteration_coerces_to_int():
    line = run_aborted_marker(
        optimization_run_id="r",
        iteration="3",  # type: ignore[arg-type]
        terminal_reason="no_structural_candidate",
        next_step="abort_run",
        reason="terminal_router_decision",
    )
    payload = json.loads(line[len("GSO_RUN_ABORTED_V1 "):])
    assert payload["iteration"] == 3
    assert isinstance(payload["iteration"], int)
