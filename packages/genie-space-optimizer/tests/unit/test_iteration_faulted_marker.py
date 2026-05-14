"""Phase 0.3 — GSO_ITERATION_FAULTED_V1 producer."""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    iteration_faulted_marker,
)


def test_marker_has_v1_prefix():
    line = iteration_faulted_marker(
        optimization_run_id="opt-1",
        iteration=2,
        exception_class="RuntimeError",
        exception_message="LLM endpoint timed out",
        traceback_head="File 'harness.py', line 17000, in _select_ag",
    )
    assert line.startswith("GSO_ITERATION_FAULTED_V1 ")


def test_marker_payload_carries_required_keys():
    line = iteration_faulted_marker(
        optimization_run_id="opt-1",
        iteration=2,
        exception_class="RuntimeError",
        exception_message="timeout",
        traceback_head="File 'harness.py', line 17000",
    )
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["optimization_run_id"] == "opt-1"
    assert payload["iteration"] == 2
    assert payload["exception_class"] == "RuntimeError"
    assert payload["exception_message"] == "timeout"
    assert "harness.py" in payload["traceback_head"]


def test_marker_truncates_traceback_to_2048_chars():
    """The traceback_head field must stay under 2048 chars so the
    stdout marker line itself stays under ~4096 chars (the
    existing convention)."""
    huge = "x" * 10000
    line = iteration_faulted_marker(
        optimization_run_id="opt-1",
        iteration=2,
        exception_class="RuntimeError",
        exception_message="",
        traceback_head=huge,
    )
    payload = json.loads(line.split(" ", 1)[1])
    assert len(payload["traceback_head"]) <= 2048
