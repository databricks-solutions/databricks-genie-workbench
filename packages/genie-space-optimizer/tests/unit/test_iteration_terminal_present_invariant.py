"""Phase 1.2 — iteration_terminal_present invariant.

Phase 0.3 already checks that every iteration emits SOME terminal
marker (full_eval / iteration_no_candidate / iteration_faulted).
This task adds the stricter check that each iteration_no_candidate
payload's terminal_reason field is in the TerminalReason
closed vocabulary.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.run_analysis_contract import (
    check_iteration_terminal_present,
)


def test_clean_run_with_valid_terminal_reason():
    stdout = "\n".join([
        'GSO_ITERATION_SUMMARY_V1 {"optimization_run_id":"r","iteration":1,'
        '"accepted_count":0,"rolled_back_count":0,"skipped_count":1,'
        '"gate_drop_count":0,"decision_record_count":0,'
        '"journey_violation_count":0}',
        'GSO_ITERATION_NO_CANDIDATE_V1 {"optimization_run_id":"r","iteration":1,'
        '"terminal_reason":"proposal_generation_empty","cluster_ids":[],"ag_id":""}',
    ])
    result = check_iteration_terminal_present(stdout=stdout)
    assert result is None


def test_unknown_terminal_reason_returns_violation():
    """A terminal_reason outside the TerminalReason enum is a
    violation."""
    stdout = (
        'GSO_ITERATION_NO_CANDIDATE_V1 {"optimization_run_id":"r","iteration":1,'
        '"terminal_reason":"made_up_value","cluster_ids":[],"ag_id":""}'
    )
    result = check_iteration_terminal_present(stdout=stdout)
    assert result is not None
    assert "made_up_value" in str(result.get("unknown_terminal_reasons", []))


def test_multiple_iterations_all_valid():
    lines = []
    for i, reason in enumerate(
        ("proposal_generation_empty", "no_applied_patches", "structural_gate_dropped_instruction_only"),
        start=1,
    ):
        lines.append(
            f'GSO_ITERATION_SUMMARY_V1 {{"optimization_run_id":"r","iteration":{i},'
            f'"accepted_count":0,"rolled_back_count":0,"skipped_count":1,'
            f'"gate_drop_count":0,"decision_record_count":0,'
            f'"journey_violation_count":0}}'
        )
        lines.append(
            f'GSO_ITERATION_NO_CANDIDATE_V1 {{"optimization_run_id":"r","iteration":{i},'
            f'"terminal_reason":"{reason}","cluster_ids":[],"ag_id":""}}'
        )
    stdout = "\n".join(lines)
    result = check_iteration_terminal_present(stdout=stdout)
    assert result is None


def test_full_eval_marker_skipped_from_reason_check():
    """FULL_EVAL_V1 markers don't carry a terminal_reason field —
    the check only validates ITERATION_NO_CANDIDATE_V1 payloads."""
    stdout = (
        'GSO_FULL_EVAL_V1 {"optimization_run_id":"r","payload":{}}'
    )
    result = check_iteration_terminal_present(stdout=stdout)
    assert result is None
