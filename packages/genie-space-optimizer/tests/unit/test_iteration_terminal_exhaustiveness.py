"""Phase 0.3 — exhaustiveness contract check.

A stdout containing N ``GSO_ITERATION_SUMMARY_V1`` lines MUST also
contain N occurrences of (``GSO_FULL_EVAL_V1`` OR
``GSO_ITERATION_NO_CANDIDATE_V1`` OR ``GSO_ITERATION_FAULTED_V1``).
"""
from __future__ import annotations

from genie_space_optimizer.optimization.run_analysis_contract import (
    check_iteration_terminal_exhaustiveness,
)


def test_clean_run_returns_none():
    stdout = "\n".join([
        'GSO_ITERATION_SUMMARY_V1 {"optimization_run_id":"r1","iteration":1,'
        '"accepted_count":1,"rolled_back_count":0,"skipped_count":0,'
        '"gate_drop_count":0,"decision_record_count":5,'
        '"journey_violation_count":0}',
        'GSO_FULL_EVAL_V1 {"optimization_run_id":"r1","payload":{"iteration":1}}',
    ])
    result = check_iteration_terminal_exhaustiveness(stdout=stdout)
    assert result is None


def test_no_terminal_marker_for_iteration_returns_violation():
    """One iteration_summary, zero terminal markers → violation."""
    stdout = (
        'GSO_ITERATION_SUMMARY_V1 {"optimization_run_id":"r1","iteration":1,'
        '"accepted_count":0,"rolled_back_count":0,"skipped_count":1,'
        '"gate_drop_count":0,"decision_record_count":0,'
        '"journey_violation_count":0}'
    )
    result = check_iteration_terminal_exhaustiveness(stdout=stdout)
    assert result is not None
    assert result["iteration_summary_count"] == 1
    assert result["terminal_marker_count"] == 0


def test_mixed_terminals_count_correctly():
    stdout = "\n".join([
        'GSO_ITERATION_SUMMARY_V1 {"optimization_run_id":"r1","iteration":1,'
        '"accepted_count":1,"rolled_back_count":0,"skipped_count":0,'
        '"gate_drop_count":0,"decision_record_count":5,'
        '"journey_violation_count":0}',
        'GSO_FULL_EVAL_V1 {"optimization_run_id":"r1","payload":{"iteration":1}}',
        'GSO_ITERATION_SUMMARY_V1 {"optimization_run_id":"r1","iteration":2,'
        '"accepted_count":0,"rolled_back_count":0,"skipped_count":1,'
        '"gate_drop_count":0,"decision_record_count":0,'
        '"journey_violation_count":0}',
        'GSO_ITERATION_NO_CANDIDATE_V1 {"optimization_run_id":"r1","iteration":2,'
        '"terminal_reason":"no_structural_candidate","cluster_ids":[],"ag_id":""}',
    ])
    result = check_iteration_terminal_exhaustiveness(stdout=stdout)
    assert result is None
