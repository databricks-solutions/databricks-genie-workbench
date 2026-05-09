"""Byte-stable snapshot tests for the new Phase B markers.

A future contributor renaming a field or reordering payload keys will
break these snapshots — that's the point. Markers are CLI-truth surface
for the postmortem analyzer; their shape must change on purpose, never
silently.

See also `test_run_analysis_contract.py` for behavior tests; these
snapshots only pin the exact byte shape.
"""

from __future__ import annotations


def test_phase_b_no_records_marker_byte_stable_shape() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_no_records_marker,
    )

    line = phase_b_no_records_marker(
        optimization_run_id="opt_run_1",
        iteration=2,
        reason="all_ags_dropped_at_grounding",
        producer_exceptions={"cluster": 1, "eval_classification": 0},
        contract_version="v1",
    )

    # Marker prefix + sorted-keys JSON payload.
    assert line == (
        'GSO_PHASE_B_NO_RECORDS_V1 '
        '{"contract_version":"v1",'
        '"iteration":2,'
        '"optimization_run_id":"opt_run_1",'
        '"producer_exceptions":{"cluster":1,"eval_classification":0},'
        '"reason":"all_ags_dropped_at_grounding"}'
    )


def test_phase_b_end_marker_byte_stable_shape() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_end_marker,
    )

    line = phase_b_end_marker(
        optimization_run_id="opt_run_1",
        total_records=120,
        iter_record_counts=[24, 24, 24, 24, 24],
        iter_violation_counts=[0, 0, 0, 0, 0],
        no_records_iterations=[],
        contract_version="v1",
    )

    assert line == (
        'GSO_PHASE_B_END_V1 '
        '{"contract_version":"v1",'
        '"iter_record_counts":[24,24,24,24,24],'
        '"iter_violation_counts":[0,0,0,0,0],'
        '"no_records_iterations":[],'
        '"optimization_run_id":"opt_run_1",'
        '"total_records":120}'
    )


def test_phase_b_no_records_marker_with_empty_producer_exceptions_byte_stable() -> None:
    """Default empty case: payload still contains every contract field."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_no_records_marker,
    )

    line = phase_b_no_records_marker(
        optimization_run_id="opt_run_1",
        iteration=1,
        reason="no_clusters",
    )

    assert line == (
        'GSO_PHASE_B_NO_RECORDS_V1 '
        '{"contract_version":"v1",'
        '"iteration":1,'
        '"optimization_run_id":"opt_run_1",'
        '"producer_exceptions":{},'
        '"reason":"no_clusters"}'
    )


def test_phase_b_end_marker_includes_no_records_iters_post_t1() -> None:
    """Cycle 14-T1: GSO_PHASE_B_END_V1.no_records_iterations carries
    every iteration that produced zero records, including those whose
    in-body Phase B block never ran due to an upstream producer
    exception."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_end_marker,
    )

    line = phase_b_end_marker(
        optimization_run_id="run-1",
        total_records=2,
        iter_record_counts=[2, 0, 0, 0, 0],
        iter_violation_counts=[0, 0, 0, 0, 0],
        no_records_iterations=[2, 3, 4, 5],
        contract_version="v1",
    )
    assert "GSO_PHASE_B_END_V1" in line
    assert '"no_records_iterations":[2,3,4,5]' in line.replace(" ", "")
