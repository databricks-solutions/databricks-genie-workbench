"""Cycle 14-T1 integration replay — Phase B aggregator covers every iteration.

Anchor: 7Now run 534010336956422 attempt 5 F9 — five iterations
executed, GSO_PHASE_B_END_V1.iter_record_counts under-reports the
iteration count when an early-iteration producer raises.

This test simulates the failure shape directly: it drives the
helper across five iterations where iterations 2-5 had producer
exceptions BEFORE the legacy in-body Phase B block could run. After
T1, the helper's finalise-call invocation runs from the try/finally
exit-path-total finaliser regardless of where the exception fired,
so all five iterations land in the iter_record_counts list.
"""

from __future__ import annotations


def test_phase_b_end_marker_covers_every_iteration_post_t1(
    monkeypatch, capsys,
) -> None:
    monkeypatch.setenv("GSO_PHASE_B_AGGREGATOR_IN_FINALIZE", "1")
    from genie_space_optimizer.optimization.phase_b_accounting import (
        record_phase_b_iter_accounting,
    )
    from genie_space_optimizer.optimization.run_analysis_contract import (
        phase_b_end_marker,
    )

    accumulators: dict = {
        "iter_record_counts": [],
        "iter_violation_counts": [],
        "no_records_iterations": [],
        "artifact_paths": [],
        "total_violations": 0,
        "_seen_iter_ids": set(),
    }

    # Iteration 1: real records (the happy path).
    record_phase_b_iter_accounting(
        run_id="run-anchor",
        iteration=1,
        current_iter_inputs={
            "decision_records": [{
                "decision_type": "acceptance_decided",
                "iteration": 1,
                "outcome": "accepted",
                "reason_code": "accepted",
                "ag_id": "AG1",
                "rca_id": "rca",
                "target_qids": ["gs_001"],
            }],
        },
        journey_events=(),
        producer_exceptions={},
        accumulators=accumulators,
        contract_version="v1",
    )

    # Iterations 2-5: early-iteration producer exception. The legacy
    # in-body block would have been skipped; the helper-from-finalise
    # path catches them. producer_exceptions values are int counts
    # per ``classify_no_records_reason``'s contract.
    for it in (2, 3, 4, 5):
        record_phase_b_iter_accounting(
            run_id="run-anchor",
            iteration=it,
            current_iter_inputs={"decision_records": []},
            journey_events=(),
            producer_exceptions={"strategist": 1},
            accumulators=accumulators,
            contract_version="v1",
        )

    # All five iterations are accounted for.
    assert accumulators["iter_record_counts"] == [1, 0, 0, 0, 0]
    # Iteration 1's violation count is whatever the validator returned
    # (≥0); iterations 2-5 are zero-record so violations=0 by
    # construction.
    assert len(accumulators["iter_violation_counts"]) == 5
    assert accumulators["iter_violation_counts"][1:] == [0, 0, 0, 0]
    assert accumulators["no_records_iterations"] == [2, 3, 4, 5]

    captured = capsys.readouterr()
    # Four GSO_PHASE_B_NO_RECORDS_V1 markers (one per zero-record iter).
    assert captured.out.count("GSO_PHASE_B_NO_RECORDS_V1") == 4

    # GSO_PHASE_B_END_V1 emission consumes the accumulators directly.
    end_marker = phase_b_end_marker(
        optimization_run_id="run-anchor",
        total_records=sum(accumulators["iter_record_counts"]),
        iter_record_counts=accumulators["iter_record_counts"],
        iter_violation_counts=accumulators["iter_violation_counts"],
        no_records_iterations=accumulators["no_records_iterations"],
        contract_version="v1",
    )
    # Five entries in iter_record_counts — one per iteration.
    assert "[1,0,0,0,0]" in end_marker.replace(" ", "")
    assert "[2,3,4,5]" in end_marker.replace(" ", "")


def test_helper_idempotent_when_both_call_sites_fire() -> None:
    """Migration-ramp safety: the legacy in-body block runs first
    on the happy path, then _finalize_iteration_summary fires the
    helper. The helper detects the (run_id, iteration) tuple in
    _seen_iter_ids and short-circuits."""
    from genie_space_optimizer.optimization.phase_b_accounting import (
        record_phase_b_iter_accounting,
    )

    accumulators: dict = {
        "iter_record_counts": [],
        "iter_violation_counts": [],
        "no_records_iterations": [],
        "artifact_paths": [],
        "total_violations": 0,
        "_seen_iter_ids": set(),
    }
    inputs = {
        "decision_records": [{
            "decision_type": "acceptance_decided",
            "iteration": 1,
            "outcome": "accepted",
            "reason_code": "accepted",
            "ag_id": "AG1",
            "rca_id": "rca",
            "target_qids": ["gs_001"],
        }],
    }

    # Simulated "in-body block" call.
    record_phase_b_iter_accounting(
        run_id="run-anchor",
        iteration=1,
        current_iter_inputs=inputs,
        journey_events=(),
        producer_exceptions={},
        accumulators=accumulators,
        contract_version="v1",
    )
    # Simulated "finalise-call" call.
    record_phase_b_iter_accounting(
        run_id="run-anchor",
        iteration=1,
        current_iter_inputs=inputs,
        journey_events=(),
        producer_exceptions={},
        accumulators=accumulators,
        contract_version="v1",
    )

    assert accumulators["iter_record_counts"] == [1]
    assert len(accumulators["iter_violation_counts"]) == 1
    assert accumulators["artifact_paths"] == [
        "phase_b/decision_trace/iter_1.json",
    ]
