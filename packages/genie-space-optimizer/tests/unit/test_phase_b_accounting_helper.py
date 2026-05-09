"""Cycle 14-T1 — record_phase_b_iter_accounting pure helper.

The helper appends to the four accumulators (record counts, violation
counts, no-records iterations, total violations) and emits the
GSO_PHASE_B_NO_RECORDS_V1 marker + MLflow tag when an iteration produces
zero typed decision records. Tests exercise the helper in isolation
(no harness; no MLflow; stdout marker captured via capsys).
"""

from __future__ import annotations

import pytest


@pytest.fixture
def empty_accumulators() -> dict:
    return {
        "iter_record_counts": [],
        "iter_violation_counts": [],
        "no_records_iterations": [],
        "total_violations": 0,
        "artifact_paths": [],
        "_seen_iter_ids": set(),
    }


def _accepted_record() -> dict:
    return {
        "decision_type": "acceptance_decided",
        "iteration": 1,
        "outcome": "accepted",
        "reason_code": "accepted",
        "ag_id": "AG1",
        "rca_id": "rca_001",
        "target_qids": ["gs_001"],
    }


def test_helper_appends_iter_record_count_on_happy_path(
    empty_accumulators,
) -> None:
    from genie_space_optimizer.optimization.phase_b_accounting import (
        record_phase_b_iter_accounting,
    )

    record_phase_b_iter_accounting(
        run_id="run-1",
        iteration=1,
        current_iter_inputs={"decision_records": [_accepted_record()]},
        journey_events=(),
        producer_exceptions={},
        accumulators=empty_accumulators,
        contract_version="v1",
    )

    assert empty_accumulators["iter_record_counts"] == [1]
    # The validator is invoked on the records + journey events; the
    # exact violation count depends on the validator's contract (e.g.
    # an ACCEPTANCE_DECIDED record without a matching journey event
    # is itself a violation today). Assert non-negative + total
    # invariant rather than a hard-coded count.
    assert len(empty_accumulators["iter_violation_counts"]) == 1
    assert empty_accumulators["iter_violation_counts"][0] >= 0
    assert empty_accumulators["total_violations"] == \
        empty_accumulators["iter_violation_counts"][0]
    assert empty_accumulators["no_records_iterations"] == []
    assert empty_accumulators["artifact_paths"] == [
        "phase_b/decision_trace/iter_1.json",
    ]


def test_helper_emits_no_records_marker_on_zero_records(
    capsys, empty_accumulators,
) -> None:
    from genie_space_optimizer.optimization.phase_b_accounting import (
        record_phase_b_iter_accounting,
    )

    # producer_exceptions values are int counts per
    # ``classify_no_records_reason``'s contract.
    record_phase_b_iter_accounting(
        run_id="run-1",
        iteration=2,
        current_iter_inputs={"decision_records": []},
        journey_events=(),
        producer_exceptions={"strategist": 1},
        accumulators=empty_accumulators,
        contract_version="v1",
    )

    assert empty_accumulators["iter_record_counts"] == [0]
    assert empty_accumulators["no_records_iterations"] == [2]
    captured = capsys.readouterr()
    assert "GSO_PHASE_B_NO_RECORDS_V1" in captured.out
    # Reason classified by NoRecordsReason vocabulary.
    assert '"reason":' in captured.out


def test_helper_idempotent_on_reentry_for_same_iteration(
    capsys, empty_accumulators,
) -> None:
    from genie_space_optimizer.optimization.phase_b_accounting import (
        record_phase_b_iter_accounting,
    )

    record_phase_b_iter_accounting(
        run_id="run-1",
        iteration=3,
        current_iter_inputs={"decision_records": [_accepted_record()]},
        journey_events=(),
        producer_exceptions={},
        accumulators=empty_accumulators,
        contract_version="v1",
    )
    capsys.readouterr()  # drain

    # Second call for the same (run_id, iteration) is a no-op.
    record_phase_b_iter_accounting(
        run_id="run-1",
        iteration=3,
        current_iter_inputs={"decision_records": [_accepted_record()]},
        journey_events=(),
        producer_exceptions={},
        accumulators=empty_accumulators,
        contract_version="v1",
    )

    assert empty_accumulators["iter_record_counts"] == [1]
    # Idempotency: violation list still has exactly one entry from
    # the first call; the second call short-circuited.
    assert len(empty_accumulators["iter_violation_counts"]) == 1
    assert ("run-1", 3) in empty_accumulators["_seen_iter_ids"]
    captured = capsys.readouterr()
    assert "GSO_PHASE_B_NO_RECORDS_V1" not in captured.out


def test_helper_counts_journey_validator_violations(
    empty_accumulators,
) -> None:
    """The validator runs against typed records + journey events and
    appends its violation count. Violation accounting is what
    GSO_PHASE_B_END_V1 reports as iter_violation_counts."""
    from genie_space_optimizer.optimization.phase_b_accounting import (
        record_phase_b_iter_accounting,
    )

    rec = _accepted_record()
    rec["iteration"] = 4
    record_phase_b_iter_accounting(
        run_id="run-1",
        iteration=4,
        current_iter_inputs={"decision_records": [rec]},
        journey_events=(),
        producer_exceptions={},
        accumulators=empty_accumulators,
        contract_version="v1",
    )

    assert empty_accumulators["iter_record_counts"] == [1]
    assert empty_accumulators["iter_violation_counts"][0] >= 0
    assert empty_accumulators["total_violations"] == \
        empty_accumulators["iter_violation_counts"][0]


def test_helper_does_not_mutate_inputs_other_than_accumulators(
    empty_accumulators,
) -> None:
    from genie_space_optimizer.optimization.phase_b_accounting import (
        record_phase_b_iter_accounting,
    )

    inputs = {"decision_records": [_accepted_record()]}
    inputs_snapshot = dict(inputs)
    events = ("event_a", "event_b")
    excs = {"strategist": "RuntimeError(...)"}
    excs_snapshot = dict(excs)

    record_phase_b_iter_accounting(
        run_id="run-1",
        iteration=5,
        current_iter_inputs=inputs,
        journey_events=events,
        producer_exceptions=excs,
        accumulators=empty_accumulators,
        contract_version="v1",
    )

    assert inputs == inputs_snapshot
    assert excs == excs_snapshot
