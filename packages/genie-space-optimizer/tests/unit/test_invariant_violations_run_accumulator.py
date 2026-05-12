"""Risk 3 — verify _run_iteration_invariants_and_append_records
populates the run-level accumulator when provided."""
from __future__ import annotations

from unittest.mock import patch


def test_accumulator_collects_violations_across_call() -> None:
    from genie_space_optimizer.optimization.harness import (
        _run_iteration_invariants_and_append_records,
    )

    accumulator: list[dict] = []
    current_iter_inputs: dict = {"decision_records": []}
    fake_violations = [
        {"invariant_id": "I12", "title": "replay validity",
         "detail": "1 illegal trunk transition"},
        {"invariant_id": "I7", "title": "open hard cluster",
         "detail": "cluster H001 has no fit RCA card"},
    ]

    with patch(
        "genie_space_optimizer.optimization.invariants.run_invariants",
        return_value=fake_violations,
    ), patch(
        "genie_space_optimizer.common.config.loop_invariants_enabled",
        return_value=True,
    ), patch(
        "genie_space_optimizer.common.config.loop_invariants_strict",
        return_value=False,
    ):
        _run_iteration_invariants_and_append_records(
            run_id="test-run",
            iteration=1,
            current_iter_inputs=current_iter_inputs,
            run_violations_accumulator=accumulator,
        )

    assert len(accumulator) == 2
    assert {v["invariant_id"] for v in accumulator} == {"I12", "I7"}


def test_accumulator_none_preserves_legacy_behaviour() -> None:
    """Legacy callers (no accumulator) still see decision records appended."""
    from genie_space_optimizer.optimization.harness import (
        _run_iteration_invariants_and_append_records,
    )

    current_iter_inputs: dict = {"decision_records": []}
    fake_violations = [
        {"invariant_id": "I9", "title": "phase H canonicality",
         "detail": "field mismatch"},
    ]

    with patch(
        "genie_space_optimizer.optimization.invariants.run_invariants",
        return_value=fake_violations,
    ), patch(
        "genie_space_optimizer.common.config.loop_invariants_enabled",
        return_value=True,
    ), patch(
        "genie_space_optimizer.common.config.loop_invariants_strict",
        return_value=False,
    ):
        _run_iteration_invariants_and_append_records(
            run_id="test-run",
            iteration=1,
            current_iter_inputs=current_iter_inputs,
            run_violations_accumulator=None,
        )

    assert len(current_iter_inputs["decision_records"]) == 1


def test_accumulator_appends_dicts_not_records() -> None:
    """Accumulator stores raw violation dicts (the shape
    ``contract_health.build_contract_health_summary`` expects), not
    DecisionRecord instances."""
    from genie_space_optimizer.optimization.harness import (
        _run_iteration_invariants_and_append_records,
    )

    accumulator: list[dict] = []
    current_iter_inputs: dict = {"decision_records": []}
    fake_violations = [
        {"invariant_id": "I12", "title": "x", "detail": "y"},
    ]

    with patch(
        "genie_space_optimizer.optimization.invariants.run_invariants",
        return_value=fake_violations,
    ), patch(
        "genie_space_optimizer.common.config.loop_invariants_enabled",
        return_value=True,
    ), patch(
        "genie_space_optimizer.common.config.loop_invariants_strict",
        return_value=False,
    ):
        _run_iteration_invariants_and_append_records(
            run_id="test-run",
            iteration=2,
            current_iter_inputs=current_iter_inputs,
            run_violations_accumulator=accumulator,
        )

    assert accumulator == [
        {"invariant_id": "I12", "title": "x", "detail": "y"},
    ]
