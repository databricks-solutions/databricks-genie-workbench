"""B2 Task 3 — _project_iteration normalization for DecisionRecord input."""

from __future__ import annotations


def _mk_decision_record(
    *,
    decision_type: str = "patch_applied",
    ag_id: str = "AG1",
    reason_code: str = "none",
):
    """Build a real DecisionRecord dataclass instance."""
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
    )

    return DecisionRecord(
        run_id="r-123",
        iteration=1,
        decision_type=DecisionType(decision_type),
        outcome=DecisionOutcome.APPLIED,
        reason_code=ReasonCode(reason_code),
        ag_id=ag_id,
    )


def test_project_iteration_normalizes_dataclass_records_to_dicts() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        _project_iteration,
    )

    iter_inputs = {
        "clusters": [],
        "strategist_response": {"action_groups": []},
        "rca_cards_present": {},
        "decision_records": [
            _mk_decision_record(decision_type="patch_applied", ag_id="AG1"),
            _mk_decision_record(decision_type="patch_applied", ag_id="AG2"),
        ],
    }
    out = _project_iteration(current_iter_inputs=iter_inputs, iteration=1)
    records = out["decision_records"]

    assert all(isinstance(r, dict) for r in records), (
        f"expected all dicts, got types {[type(r).__name__ for r in records]}"
    )
    # The dicts must support .get() — the contract the invariants need.
    assert records[0].get("decision_type") == "patch_applied"
    assert records[1].get("ag_id") == "AG2"


def test_project_iteration_preserves_existing_dict_records() -> None:
    """Legacy fixtures that already use dicts must pass through unchanged."""
    from genie_space_optimizer.optimization.invariant_projection import (
        _project_iteration,
    )

    legacy = [
        {"decision_type": "patch_applied", "ag_id": "AG1"},
        {"decision_type": "cluster_blocked_no_rca", "cluster_id": "c-7"},
    ]
    iter_inputs = {
        "clusters": [],
        "strategist_response": {"action_groups": []},
        "rca_cards_present": {},
        "decision_records": legacy,
    }
    out = _project_iteration(current_iter_inputs=iter_inputs, iteration=1)
    records = out["decision_records"]
    assert all(isinstance(r, dict) for r in records)
    assert records[0]["decision_type"] == "patch_applied"
    assert records[1]["cluster_id"] == "c-7"


def test_project_iteration_drops_garbage_records_cleanly() -> None:
    """Strings, ints, None — must be dropped, not propagate."""
    from genie_space_optimizer.optimization.invariant_projection import (
        _project_iteration,
    )

    iter_inputs = {
        "clusters": [],
        "strategist_response": {"action_groups": []},
        "rca_cards_present": {},
        "decision_records": [
            _mk_decision_record(ag_id="AG1"),
            "garbage_string",
            None,
            42,
        ],
    }
    out = _project_iteration(current_iter_inputs=iter_inputs, iteration=1)
    records = out["decision_records"]
    # Only the valid DecisionRecord survives → 1 entry.
    assert len(records) == 1
    assert records[0]["ag_id"] == "AG1"


def test_project_iteration_mixed_dataclass_and_dict() -> None:
    """The realistic 2314bb2c shape: some emit sites use to_dict(), others
    don't — projection must produce a uniform dict list."""
    from genie_space_optimizer.optimization.invariant_projection import (
        _project_iteration,
    )

    record_a = _mk_decision_record(decision_type="patch_applied", ag_id="AG1")
    iter_inputs = {
        "clusters": [],
        "strategist_response": {"action_groups": []},
        "rca_cards_present": {},
        "decision_records": [
            record_a,
            {"decision_type": "control_plane_acceptance", "ag_id": "AG1"},
            record_a.to_dict(),
        ],
    }
    out = _project_iteration(current_iter_inputs=iter_inputs, iteration=1)
    records = out["decision_records"]
    assert len(records) == 3
    assert all(isinstance(r, dict) for r in records)
    assert [r["decision_type"] for r in records] == [
        "patch_applied",
        "control_plane_acceptance",
        "patch_applied",
    ]


def test_project_iteration_empty_decision_records() -> None:
    from genie_space_optimizer.optimization.invariant_projection import (
        _project_iteration,
    )

    iter_inputs = {
        "clusters": [],
        "strategist_response": {"action_groups": []},
        "rca_cards_present": {},
        "decision_records": [],
    }
    out = _project_iteration(current_iter_inputs=iter_inputs, iteration=1)
    assert out["decision_records"] == []
