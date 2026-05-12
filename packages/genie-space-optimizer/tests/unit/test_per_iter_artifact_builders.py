"""Unit tests for the six new per-iteration artifact builders in
``run_output_bundle.py``. Each builder is pure: dict-in, dict-out,
no I/O. The builders are consumed by ``_materialize_per_iter_contract_paths``
in the harness terminate path; tests here pin the contract shapes
the harness writer depends on.
"""
from __future__ import annotations


def test_build_iteration_summary_payload_carries_exit_path_and_counts() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_summary_payload,
    )
    payload = build_iteration_summary_payload(
        iteration=3,
        iter_summary={
            "iteration": 3,
            "accepted_count": 0,
            "rolled_back_count": 0,
            "skipped_count": 1,
            "gate_drop_count": 2,
            "decision_record_count": 5,
            "journey_violation_count": 1,
            "exit_path": "skipped_no_applied_patches",
        },
        invariant_violations=({"kind": "ungrounded_rca", "iteration": 3},),
    )
    assert payload["schema_version"] == "v1"
    assert payload["iteration"] == 3
    assert payload["exit_path"] == "skipped_no_applied_patches"
    assert payload["gate_drop_count"] == 2
    assert payload["accepted_count"] == 0
    assert payload["invariant_violations"] == [
        {"kind": "ungrounded_rca", "iteration": 3},
    ]


def test_build_iteration_decision_trace_payload_serializes_records() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_decision_trace_payload,
    )
    records = [
        {"decision_type": "eval_classified", "iteration": 2, "outcome": "info"},
        {"decision_type": "gate_decision", "iteration": 2, "outcome": "dropped"},
    ]
    payload = build_iteration_decision_trace_payload(
        iteration=2,
        decision_records=records,
    )
    assert payload["schema_version"] == "v1"
    assert payload["iteration"] == 2
    assert payload["record_count"] == 2
    assert payload["records"] == records


def test_build_iteration_decision_trace_payload_handles_empty_iter() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_decision_trace_payload,
    )
    payload = build_iteration_decision_trace_payload(
        iteration=4,
        decision_records=[],
    )
    assert payload["record_count"] == 0
    assert payload["records"] == []


def test_build_iteration_journey_validation_payload_carries_violations() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_journey_validation_payload,
    )
    report = {
        "violations": [
            {"kind": "clustered_to_soft_signal", "question_id": "gs_021"},
        ],
        "bucket_assignments": {"unanswered": ["gs_021"]},
    }
    payload = build_iteration_journey_validation_payload(
        iteration=1,
        journey_report=report,
    )
    assert payload["iteration"] == 1
    assert payload["is_valid"] is False
    assert payload["violation_count"] == 1
    assert payload["violations"] == report["violations"]
    assert payload["bucket_assignments"] == report["bucket_assignments"]


def test_build_iteration_journey_validation_payload_no_report_is_valid() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_journey_validation_payload,
    )
    payload = build_iteration_journey_validation_payload(
        iteration=5,
        journey_report=None,
    )
    assert payload["is_valid"] is True
    assert payload["violation_count"] == 0
    assert payload["violations"] == []
    assert payload["bucket_assignments"] == {}


def test_build_iteration_rca_ledger_payload_passes_through_themes_and_cards() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_rca_ledger_payload,
    )
    ledger = {
        "themes": [
            {"theme_id": "t1", "members": ["gs_001", "gs_002"]},
        ],
        "conflicts": [],
        "cards_by_cluster": {
            "c0": {"is_grounded": False, "root_cause": "wrong_aggregation"},
        },
    }
    payload = build_iteration_rca_ledger_payload(
        iteration=2,
        rca_ledger=ledger,
    )
    assert payload["iteration"] == 2
    assert payload["theme_count"] == 1
    assert payload["conflict_count"] == 0
    assert payload["cards_by_cluster"] == ledger["cards_by_cluster"]
    assert payload["themes"] == ledger["themes"]


def test_build_iteration_rca_ledger_payload_empty_iter() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_rca_ledger_payload,
    )
    payload = build_iteration_rca_ledger_payload(iteration=4, rca_ledger=None)
    assert payload["theme_count"] == 0
    assert payload["conflict_count"] == 0
    assert payload["cards_by_cluster"] == {}
    assert payload["themes"] == []


def test_build_iteration_proposal_inventory_payload_summarizes_disposition() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_proposal_inventory_payload,
    )
    inventory = {
        "proposals": [
            {"proposal_id": "p1", "disposition": "applied"},
            {"proposal_id": "p2", "disposition": "gate_dropped"},
            {"proposal_id": "p3", "disposition": "gate_dropped"},
        ],
        "by_ag": {"ag1": ["p1", "p2", "p3"]},
    }
    payload = build_iteration_proposal_inventory_payload(
        iteration=3,
        proposal_inventory=inventory,
    )
    assert payload["iteration"] == 3
    assert payload["proposal_count"] == 3
    assert payload["disposition_counts"] == {
        "applied": 1,
        "gate_dropped": 2,
    }
    assert payload["proposals"] == inventory["proposals"]
    assert payload["by_ag"] == inventory["by_ag"]


def test_build_iteration_proposal_inventory_payload_empty_iter() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_proposal_inventory_payload,
    )
    payload = build_iteration_proposal_inventory_payload(
        iteration=5, proposal_inventory=None,
    )
    assert payload["proposal_count"] == 0
    assert payload["disposition_counts"] == {}
    assert payload["proposals"] == []
    assert payload["by_ag"] == {}


def test_build_iteration_stage_index_payload_lists_captured_and_skipped_stages() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_stage_index_payload,
    )
    payload = build_iteration_stage_index_payload(
        iteration=2,
        captured_stage_keys=("evaluation_state", "rca_evidence", "cluster_formation"),
    )
    assert payload["iteration"] == 2
    assert payload["captured_count"] == 3
    assert "evaluation_state" in payload["captured"]
    assert "cluster_formation" in payload["captured"]
    # Stages declared by the contract but not captured this iteration
    # are listed under ``skipped`` so postmortem can see the exit-path
    # cliff at a glance.
    assert "applied_patches" in payload["skipped"]
    assert "acceptance_decision" in payload["skipped"]


def test_build_iteration_stage_index_payload_zero_captures() -> None:
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_iteration_stage_index_payload,
    )
    payload = build_iteration_stage_index_payload(
        iteration=4, captured_stage_keys=(),
    )
    assert payload["captured_count"] == 0
    assert payload["captured"] == []
    assert "evaluation_state" in payload["skipped"]
