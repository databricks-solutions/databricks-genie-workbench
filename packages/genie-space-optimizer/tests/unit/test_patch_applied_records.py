"""Phase B delta — Task 6.

Pins ``patch_applied_records``: one ``PATCH_APPLIED`` per applied
patch, mirroring the harness's ``_journey_emit("applied_targeted",
...)`` site.

Plan: ``docs/2026-05-03-phase-b-decision-trace-completion-plan.md`` Task 6.
"""
from __future__ import annotations


def test_patch_applied_records_one_per_applied_entry() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        patch_applied_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    records = patch_applied_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG1",
        applied_entries=[
            {
                "patch": {
                    "proposal_id": "P001",
                    "patch_type": "update_instruction_section",
                    "_grounding_target_qids": ["q1"],
                    "cluster_id": "H001",
                }
            },
            {
                "patch": {
                    "proposal_id": "P002",
                    "patch_type": "add_sql_snippet_general",
                    "target_qids": ["q2"],
                    "cluster_id": "H001",
                }
            },
        ],
        rca_id_by_cluster={"H001": "rca_h001"},
        cluster_root_cause_by_id={"H001": "missing_filter"},
    )

    assert len(records) == 2
    assert all(r.decision_type == DecisionType.PATCH_APPLIED for r in records)
    assert all(r.outcome == DecisionOutcome.APPLIED for r in records)
    assert all(r.reason_code == ReasonCode.PATCH_APPLIED for r in records)
    by_id = {r.proposal_id: r for r in records}
    assert by_id["P001"].target_qids == ("q1",)
    assert by_id["P001"].rca_id == "rca_h001"
    assert by_id["P001"].root_cause == "missing_filter"
    assert by_id["P001"].ag_id == "AG1"
    assert by_id["P001"].evidence_refs == ("ag:AG1", "cluster:H001", "rca:rca_h001")
    assert by_id["P002"].target_qids == ("q2",)


def test_patch_applied_records_skips_entries_with_no_target_qids() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        patch_applied_records,
    )

    records = patch_applied_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG1",
        applied_entries=[{"patch": {"proposal_id": "P_BROKEN"}}],
        rca_id_by_cluster={},
        cluster_root_cause_by_id={},
    )

    assert records == []


def test_patch_applied_records_passes_cross_checker_against_applied_targeted_stage() -> None:
    """Task 8 relaxes the validator to accept ``applied_targeted`` for
    ``PATCH_APPLIED``; this test pins that contract end-to-end."""
    from genie_space_optimizer.optimization.decision_emitters import (
        patch_applied_records,
    )
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    records = patch_applied_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG1",
        applied_entries=[
            {
                "patch": {
                    "proposal_id": "P001",
                    "patch_type": "update_instruction_section",
                    "_grounding_target_qids": ["q1"],
                    "cluster_id": "H001",
                }
            }
        ],
        rca_id_by_cluster={"H001": "rca_h001"},
        cluster_root_cause_by_id={"H001": "missing_filter"},
    )
    events = [
        QuestionJourneyEvent(question_id="q1", stage="evaluated"),
        QuestionJourneyEvent(
            question_id="q1", stage="applied_targeted", proposal_id="P001",
        ),
    ]

    violations = validate_decisions_against_journey(records=records, events=events)
    assert violations == [], f"Expected no violations, got: {violations}"
