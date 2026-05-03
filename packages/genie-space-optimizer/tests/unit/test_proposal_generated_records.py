"""Phase B delta — Task 4.

Pins ``proposal_generated_records``: one ``PROPOSAL_GENERATED`` per
proposal that survived to ``proposals_to_patches``.

Plan: ``docs/2026-05-03-phase-b-decision-trace-completion-plan.md`` Task 4.
"""
from __future__ import annotations


def test_proposal_generated_records_one_per_proposal_with_target_qids() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generated_records,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    records = proposal_generated_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG1",
        proposals=[
            {
                "proposal_id": "P001",
                "patch_type": "update_instruction_section",
                "_grounding_target_qids": ["q1"],
                "cluster_id": "H001",
            },
            {
                "proposal_id": "P002",
                "patch_type": "add_sql_snippet_general",
                "target_qids": ["q2"],
                "cluster_id": "H001",
            },
        ],
        rca_id_by_cluster={"H001": "rca_h001"},
        cluster_root_cause_by_id={"H001": "missing_filter"},
    )

    assert len(records) == 2
    assert all(r.decision_type == DecisionType.PROPOSAL_GENERATED for r in records)
    assert all(r.outcome == DecisionOutcome.ACCEPTED for r in records)
    assert all(r.reason_code == ReasonCode.PROPOSAL_EMITTED for r in records)
    by_id = {r.proposal_id: r for r in records}
    assert by_id["P001"].target_qids == ("q1",)
    assert by_id["P001"].rca_id == "rca_h001"
    assert by_id["P001"].root_cause == "missing_filter"
    assert by_id["P001"].cluster_id == "H001"
    assert by_id["P001"].ag_id == "AG1"
    assert by_id["P001"].evidence_refs == ("ag:AG1", "cluster:H001", "rca:rca_h001")
    # P002 falls back to target_qids when _grounding_target_qids is absent.
    assert by_id["P002"].target_qids == ("q2",)


def test_proposal_generated_records_skips_proposals_with_no_target_qids() -> None:
    """Proposals without target_qids cannot satisfy the cross-checker's
    target_qids requirement; skip them rather than emit invalid records."""
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generated_records,
    )

    records = proposal_generated_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG1",
        proposals=[{"proposal_id": "P_BROKEN", "patch_type": "update_instruction_section"}],
        rca_id_by_cluster={},
        cluster_root_cause_by_id={},
    )

    assert records == []


def test_proposal_generated_records_passes_cross_checker() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generated_records,
    )
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    records = proposal_generated_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG1",
        proposals=[
            {
                "proposal_id": "P001",
                "patch_type": "update_instruction_section",
                "_grounding_target_qids": ["q1"],
                "cluster_id": "H001",
            }
        ],
        rca_id_by_cluster={"H001": "rca_h001"},
        cluster_root_cause_by_id={"H001": "missing_filter"},
    )
    events = [
        QuestionJourneyEvent(question_id="q1", stage="evaluated"),
        QuestionJourneyEvent(
            question_id="q1", stage="proposed", proposal_id="P001",
        ),
    ]

    violations = validate_decisions_against_journey(records=records, events=events)
    assert violations == []


def test_harness_call_shape_matches_producer_signature() -> None:
    """Static check: the harness's existing site uses
    ``_grounding_target_qids`` first then ``target_qids`` — our
    producer must respect the same precedence."""
    from genie_space_optimizer.optimization.decision_emitters import (
        proposal_generated_records,
    )

    records = proposal_generated_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG1",
        proposals=[
            {
                "proposal_id": "P1",
                "patch_type": "update_instruction_section",
                "_grounding_target_qids": ["q_grounded"],
                "target_qids": ["q_fallback"],
                "cluster_id": "H001",
            }
        ],
        rca_id_by_cluster={"H001": "rca_h001"},
        cluster_root_cause_by_id={"H001": "missing_filter"},
    )

    # _grounding_target_qids takes precedence over target_qids.
    assert records[0].target_qids == ("q_grounded",)
