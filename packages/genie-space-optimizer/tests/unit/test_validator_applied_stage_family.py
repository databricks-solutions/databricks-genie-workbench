"""Phase B delta — Task 8.

Pins that the validator accepts ``applied``, ``applied_targeted``, and
``applied_broad_ag_scope`` as journey-stage matches for
``PATCH_APPLIED`` records.

Plan: ``docs/2026-05-03-phase-b-decision-trace-completion-plan.md`` Task 8.
"""
from __future__ import annotations


def _patch_applied_record(qid: str, proposal_id: str):
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
    )

    return DecisionRecord(
        iteration=1,
        decision_type=DecisionType.PATCH_APPLIED,
        outcome=DecisionOutcome.APPLIED,
        reason_code=ReasonCode.PATCH_APPLIED,
        question_id=qid,
        evidence_refs=("ag:AG1",),
        rca_id="rca_h001",
        root_cause="missing_filter",
        target_qids=(qid,),
        affected_qids=(qid,),
        proposal_id=proposal_id,
    )


def test_validator_accepts_applied_targeted_stage() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    violations = validate_decisions_against_journey(
        records=[_patch_applied_record("q1", "P001")],
        events=[
            QuestionJourneyEvent(
                question_id="q1", stage="applied_targeted", proposal_id="P001",
            ),
        ],
    )
    assert violations == []


def test_validator_accepts_applied_broad_ag_scope_stage() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    violations = validate_decisions_against_journey(
        records=[_patch_applied_record("q1", "P001")],
        events=[
            QuestionJourneyEvent(
                question_id="q1", stage="applied_broad_ag_scope", proposal_id="P001",
            ),
        ],
    )
    assert violations == []


def test_validator_accepts_plain_applied_stage_unchanged() -> None:
    """Backwards-compat: legacy fixtures that still emit `applied`
    must keep validating."""
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    violations = validate_decisions_against_journey(
        records=[_patch_applied_record("q1", "P001")],
        events=[
            QuestionJourneyEvent(
                question_id="q1", stage="applied", proposal_id="P001",
            ),
        ],
    )
    assert violations == []


def test_validator_rejects_when_no_applied_family_event_present() -> None:
    """Negative case: still fails when none of the applied-family stages
    match — the contract is widened, not removed."""
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    violations = validate_decisions_against_journey(
        records=[_patch_applied_record("q1", "P001")],
        events=[QuestionJourneyEvent(question_id="q1", stage="evaluated")],
    )
    assert any(
        "has no matching journey stage applied" in v for v in violations
    )
