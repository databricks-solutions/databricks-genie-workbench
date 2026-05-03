"""Phase C Task 7 — validator allows RCA_FORMED + RCA_UNGROUNDED
without an rca_id.

Cycle 9 / Phase B's validator enforces ``rca_id`` non-empty for every
``rca_required`` decision_type. The new no-RCA-for-cluster record
deliberately carries an empty ``rca_id`` (there is no RCA to point
at). The validator must exempt this exact combination — but only
when ``reason_code == RCA_UNGROUNDED`` so a record with empty
rca_id under any other reason still flags loud.

Plan: ``docs/2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md`` Task 7.
"""
from __future__ import annotations


def test_validator_allows_rca_formed_ungrounded_without_rca_id() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
        validate_decisions_against_journey,
    )

    record = DecisionRecord(
        run_id="r",
        iteration=1,
        decision_type=DecisionType.RCA_FORMED,
        outcome=DecisionOutcome.UNRESOLVED,
        reason_code=ReasonCode.RCA_UNGROUNDED,
        cluster_id="H001",
        rca_id="",
        root_cause="unknown",
        evidence_refs=("cluster:H001",),
        target_qids=("q1",),
        affected_qids=("q1",),
    )
    violations = validate_decisions_against_journey(records=[record], events=[])
    # Validator may flag root_cause="unknown" — that's fine. The
    # specific violation we are exempting is "has no rca_id".
    assert all("has no rca_id" not in v for v in violations), (
        f"Validator should exempt RCA_FORMED+RCA_UNGROUNDED from "
        f"the rca_id requirement; got: {violations}"
    )


def test_validator_still_flags_other_decision_types_with_empty_rca_id() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
        validate_decisions_against_journey,
    )

    record = DecisionRecord(
        run_id="r",
        iteration=1,
        decision_type=DecisionType.PATCH_APPLIED,
        outcome=DecisionOutcome.APPLIED,
        reason_code=ReasonCode.RCA_UNGROUNDED,
        ag_id="AG1",
        proposal_id="P001",
        rca_id="",
        root_cause="missing_filter",
        evidence_refs=("ag:AG1",),
        target_qids=("q1",),
        affected_qids=("q1",),
    )
    violations = validate_decisions_against_journey(records=[record], events=[])
    assert any("has no rca_id" in v for v in violations), (
        f"Exemption is scoped to RCA_FORMED only; other types must "
        f"still flag empty rca_id. Got: {violations}"
    )
