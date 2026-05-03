"""TDD coverage for blast-radius DecisionRecord emission (Cycle9 T6).

Per postmortem follow-up: producer lives in decision_emitters.py and
populates the RCA-grounding contract fields (no `details` field).
Cycle 9 demonstrated that AGs fully dropped by the blast-radius gate
contributed zero DecisionRecords (the patch-cap producer never fired)
and Phase B's operator transcript rendered nothing.

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T6.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.decision_emitters import (
    blast_radius_decision_records,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
)


def _dropped(proposal_id: str, target: str, outside: list[str]) -> dict:
    return {
        "proposal_id": proposal_id,
        "patch_type": "add_sql_snippet_filter",
        "reason": "high_collateral_risk_flagged",
        "passing_dependents_outside_target": outside,
        "target": target,
    }


def test_emits_one_record_per_dropped_patch():
    records = blast_radius_decision_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG_DECOMPOSED_H001",
        rca_id="rca_h001",
        root_cause="missing_filter",
        target_qids=["gs_024"],
        dropped=[
            _dropped("P001#1", "ucat.dev.tkt_payment", ["gs_003"]),
            _dropped("P002#1", "ucat.dev.tkt_payment", ["gs_003"]),
        ],
    )
    assert len(records) == 2
    for r in records:
        assert r.decision_type == DecisionType.GATE_DECISION
        assert r.outcome == DecisionOutcome.DROPPED
        assert r.reason_code == ReasonCode.NO_CAUSAL_TARGET
        assert r.ag_id == "AG_DECOMPOSED_H001"
        assert r.gate == "blast_radius"
        assert r.rca_id == "rca_h001"
        assert r.root_cause == "missing_filter"
        assert r.evidence_refs == ("ag:AG_DECOMPOSED_H001", "blast_radius_gate")
        assert r.target_qids == ("gs_024",)
        # Gate-specific bits land in metrics; cross-checker doesn't read these.
        assert "passing_dependents_outside_target" in r.metrics
        assert r.metrics["passing_dependents_outside_target"] == ["gs_003"]
        assert r.metrics["target"] == "ucat.dev.tkt_payment"


def test_returns_empty_for_no_drops():
    assert blast_radius_decision_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG_X",
        rca_id="",
        root_cause="",
        target_qids=[],
        dropped=[],
    ) == []


def test_empty_target_qids_is_allowed():
    """target_qids=[] reflects an unscoped AG; the producer still emits
    records but cross-checker may flag missing target_qids elsewhere."""
    records = blast_radius_decision_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG_X",
        rca_id="",
        root_cause="",
        target_qids=[],
        dropped=[_dropped("P001", "ucat.dev.t1", [])],
    )
    assert len(records) == 1
    assert records[0].target_qids == ()


def test_blast_radius_record_passes_cross_checker_when_grounded():
    """A blast-radius record with rca_id + root_cause + target_qids
    + evidence_refs satisfies the RCA-grounding contract."""
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    records = blast_radius_decision_records(
        run_id="run_1",
        iteration=1,
        ag_id="AG_DECOMPOSED_H001",
        rca_id="rca_h001",
        root_cause="missing_filter",
        target_qids=["gs_024"],
        dropped=[_dropped("P001#1", "ucat.dev.tkt_payment", ["gs_003"])],
    )
    events = [
        QuestionJourneyEvent(question_id="gs_024", stage="evaluated"),
    ]

    violations = validate_decisions_against_journey(
        records=records, events=events,
    )
    assert violations == []
