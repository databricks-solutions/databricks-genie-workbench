"""TDD coverage for per-signature dead-on-arrival DecisionRecord emission (Cycle9 T7).

Per postmortem follow-up: ACCEPTANCE_DECIDED already covers the AG-level
``skipped_dead_on_arrival`` / ``skipped_no_applied_patches`` outcomes via
``_phase_b_emit_ag_outcome_record``. T7's added value is finer-grained
``PATCH_SKIPPED`` records — one per proposal_id in the dead-on-arrival
signature — so the operator can attribute "which specific patch was the
no-op" rather than just "the AG dropped."

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T7.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.decision_emitters import (
    dead_on_arrival_decision_records,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
)


def test_emits_one_patch_skipped_per_signature_entry():
    records = dead_on_arrival_decision_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG_DECOMPOSED_H001",
        rca_id="rca_h001",
        root_cause="missing_filter",
        target_qids=["gs_024"],
        signature=("P001#1", "P002#1"),
        reason="all_selected_patches_dropped_by_applier",
    )
    assert len(records) == 2
    for r in records:
        assert r.decision_type == DecisionType.PATCH_SKIPPED
        assert r.outcome == DecisionOutcome.SKIPPED
        assert r.reason_code == ReasonCode.NO_APPLIED_PATCHES
        assert r.ag_id == "AG_DECOMPOSED_H001"
        assert r.rca_id == "rca_h001"
        assert r.root_cause == "missing_filter"
        assert r.target_qids == ("gs_024",)
        assert r.metrics["signature"] == ["P001#1", "P002#1"]
    assert [r.proposal_id for r in records] == ["P001#1", "P002#1"]


def test_empty_signature_returns_empty_list():
    """Blast-radius dropped every patch → empty signature → no
    per-patch records (ACCEPTANCE_DECIDED carries the AG-level signal)."""
    records = dead_on_arrival_decision_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG_X",
        rca_id="",
        root_cause="",
        target_qids=[],
        signature=(),
        reason="all_patches_dropped_by_blast_radius",
    )
    assert records == []


def test_empty_proposal_ids_in_signature_are_skipped():
    records = dead_on_arrival_decision_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG_X",
        rca_id="rca_h001",
        root_cause="missing_filter",
        target_qids=["gs_024"],
        signature=("P001", "", "P002"),
        reason="r",
    )
    assert [r.proposal_id for r in records] == ["P001", "P002"]


def test_dead_on_arrival_records_pass_cross_checker_when_grounded():
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    records = dead_on_arrival_decision_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG_DECOMPOSED_H001",
        rca_id="rca_h001",
        root_cause="missing_filter",
        target_qids=["gs_024"],
        signature=("P001#1",),
        reason="all_selected_patches_dropped_by_applier",
    )
    events = [
        QuestionJourneyEvent(question_id="gs_024", stage="evaluated"),
    ]

    violations = validate_decisions_against_journey(
        records=records, events=events,
    )
    # PATCH_SKIPPED isn't in stage_requirements (no journey-stage
    # cross-check) so violations should be empty modulo RCA-required
    # checks, all of which are populated.
    assert violations == []
