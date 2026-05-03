"""Cycle 8 Bug 1 Phase 3b Task B — validator passes for Lever 5 gate records.

The Lever 5 structural gate emits records with
``decision_type=GATE_DECISION`` + ``reason_code=RCA_UNGROUNDED``.
Validate the record passes the cross-checker when both ``rca_id`` and
``root_cause`` are populated (the dominant case where a finding does
exist for the cluster) — no exemption needed.

Plan: ``docs/2026-05-04-cycle8-bug1-phase3b-lever5-structural-gate-rerouting-plan.md``
Task B.
"""
from __future__ import annotations


def test_validator_passes_for_lever5_gate_record_with_populated_grounding() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        lever5_structural_gate_records,
    )
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        validate_decisions_against_journey,
    )

    records = lever5_structural_gate_records(
        run_id="run_1",
        iteration=2,
        ag_id="AG_DECOMPOSED_H001",
        rca_id="rca_h001",
        root_cause="wrong_aggregation",
        target_qids=("gs_024",),
        drops=[{
            "ag_id": "AG_DECOMPOSED_H001",
            "source_clusters": ("H001",),
            "root_causes": ("wrong_aggregation",),
            "target_lever": 5,
            "had_example_sqls": False,
            "instruction_sections_dropped": True,
            "instruction_guidance_dropped": False,
        }],
    )
    events = [
        QuestionJourneyEvent(question_id="gs_024", stage="evaluated"),
    ]
    violations = validate_decisions_against_journey(records=records, events=events)
    assert violations == [], (
        f"Lever 5 gate record with populated rca_id/root_cause should "
        f"pass cross-checker; got: {violations}"
    )
