"""Phase B unified-trace contract tests.

Pin the typed DecisionRecord / OptimizationTrace contract that every
Phase B projection (replay, fixtures, persistence, operator transcript)
derives from. See
``docs/2026-05-02-unified-trace-and-operator-transcript-plan.md``.
"""
from __future__ import annotations

import json


def test_decision_record_to_dict_uses_stable_json_safe_shape() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
    )

    rec = DecisionRecord(
        run_id="run_1",
        iteration=2,
        decision_type=DecisionType.GATE_DECISION,
        outcome=DecisionOutcome.DROPPED,
        reason_code=ReasonCode.PATCH_CAP_DROPPED,
        question_id="q2",
        cluster_id="H002",
        ag_id="AG1",
        proposal_id="P002",
        patch_id="P002#1",
        gate="patch_cap",
        reason_detail="lower_causal_rank",
        affected_qids=("q2",),
        source_cluster_ids=("H002",),
        proposal_ids=("P002",),
        metrics={"rank": 2, "relevance_score": 0.42},
    )

    assert rec.to_dict() == {
        "run_id": "run_1",
        "iteration": 2,
        "decision_type": "gate_decision",
        "outcome": "dropped",
        "reason_code": "patch_cap_dropped",
        "question_id": "q2",
        "cluster_id": "H002",
        "ag_id": "AG1",
        "proposal_id": "P002",
        "patch_id": "P002#1",
        "gate": "patch_cap",
        "reason_detail": "lower_causal_rank",
        "affected_qids": ["q2"],
        "source_cluster_ids": ["H002"],
        "proposal_ids": ["P002"],
        "metrics": {"rank": 2, "relevance_score": 0.42},
    }


def test_canonical_decision_json_is_order_independent() -> None:
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        ReasonCode,
        canonical_decision_json,
    )

    later = DecisionRecord(
        run_id="run_1",
        iteration=2,
        decision_type=DecisionType.PATCH_APPLIED,
        outcome=DecisionOutcome.APPLIED,
        reason_code=ReasonCode.PATCH_APPLIED,
        question_id="q2",
        proposal_id="P002",
    )
    earlier = DecisionRecord(
        run_id="run_1",
        iteration=1,
        decision_type=DecisionType.EVAL_CLASSIFIED,
        outcome=DecisionOutcome.INFO,
        reason_code=ReasonCode.HARD_FAILURE,
        question_id="q1",
    )

    left = canonical_decision_json([later, earlier])
    right = canonical_decision_json([earlier, later])

    assert left == right
    assert json.loads(left)[0]["iteration"] == 1
    assert json.loads(left)[1]["decision_type"] == "patch_applied"


def test_optimization_trace_serializes_decisions_and_renders_transcript() -> None:
    from genie_space_optimizer.optimization.question_journey import QuestionJourneyEvent
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionRecord,
        DecisionType,
        OptimizationTrace,
        ReasonCode,
    )

    trace = OptimizationTrace(
        journey_events=(
            QuestionJourneyEvent(question_id="q1", stage="evaluated"),
            QuestionJourneyEvent(
                question_id="q1",
                stage="clustered",
                cluster_id="H001",
                root_cause="missing_filter",
            ),
        ),
        decision_records=(
            DecisionRecord(
                run_id="run_1",
                iteration=1,
                decision_type=DecisionType.CLUSTER_SELECTED,
                outcome=DecisionOutcome.INFO,
                reason_code=ReasonCode.CLUSTERED,
                question_id="q1",
                cluster_id="H001",
                reason_detail="missing_filter",
            ),
        ),
    )

    assert "cluster_selected" in trace.canonical_decision_json()
    transcript = trace.render_operator_transcript(iteration=1)
    assert "OPERATOR TRANSCRIPT  iteration=1" in transcript
    assert "Decision records: 1" in transcript
    assert "cluster_selected" in transcript
    assert "q1" in transcript
