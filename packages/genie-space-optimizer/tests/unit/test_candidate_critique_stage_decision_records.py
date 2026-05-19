"""Plan 6 Task 10 — pin the CANDIDATE_CRITIQUED decision-record shape.

Postmortem joining + operator transcript rendering both depend on a
stable record shape. Pin: decision_type, outcome, reason_code,
affected_qids (= predicted regressions), and the metrics dict.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionOutcome,
    DecisionType,
    ReasonCode,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.stages._context import StageContext
from genie_space_optimizer.optimization.stages.candidate_critique import (
    CritiqueInput,
    execute,
)


def _ctx(decisions: list) -> StageContext:
    return StageContext(
        run_id="r", iteration=2, space_id="s", domain="d",
        catalog="c", schema="x", apply_mode="dry_run",
        journey_emit=lambda **kw: None,
        decision_emit=lambda rec: decisions.append(rec),
        feature_flags={},
    )


def _proposal_intent_evidence_input(recommendation: str) -> CritiqueInput:
    intent = RepairIntent(
        intent_id="intent_001",
        intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="x",
        cluster_id="H001", target_qids=(), blame_set=(),
        rca_card_id="", ag_id="AG3",
    )
    ev = PerQidRcaEvidence(
        qid="gs_001", observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x", blame_set=(),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )
    return CritiqueInput(
        proposals_by_ag={"AG3": (
            {"proposal_id": "prop_001",
             "intent_id": "intent_001",
             "repair_intent": intent.to_json()},
        )},
        repair_intents_by_id={"intent_001": intent},
        rca_evidence_typed_by_cluster={"H001": {"gs_001": ev}},
        passing_qids_at_risk_by_proposal_id={"prop_001": ("gs_044",)},
        cluster_semantic_theme_by_cluster={"H001": "x"},
        cluster_id_by_proposal_id={"prop_001": "H001"},
        ag_id_by_proposal_id={"prop_001": "AG3"},
    )


def _envelope(recommendation: str) -> str:
    return json.dumps({
        "result": {
            "addresses_target_failure": recommendation != "discard",
            "is_overgeneralized": recommendation == "discard",
            "likely_neighbor_regressions": ["gs_044"] if recommendation != "proceed" else [],
            "matches_intended_shape": recommendation != "discard",
            "overall_recommendation": recommendation,
            "rationale": f"{recommendation} reasoning",
        },
        "declined": None,
    })


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock()
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=800, completion_tokens=200, total_tokens=1000,
    )
    client.chat.completions.create.return_value = completion
    return client


def test_proceed_emits_info_outcome_with_critique_proceed_reason(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    decisions: list = []
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_envelope("proceed")),
    ):
        execute(_ctx(decisions),
                _proposal_intent_evidence_input("proceed"))

    critique_records = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.CANDIDATE_CRITIQUED
    ]
    assert len(critique_records) == 1
    rec = critique_records[0]
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.CRITIQUE_PROCEED


def test_rework_emits_info_outcome_with_critique_rework_reason(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    decisions: list = []
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_envelope("rework")),
    ):
        execute(_ctx(decisions),
                _proposal_intent_evidence_input("rework"))

    critique_records = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.CANDIDATE_CRITIQUED
    ]
    rec = critique_records[0]
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.CRITIQUE_REWORK


def test_discard_advisory_emits_info_outcome_not_dropped(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    decisions: list = []
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_envelope("discard")),
    ):
        execute(_ctx(decisions),
                _proposal_intent_evidence_input("discard"))

    critique_records = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.CANDIDATE_CRITIQUED
    ]
    rec = critique_records[0]
    assert rec.outcome == DecisionOutcome.INFO
    assert rec.reason_code == ReasonCode.CRITIQUE_DISCARD
    assert rec.metrics["is_blocked_by_critique"] is False


def test_discard_enforcing_emits_dropped_outcome(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "true")
    decisions: list = []
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_envelope("discard")),
    ):
        execute(_ctx(decisions),
                _proposal_intent_evidence_input("discard"))

    critique_records = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.CANDIDATE_CRITIQUED
    ]
    rec = critique_records[0]
    assert rec.outcome == DecisionOutcome.DROPPED
    assert rec.reason_code == ReasonCode.CRITIQUE_DISCARD
    assert rec.metrics["is_blocked_by_critique"] is True


def test_record_carries_affected_qids_equal_to_predicted_regressions(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    decisions: list = []
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_envelope("discard")),
    ):
        execute(_ctx(decisions),
                _proposal_intent_evidence_input("discard"))

    rec = decisions[0]
    assert rec.affected_qids == ("gs_044",)
    assert rec.target_qids == ("gs_044",)


def test_record_evidence_refs_carry_proposal_id(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CRITIQUE_GATE_ENFORCING", "false")
    decisions: list = []
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_envelope("proceed")),
    ):
        execute(_ctx(decisions),
                _proposal_intent_evidence_input("proceed"))

    rec = decisions[0]
    assert rec.evidence_refs == ("proposal:prop_001",)
