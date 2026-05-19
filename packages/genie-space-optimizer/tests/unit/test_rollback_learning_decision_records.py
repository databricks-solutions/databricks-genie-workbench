"""Plan 7 Task 13 — pin the NEXT_ATTEMPT_HYPOTHESIZED decision-record shape."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionType,
    ReasonCode,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    IntentOutcome,
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.rollback_learning import (
    hypothesize_next_attempts_for_iteration,
)
from genie_space_optimizer.optimization.stages._context import StageContext
from genie_space_optimizer.optimization.stages.acceptance import (
    AgOutcome,
    AgOutcomeRecord,
)


def _ctx(decisions: list) -> StageContext:
    return StageContext(
        run_id="r", iteration=2, space_id="s", domain="d",
        catalog="c", schema="x", apply_mode="dry_run",
        journey_emit=lambda **kw: None,
        decision_emit=lambda rec: decisions.append(rec),
        feature_flags={},
    )


def _intent(intent_id: str = "i_001") -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id, intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="x",
        cluster_id="H001", target_qids=("gs_009",),
        blame_set=("sales.fact_sales.revenue",),
        rca_card_id="", ag_id="AG3",
    )


def _outcome() -> IntentOutcome:
    return IntentOutcome(
        intent_id="i_001", ag_id="AG3", outcome="rolled_back",
        applied_signature="sig_a", applied_at_iter=2,
        rollback_reason="out_of_target_regression",
    )


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x", blame_set=(),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _ag_outcome() -> AgOutcome:
    rec = AgOutcomeRecord(
        ag_id="AG3", outcome="rolled_back",
        reason_code="out_of_target_regression",
        target_qids=("gs_009",), affected_qids=("gs_009",),
        content_fingerprints=("sig_a",),
        intent_outcomes=(_outcome(),),
    )
    return AgOutcome(
        outcomes_by_ag={"AG3": rec},
        qid_resolutions={"gs_009": "hold_fail"},
        rolled_back_content_fingerprints=frozenset({"sig_a"}),
        intent_outcomes_by_id={"i_001": _outcome()},
    )


def _stub_success() -> MagicMock:
    client = MagicMock()
    choice = MagicMock()
    choice.message.content = json.dumps({
        "result": {
            "why_failed": "patch was too broad",
            "failure_mode": "overgeneralized_filter",
            "revised_repair_shape": None,
            "revised_patch_type": "add_sql_snippet_filter",
            "revised_blame_set": None,
            "additional_evidence_needed": [],
            "forbidden_signatures": ["sig_a"],
            "confidence": "high",
        },
        "declined": None,
    })
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=900, completion_tokens=350, total_tokens=1250,
    )
    client.chat.completions.create.return_value = completion
    return client


def test_success_emits_record_with_high_confidence_reason_code(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")
    decisions: list = []
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_success(),
    ):
        hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=_ag_outcome(),
            repair_intents_by_id={"i_001": _intent("i_001")},
            per_qid_evidence_by_cluster={"H001": {"gs_009": _evidence("gs_009")}},
            critique_verdicts_by_intent_id={},
            pre_rows=(), post_rows=(),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_a"}},
            identifier_allowlist_by_ag={"AG3": {"sales.fact_sales.revenue"}},
            cluster_id_by_intent_id={"i_001": "H001"},
        )
    recs = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.NEXT_ATTEMPT_HYPOTHESIZED
    ]
    assert len(recs) == 1
    rec = recs[0]
    assert rec.reason_code == ReasonCode.HYPOTHESIS_HIGH_CONFIDENCE
    assert rec.cluster_id == "H001"
    assert rec.ag_id == "AG3"
    assert rec.metrics["rolled_back_intent_id"] == "i_001"
    assert rec.metrics["failure_mode"] == "overgeneralized_filter"
    assert rec.metrics["confidence"] == "high"
    assert rec.metrics["revised_patch_type"] == "add_sql_snippet_filter"
    assert rec.metrics["forbidden_signatures"] == ["sig_a"]


def test_declined_emits_record_with_hypothesis_declined_reason_code(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")
    decline_envelope = json.dumps({
        "result": None,
        "declined": {
            "reason": "insufficient_signal",
            "explanation": "x",
            "needed_evidence": [],
            "suggested_next_step": "x",
        },
    })
    client = MagicMock()
    choice = MagicMock()
    choice.message.content = decline_envelope
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=600, completion_tokens=100, total_tokens=700,
    )
    client.chat.completions.create.return_value = completion

    decisions: list = []
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=_ag_outcome(),
            repair_intents_by_id={"i_001": _intent("i_001")},
            per_qid_evidence_by_cluster={"H001": {"gs_009": _evidence("gs_009")}},
            critique_verdicts_by_intent_id={},
            pre_rows=(), post_rows=(),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_a"}},
            identifier_allowlist_by_ag={"AG3": {"sales.fact_sales.revenue"}},
            cluster_id_by_intent_id={"i_001": "H001"},
        )
    recs = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.NEXT_ATTEMPT_HYPOTHESIZED
    ]
    assert len(recs) == 1
    assert recs[0].reason_code == ReasonCode.HYPOTHESIS_DECLINED


def test_missing_intent_stamp_emits_validation_rejected_reason_code(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")
    decisions: list = []
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=_ag_outcome(),
            repair_intents_by_id={},
            per_qid_evidence_by_cluster={"H001": {"gs_009": _evidence("gs_009")}},
            critique_verdicts_by_intent_id={},
            pre_rows=(), post_rows=(),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_a"}},
            identifier_allowlist_by_ag={"AG3": {"sales.fact_sales.revenue"}},
            cluster_id_by_intent_id={"i_001": "H001"},
        )
    recs = [
        r for r in decisions
        if getattr(r, "decision_type", None) == DecisionType.NEXT_ATTEMPT_HYPOTHESIZED
    ]
    assert len(recs) == 1
    assert recs[0].reason_code == ReasonCode.HYPOTHESIS_VALIDATION_REJECTED
    assert client.chat.completions.create.call_count == 0
