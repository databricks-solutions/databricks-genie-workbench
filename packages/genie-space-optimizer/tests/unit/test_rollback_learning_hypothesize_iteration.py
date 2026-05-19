"""Plan 7 Task 9 — hypothesize_next_attempts_for_iteration walks the
AgOutcome and dispatches one LLM call per rolled-back cluster.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    IntentOutcome,
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.rollback_hypothesis_typed import (
    NextAttemptHypothesis,
)
from genie_space_optimizer.optimization.rollback_learning import (
    hypothesize_next_attempts_for_iteration,
)
from genie_space_optimizer.optimization.stages._context import StageContext
from genie_space_optimizer.optimization.stages.acceptance import (
    AgOutcome,
    AgOutcomeRecord,
)


def _ctx(decisions: list, iteration: int = 2) -> StageContext:
    return StageContext(
        run_id="r_x", iteration=iteration, space_id="s_x",
        domain="sales", catalog="main", schema="gso_test",
        apply_mode="dry_run",
        journey_emit=lambda **kw: None,
        decision_emit=lambda rec: decisions.append(rec),
        feature_flags={},
    )


def _intent(intent_id: str) -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id, intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="x",
        cluster_id="H001", target_qids=("gs_009",),
        blame_set=("sales.fact_sales.revenue",),
        rca_card_id="", ag_id="AG3",
    )


def _outcome_rolled_back(intent_id: str) -> IntentOutcome:
    return IntentOutcome(
        intent_id=intent_id, ag_id="AG3", outcome="rolled_back",
        applied_signature=f"sig_{intent_id}",
        applied_at_iter=2, rollback_reason="out_of_target_regression",
    )


def _outcome_accepted(intent_id: str) -> IntentOutcome:
    return IntentOutcome(
        intent_id=intent_id, ag_id="AG3", outcome="accepted",
        applied_signature=f"sig_{intent_id}",
        applied_at_iter=2, rollback_reason=None,
    )


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x", blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _success_envelope() -> str:
    return json.dumps({
        "result": {
            "why_failed": "x", "failure_mode": "x",
            "revised_repair_shape": None,
            "revised_patch_type": None,
            "revised_blame_set": None,
            "additional_evidence_needed": [],
            "forbidden_signatures": [],
            "confidence": "medium",
        },
        "declined": None,
    })


def _stub_sequence(envelopes: list[str]) -> MagicMock:
    client = MagicMock()
    completions = []
    for env in envelopes:
        choice = MagicMock()
        choice.message.content = env
        c = MagicMock()
        c.choices = [choice]
        c.usage = MagicMock(
            prompt_tokens=800, completion_tokens=300, total_tokens=1100,
        )
        completions.append(c)
    client.chat.completions.create.side_effect = completions
    return client


def _build_ag_outcome(*, intent_id: str, ag_outcome: str) -> AgOutcome:
    rec = AgOutcomeRecord(
        ag_id="AG3", outcome=ag_outcome,
        reason_code=ag_outcome,
        target_qids=("gs_009",), affected_qids=("gs_009",),
        content_fingerprints=(f"sig_{intent_id}",),
        intent_outcomes=(
            _outcome_rolled_back(intent_id)
            if ag_outcome == "rolled_back"
            else _outcome_accepted(intent_id),
        ),
    )
    return AgOutcome(
        outcomes_by_ag={"AG3": rec},
        qid_resolutions={"gs_009": (
            "fail_to_pass" if ag_outcome == "accepted" else "hold_fail"
        )},
        rolled_back_content_fingerprints=(
            frozenset({f"sig_{intent_id}"})
            if ag_outcome == "rolled_back"
            else frozenset()
        ),
        intent_outcomes_by_id={
            intent_id: (
                _outcome_rolled_back(intent_id)
                if ag_outcome == "rolled_back"
                else _outcome_accepted(intent_id)
            ),
        },
    )


def test_iteration_skips_when_no_rolled_back_outcomes(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")
    ag_outcome = _build_ag_outcome(
        intent_id="i_001", ag_outcome="accepted",
    )
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    decisions: list = []
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=ag_outcome,
            repair_intents_by_id={"i_001": _intent("i_001")},
            per_qid_evidence_by_cluster={"H001": {
                "gs_009": _evidence("gs_009"),
            }},
            critique_verdicts_by_intent_id={},
            pre_rows=(), post_rows=(),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_i_001"}},
            identifier_allowlist_by_ag={"AG3": {
                "sales.fact_sales.revenue",
            }},
            cluster_id_by_intent_id={"i_001": "H001"},
        )
    assert result == {}
    assert client.chat.completions.create.call_count == 0


def test_iteration_skips_entirely_when_flag_disabled(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "false")
    ag_outcome = _build_ag_outcome(
        intent_id="i_001", ag_outcome="rolled_back",
    )
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    decisions: list = []
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=ag_outcome,
            repair_intents_by_id={"i_001": _intent("i_001")},
            per_qid_evidence_by_cluster={"H001": {
                "gs_009": _evidence("gs_009"),
            }},
            critique_verdicts_by_intent_id={},
            pre_rows=(), post_rows=(),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_i_001"}},
            identifier_allowlist_by_ag={"AG3": {
                "sales.fact_sales.revenue",
            }},
            cluster_id_by_intent_id={"i_001": "H001"},
        )
    assert result == {}
    assert client.chat.completions.create.call_count == 0


def test_iteration_dispatches_one_llm_call_per_rolled_back_cluster(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")
    ag_outcome = _build_ag_outcome(
        intent_id="i_001", ag_outcome="rolled_back",
    )
    decisions: list = []
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_sequence([_success_envelope()]),
    ):
        result = hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=ag_outcome,
            repair_intents_by_id={"i_001": _intent("i_001")},
            per_qid_evidence_by_cluster={"H001": {
                "gs_009": _evidence("gs_009"),
            }},
            critique_verdicts_by_intent_id={},
            pre_rows=({"question_id": "gs_009",
                       "result_correctness": "no"},),
            post_rows=({"question_id": "gs_009",
                        "result_correctness": "no"},),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_i_001"}},
            identifier_allowlist_by_ag={"AG3": {
                "sales.fact_sales.revenue",
            }},
            cluster_id_by_intent_id={"i_001": "H001"},
        )
    assert set(result.keys()) == {"H001"}
    assert isinstance(result["H001"], NextAttemptHypothesis)
    assert result["H001"].cluster_id == "H001"
    assert result["H001"].confidence == "medium"


def test_iteration_emits_decision_record_per_dispatched_cluster(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")
    ag_outcome = _build_ag_outcome(
        intent_id="i_001", ag_outcome="rolled_back",
    )
    decisions: list = []
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_sequence([_success_envelope()]),
    ):
        hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=ag_outcome,
            repair_intents_by_id={"i_001": _intent("i_001")},
            per_qid_evidence_by_cluster={"H001": {
                "gs_009": _evidence("gs_009"),
            }},
            critique_verdicts_by_intent_id={},
            pre_rows=(), post_rows=(),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_i_001"}},
            identifier_allowlist_by_ag={"AG3": {
                "sales.fact_sales.revenue",
            }},
            cluster_id_by_intent_id={"i_001": "H001"},
        )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )
    recs = [
        r for r in decisions
        if getattr(r, "decision_type", None)
        == DecisionType.NEXT_ATTEMPT_HYPOTHESIZED
    ]
    assert len(recs) == 1


def test_iteration_skips_intents_with_missing_repair_intent(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "true")
    ag_outcome = _build_ag_outcome(
        intent_id="i_legacy", ag_outcome="rolled_back",
    )
    decisions: list = []
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = hypothesize_next_attempts_for_iteration(
            ctx=_ctx(decisions),
            ag_outcome=ag_outcome,
            repair_intents_by_id={},
            per_qid_evidence_by_cluster={"H001": {
                "gs_009": _evidence("gs_009"),
            }},
            critique_verdicts_by_intent_id={},
            pre_rows=(), post_rows=(),
            applied_patch_fingerprints_by_ag={"AG3": {"sig_i_legacy"}},
            identifier_allowlist_by_ag={"AG3": {
                "sales.fact_sales.revenue",
            }},
            cluster_id_by_intent_id={"i_legacy": "H001"},
        )
    assert result == {}
    assert client.chat.completions.create.call_count == 0
