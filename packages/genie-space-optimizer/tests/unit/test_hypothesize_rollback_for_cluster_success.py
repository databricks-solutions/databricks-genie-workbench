"""Plan 7 Task 7 — hypothesize_rollback_for_cluster driver (success path)."""
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
    _build_request,
    hypothesize_rollback_for_cluster,
)


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=900, completion_tokens=350, total_tokens=1250,
    )
    client.chat.completions.create.return_value = completion
    return client


def _intent() -> RepairIntent:
    return RepairIntent(
        intent_id="intent_H001_AG3_001",
        intent_name="top_n_revenue_by_region",
        intent_description="top-N revenue by region",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="llm_l5b_synthesis",
        cluster_id="H001",
        target_qids=("gs_009", "gs_017"),
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
        rca_card_id="", ag_id="AG3",
    )


def _intent_outcome() -> IntentOutcome:
    return IntentOutcome(
        intent_id="intent_H001_AG3_001",
        ag_id="AG3",
        outcome="rolled_back",
        applied_signature="sig_top_n_abc123",
        applied_at_iter=2,
        rollback_reason="out_of_target_regression",
    )


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="missing top-N",
        generated_sql_issue="x",
        expected_sql_shape="ORDER BY revenue DESC LIMIT 3",
        blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="top_n",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _success_envelope() -> str:
    return json.dumps({
        "result": {
            "why_failed": (
                "rollback_reason=out_of_target_regression — gs_009/gs_017 "
                "fail_to_pass but gs_044/gs_055 pass_to_fail because top-N "
                "leaked to monthly-revenue questions."
            ),
            "failure_mode": "overgeneralized_top_n_to_monthly_breakdown",
            "revised_repair_shape": None,
            "revised_patch_type": "add_sql_snippet_filter",
            "revised_blame_set": None,
            "additional_evidence_needed": [],
            "forbidden_signatures": ["sig_top_n_abc123"],
            "confidence": "high",
        },
        "declined": None,
    })


def test_driver_returns_typed_hypothesis_on_success() -> None:
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_success_envelope()),
    ):
        hypothesis = hypothesize_rollback_for_cluster(
            w=None,
            cluster_id="H001",
            ag_id="AG3",
            iteration=2,
            rolled_back_repair_intent=_intent(),
            intent_outcome=_intent_outcome(),
            per_qid_evidence={"gs_009": _evidence("gs_009"),
                              "gs_017": _evidence("gs_017")},
            critique_verdict=None,
            eval_diffs_for_cluster=({"qid": "gs_009",
                                     "pre_correctness": "no",
                                     "post_correctness": "yes",
                                     "pre_arbiter": "no",
                                     "post_arbiter": "yes",
                                     "transition": "fail_to_pass"},),
            identifier_allowlist={"sales.fact_sales.revenue",
                                  "sales.fact_sales.region"},
            applied_patch_fingerprints={"sig_top_n_abc123"},
        )
    assert hypothesis is not None
    assert isinstance(hypothesis, NextAttemptHypothesis)
    assert hypothesis.rolled_back_intent_id == "intent_H001_AG3_001"
    assert hypothesis.cluster_id == "H001"
    assert hypothesis.ag_id == "AG3"
    assert hypothesis.iteration == 2
    assert hypothesis.confidence == "high"
    assert hypothesis.revised_patch_type == PatchType.ADD_SQL_SNIPPET_FILTER
    assert hypothesis.forbidden_signatures == ("sig_top_n_abc123",)


def test_driver_call_id_is_cluster_iteration_scoped() -> None:
    """call_id format: "rollback_learning.iter_{N}.{cluster_id}"."""
    request = _build_request(
        cluster_id="H001",
        ag_id="AG3",
        iteration=5,
        rolled_back_repair_intent=_intent(),
        intent_outcome=_intent_outcome(),
        per_qid_evidence={},
        critique_verdict=None,
        eval_diffs_for_cluster=(),
        identifier_allowlist=set(),
        applied_patch_fingerprints=set(),
    )
    assert request.call_id == "rollback_learning.iter_5.H001"
    assert request.skill_id == "rollback-learning"
    assert request.max_tokens == 700


def test_driver_rendered_prompt_includes_all_required_context() -> None:
    captured: list[dict] = []
    client = MagicMock()

    def _spy(**kwargs):
        captured.append(kwargs)
        choice = MagicMock()
        choice.message.content = _success_envelope()
        completion = MagicMock()
        completion.choices = [choice]
        completion.usage = MagicMock(
            prompt_tokens=900, completion_tokens=350, total_tokens=1250,
        )
        return completion

    client.chat.completions.create.side_effect = _spy

    with patch.object(optimizer, "_get_openai_client", return_value=client):
        hypothesize_rollback_for_cluster(
            w=None, cluster_id="H001", ag_id="AG3", iteration=2,
            rolled_back_repair_intent=_intent(),
            intent_outcome=_intent_outcome(),
            per_qid_evidence={"gs_009": _evidence("gs_009"),
                              "gs_017": _evidence("gs_017")},
            critique_verdict=None,
            eval_diffs_for_cluster=({"qid": "gs_009",
                                     "transition": "fail_to_pass"},
                                    {"qid": "gs_044",
                                     "transition": "pass_to_fail"}),
            identifier_allowlist={"sales.fact_sales.revenue",
                                  "sales.fact_sales.region"},
            applied_patch_fingerprints={"sig_top_n_abc123"},
        )

    assert len(captured) == 1
    user_msg = next(
        m["content"] for m in captured[0]["messages"] if m["role"] == "user"
    )
    assert '"cluster_id": "H001"' in user_msg
    assert '"ag_id": "AG3"' in user_msg
    assert "top_n_revenue_by_region" in user_msg
    assert "out_of_target_regression" in user_msg
    assert "gs_009" in user_msg
    assert "fail_to_pass" in user_msg
    assert "pass_to_fail" in user_msg
    assert "sales.fact_sales.revenue" in user_msg
    assert "sig_top_n_abc123" in user_msg


def test_driver_returns_none_when_validators_reject_revised_blame_set() -> None:
    envelope = json.dumps({
        "result": {
            "why_failed": "x", "failure_mode": "x",
            "revised_repair_shape": None,
            "revised_patch_type": None,
            "revised_blame_set": ["sales.fact_sales.evil_column"],
            "additional_evidence_needed": [],
            "forbidden_signatures": [],
            "confidence": "high",
        },
        "declined": None,
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(envelope),
    ):
        hypothesis = hypothesize_rollback_for_cluster(
            w=None, cluster_id="H001", ag_id="AG3", iteration=2,
            rolled_back_repair_intent=_intent(),
            intent_outcome=_intent_outcome(),
            per_qid_evidence={},
            critique_verdict=None,
            eval_diffs_for_cluster=(),
            identifier_allowlist={"sales.fact_sales.revenue"},
            applied_patch_fingerprints=set(),
        )
    assert hypothesis is None


def test_driver_returns_none_when_validators_reject_forbidden_signatures() -> None:
    envelope = json.dumps({
        "result": {
            "why_failed": "x", "failure_mode": "x",
            "revised_repair_shape": None,
            "revised_patch_type": None,
            "revised_blame_set": None,
            "additional_evidence_needed": [],
            "forbidden_signatures": ["fp_hallucinated"],
            "confidence": "high",
        },
        "declined": None,
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(envelope),
    ):
        hypothesis = hypothesize_rollback_for_cluster(
            w=None, cluster_id="H001", ag_id="AG3", iteration=2,
            rolled_back_repair_intent=_intent(),
            intent_outcome=_intent_outcome(),
            per_qid_evidence={},
            critique_verdict=None,
            eval_diffs_for_cluster=(),
            identifier_allowlist=set(),
            applied_patch_fingerprints={"sig_top_n_abc123"},
        )
    assert hypothesis is None
