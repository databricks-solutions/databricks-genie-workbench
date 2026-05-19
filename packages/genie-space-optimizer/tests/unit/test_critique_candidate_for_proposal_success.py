"""Plan 6 Task 6 — critique_candidate_for_proposal driver (success path).

Builds an LlmReasoningRequest from the proposal + intent + per-qid
evidence + passing-qid neighbor list, dispatches through Plan 2's
LlmReasoningCall, returns CritiqueVerdict | None.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.candidate_critique_typed import (
    CritiqueVerdict,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.stages.candidate_critique import (
    _build_request,
    critique_candidate_for_proposal,
)


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=800, completion_tokens=200, total_tokens=1000,
    )
    client.chat.completions.create.return_value = completion
    return client


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="missing LIMIT",
        generated_sql_issue="no top-n",
        expected_sql_shape="GROUP BY region ORDER BY revenue DESC LIMIT 3",
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _intent() -> RepairIntent:
    return RepairIntent(
        intent_id="intent_H001_AG3_001",
        intent_name="top_n_revenue_by_region",
        intent_description="add example_sql for top-N revenue by region",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="cluster blames missing LIMIT/ORDER BY",
        confidence="high", source="llm_l5b_synthesis",
        cluster_id="H001",
        target_qids=("gs_009", "gs_017"),
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
        rca_card_id="", ag_id="AG3",
    )


def _success_envelope() -> str:
    return json.dumps({
        "result": {
            "addresses_target_failure": True,
            "is_overgeneralized": False,
            "likely_neighbor_regressions": [],
            "matches_intended_shape": True,
            "overall_recommendation": "proceed",
            "rationale": "example_sql cleanly demonstrates top-N pattern",
        },
        "declined": None,
    })


def test_driver_returns_typed_verdict_on_success() -> None:
    proposal = {
        "proposal_id": "prop_H001_AG3_001",
        "example_question": "What are the top 3 regions by revenue?",
        "example_sql": "SELECT region, SUM(revenue) FROM sales.fact_sales GROUP BY region ORDER BY 2 DESC LIMIT 3",
        "usage_guidance": "use for top-N",
        "parameters": [],
        "repair_intent": _intent().to_json(),
        "intent_id": "intent_H001_AG3_001",
    }
    rca = {"gs_009": _evidence("gs_009"), "gs_017": _evidence("gs_017")}
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_success_envelope()),
    ):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=proposal,
            cluster_id="H001", ag_id="AG3", iteration=2,
            cluster_semantic_theme="top-N revenue ranking missing",
            per_qid_evidence=rca,
            passing_qids_at_risk=("gs_044", "gs_055"),
        )
    assert verdict is not None
    assert isinstance(verdict, CritiqueVerdict)
    assert verdict.proposal_id == "prop_H001_AG3_001"
    assert verdict.overall_recommendation == "proceed"
    assert verdict.addresses_target_failure is True
    assert verdict.likely_neighbor_regressions == ()


def test_driver_call_id_is_proposal_iteration_scoped() -> None:
    """call_id format: "candidate_critique.iter_{N}.{proposal_id}".
    Joinable in postmortems."""
    proposal = {
        "proposal_id": "prop_H001_AG3_001",
        "repair_intent": _intent().to_json(),
    }
    request = _build_request(
        proposal=proposal,
        cluster_id="H001", ag_id="AG3", iteration=5,
        cluster_semantic_theme="x",
        per_qid_evidence={},
        passing_qids_at_risk=(),
    )
    assert request.call_id == "candidate_critique.iter_5.prop_H001_AG3_001"
    assert request.skill_id == "candidate-critique"
    assert request.max_tokens == 500


def test_driver_rendered_prompt_includes_all_required_context() -> None:
    """Smoke-test the user_prompt JSON serialisation: it must contain
    every promised <context_inputs> field so the LLM has just-in-time
    grounding."""
    proposal = {
        "proposal_id": "prop_H001_AG3_001",
        "example_question": "What are the top 3 regions?",
        "example_sql": "SELECT 1",
        "repair_intent": _intent().to_json(),
    }
    captured: list[dict] = []
    client = MagicMock()

    def _spy(**kwargs):
        captured.append(kwargs)
        choice = MagicMock()
        choice.message.content = _success_envelope()
        completion = MagicMock()
        completion.choices = [choice]
        completion.usage = MagicMock(
            prompt_tokens=800, completion_tokens=200, total_tokens=1000,
        )
        return completion

    client.chat.completions.create.side_effect = _spy
    rca = {"gs_009": _evidence("gs_009"), "gs_017": _evidence("gs_017")}

    with patch.object(optimizer, "_get_openai_client", return_value=client):
        critique_candidate_for_proposal(
            w=None, proposal=proposal,
            cluster_id="H001", ag_id="AG3", iteration=2,
            cluster_semantic_theme="top-N revenue ranking missing",
            per_qid_evidence=rca,
            passing_qids_at_risk=("gs_044", "gs_055"),
        )

    assert len(captured) == 1
    user_msg = next(
        m["content"] for m in captured[0]["messages"] if m["role"] == "user"
    )
    assert '"proposal_id": "prop_H001_AG3_001"' in user_msg
    assert '"cluster_id": "H001"' in user_msg
    assert '"ag_id": "AG3"' in user_msg
    assert "top_n_revenue_by_region" in user_msg
    assert "top_n_by_metric" in user_msg
    assert "What are the top 3 regions?" in user_msg
    assert "gs_009" in user_msg
    assert "gs_017" in user_msg
    assert "gs_044" in user_msg
    assert "gs_055" in user_msg


def test_driver_returns_none_when_proposal_has_no_repair_intent() -> None:
    """Per SKILL.md insufficient_signal rule — no stamp → declined.
    Driver short-circuits without dispatching the LLM (cheaper)."""
    proposal = {
        "proposal_id": "prop_unstamped",
        "example_question": "x",
        "example_sql": "SELECT 1",
    }
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=proposal,
            cluster_id="H001", ag_id="AG3", iteration=2,
            cluster_semantic_theme="x",
            per_qid_evidence={"gs_009": _evidence("gs_009")},
            passing_qids_at_risk=(),
        )
    assert verdict is None
    assert client.chat.completions.create.call_count == 0


def test_driver_filters_predicted_regressions_outside_passing_qids_at_risk() -> None:
    """LLM may hallucinate qids not in passing_qids_at_risk; the
    framework silently drops them post-parse (data hygiene)."""
    envelope = json.dumps({
        "result": {
            "addresses_target_failure": True,
            "is_overgeneralized": True,
            "likely_neighbor_regressions": ["gs_044", "gs_hallucinated", "gs_055"],
            "matches_intended_shape": True,
            "overall_recommendation": "rework",
            "rationale": "x",
        },
        "declined": None,
    })
    proposal = {
        "proposal_id": "prop_H001_AG3_001",
        "repair_intent": _intent().to_json(),
    }
    rca = {"gs_009": _evidence("gs_009"), "gs_017": _evidence("gs_017")}
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(envelope),
    ):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=proposal,
            cluster_id="H001", ag_id="AG3", iteration=2,
            cluster_semantic_theme="x",
            per_qid_evidence=rca,
            passing_qids_at_risk=("gs_044", "gs_055"),
        )
    assert verdict is not None
    assert "gs_hallucinated" not in verdict.likely_neighbor_regressions
    assert set(verdict.likely_neighbor_regressions) == {"gs_044", "gs_055"}
