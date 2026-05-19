"""Plan 5 Task 7 — synthesize_repair_intent_for_cluster (success path).

Builds an LlmReasoningRequest from cluster + PerQidRcaEvidence batch +
identifier_allowlist + Plan-4 semantic_theme, dispatches through Plan
2's LlmReasoningCall, validates, stamps deterministic intent_id, and
returns RepairProposal | None.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_intent_synthesizer import (
    _build_request,
    synthesize_repair_intent_for_cluster,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=2500, completion_tokens=600, total_tokens=3100,
    )
    client.chat.completions.create.return_value = completion
    return client


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure=f"missing LIMIT for {qid}",
        generated_sql_issue="no top-n", expected_sql_shape="LIMIT 3",
        blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _llm_cluster() -> LlmCluster:
    return LlmCluster(
        cluster_id="H001", semantic_theme="top-N revenue ranking missing",
        member_qids=("gs_001", "gs_002"),
        unifying_evidence="both miss LIMIT/ORDER BY",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=("sales.fact_sales.revenue",),
        confidence="high",
    )


def _success_envelope() -> str:
    return json.dumps({
        "result": {
            "intent_name": "top_n_revenue_by_region",
            "intent_description": "add example_sql for top-N revenue by region",
            "repair_shape": "top_n_by_metric",
            "patch_type": "add_example_sql",
            "rationale": "both qids miss LIMIT/ORDER BY",
            "confidence": "high",
            "patch_body": {
                "example_question": "What are the top 3 regions by revenue?",
                "example_sql": "SELECT region, SUM(revenue) r FROM sales.fact_sales GROUP BY region ORDER BY r DESC LIMIT 3",
                "usage_guidance": "use when the user asks for top-N",
                "parameters": [],
            },
            "blame_set": ["sales.fact_sales.revenue", "sales.fact_sales.region"],
        },
        "declined": None,
    })


def test_driver_returns_stamped_typed_proposal_on_success() -> None:
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_success_envelope()),
    ):
        rp = synthesize_repair_intent_for_cluster(
            w=None,
            cluster=_llm_cluster(),
            rca_evidence_typed=rca,
            identifier_allowlist={
                "sales.fact_sales.revenue", "sales.fact_sales.region",
                "sales.fact_sales",
            },
            ag_id="AG3", iteration=2, seq=1,
            existing_examples_preview="(none)",
            benchmarks=None,
        )
    assert rp is not None
    assert isinstance(rp, RepairProposal)
    assert rp.intent_id == "intent_H001_AG3_001"
    assert rp.repair_shape is RepairShape.TOP_N_BY_METRIC
    assert rp.patch_type is PatchType.ADD_EXAMPLE_SQL
    assert rp.blame_set == (
        "sales.fact_sales.revenue", "sales.fact_sales.region",
    )


def test_driver_call_id_is_cluster_iteration_scoped() -> None:
    """The call_id format is "repair_intent_synthesis.iter_{N}.{cluster_id}".
    Joinable in postmortems without prompt-SHA inference."""
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}
    request = _build_request(
        cluster=_llm_cluster(),
        rca_evidence_typed=rca,
        identifier_allowlist={"sales.fact_sales.revenue"},
        ag_id="AG3", iteration=5,
        existing_examples_preview="",
    )
    assert request.call_id == "repair_intent_synthesis.iter_5.H001"
    assert request.skill_id == "repair-intent-synthesis"
    assert request.max_tokens == 1200


def test_driver_rendered_prompt_includes_all_required_context() -> None:
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}
    captured: list[dict] = []
    client = MagicMock()

    def _spy(**kwargs):
        captured.append(kwargs)
        choice = MagicMock()
        choice.message.content = _success_envelope()
        completion = MagicMock()
        completion.choices = [choice]
        completion.usage = MagicMock(
            prompt_tokens=2000, completion_tokens=400, total_tokens=2400,
        )
        return completion

    client.chat.completions.create.side_effect = _spy
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        synthesize_repair_intent_for_cluster(
            w=None, cluster=_llm_cluster(), rca_evidence_typed=rca,
            identifier_allowlist={"sales.fact_sales.revenue"},
            ag_id="AG3", iteration=2, seq=1,
            existing_examples_preview="(1) 'Total revenue?'",
            benchmarks=None,
        )

    assert len(captured) == 1
    user_msg = next(
        m["content"] for m in captured[0]["messages"] if m["role"] == "user"
    )
    assert '"cluster_id": "H001"' in user_msg
    assert '"ag_id": "AG3"' in user_msg
    assert '"iteration": 2' in user_msg
    assert "top-N revenue ranking missing" in user_msg
    assert "top_n_by_metric" in user_msg
    assert "gs_001" in user_msg
    assert "gs_002" in user_msg
    assert "sales.fact_sales.revenue" in user_msg
    assert "Total revenue?" in user_msg


def test_driver_rejects_when_blame_set_outside_allowlist_and_returns_none() -> None:
    """Validator rejects blame_set outside allowlist → driver returns
    None → caller falls back to deterministic intent_from_archetype."""
    envelope = json.dumps({
        "result": {
            "intent_name": "x", "intent_description": "x",
            "repair_shape": "top_n_by_metric",
            "patch_type": "add_example_sql",
            "rationale": "x", "confidence": "high",
            "patch_body": {
                "example_question": "q", "example_sql": "SELECT 1",
            },
            "blame_set": ["bogus.schema.does_not_exist"],
        },
        "declined": None,
    })
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(envelope),
    ):
        rp = synthesize_repair_intent_for_cluster(
            w=None, cluster=_llm_cluster(), rca_evidence_typed=rca,
            identifier_allowlist={"sales.fact_sales.revenue"},
            ag_id="AG3", iteration=1, seq=1,
            existing_examples_preview="", benchmarks=None,
        )
    assert rp is None


def test_driver_rejects_when_patch_body_missing_required_field() -> None:
    envelope = json.dumps({
        "result": {
            "intent_name": "x", "intent_description": "x",
            "repair_shape": "top_n_by_metric",
            "patch_type": "add_example_sql",
            "rationale": "x", "confidence": "high",
            "patch_body": {"example_question": "q only"},
            "blame_set": ["sales.fact_sales.revenue"],
        },
        "declined": None,
    })
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(envelope),
    ):
        rp = synthesize_repair_intent_for_cluster(
            w=None, cluster=_llm_cluster(), rca_evidence_typed=rca,
            identifier_allowlist={"sales.fact_sales.revenue"},
            ag_id="AG3", iteration=1, seq=1,
            existing_examples_preview="", benchmarks=None,
        )
    assert rp is None


def test_driver_runs_relaxed_leakage_gate_when_repair_shape_is_other() -> None:
    envelope = json.dumps({
        "result": {
            "intent_name": "x", "intent_description": "x",
            "repair_shape": "other",
            "patch_type": "add_example_sql",
            "rationale": "x", "confidence": "low",
            "patch_body": {
                "example_question": "How many distinct customers visited last week?",
                "example_sql": "SELECT 1",
            },
            "blame_set": ["sales.fact_sales.revenue"],
        },
        "declined": None,
    })
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}
    benchmarks = [
        {"question": "How many distinct customers visited last week?"},
    ]
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(envelope),
    ):
        rp = synthesize_repair_intent_for_cluster(
            w=None, cluster=_llm_cluster(), rca_evidence_typed=rca,
            identifier_allowlist={"sales.fact_sales.revenue"},
            ag_id="AG3", iteration=1, seq=1,
            existing_examples_preview="", benchmarks=benchmarks,
        )
    assert rp is None
