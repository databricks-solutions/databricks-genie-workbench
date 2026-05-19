"""Plan 5 Task 12 — _dispatch_lever_5b_for_cluster prepended LLM short-circuit.

When the flag is on AND rca_evidence_typed is non-empty AND llm_cluster
is non-None, _dispatch_lever_5b_for_cluster dispatches through the
Plan-5 synthesizer. The existing rich-path branch remains untouched
as the deterministic fallback.

New optional kwargs (back-compat with every existing caller):
  rca_evidence_typed: dict[str, PerQidRcaEvidence] | None
  llm_cluster:        LlmCluster | None
  ag_id:              str | None
  iteration:          int | None
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.optimizer import (
    _dispatch_lever_5b_for_cluster,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x", blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _llm_cluster() -> LlmCluster:
    return LlmCluster(
        cluster_id="H001", semantic_theme="x",
        member_qids=("gs_001", "gs_002"), unifying_evidence="x",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=("sales.fact_sales.revenue",),
        confidence="high",
    )


def _success_envelope() -> str:
    return json.dumps({
        "result": {
            "intent_name": "top_n_revenue_by_region",
            "intent_description": "x",
            "repair_shape": "top_n_by_metric",
            "patch_type": "add_example_sql",
            "rationale": "x", "confidence": "high",
            "patch_body": {
                "example_question": "What are top 3 regions?",
                "example_sql": "SELECT region, SUM(revenue) r FROM sales.fact_sales GROUP BY region ORDER BY r DESC LIMIT 3",
                "usage_guidance": "use for top-N",
                "parameters": [],
            },
            "blame_set": ["sales.fact_sales.revenue"],
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
        prompt_tokens=2500, completion_tokens=500, total_tokens=3000,
    )
    client.chat.completions.create.return_value = completion
    return client


def test_dispatch_signature_carries_four_new_kwargs() -> None:
    import inspect
    sig = inspect.signature(_dispatch_lever_5b_for_cluster)
    new_params = {"rca_evidence_typed", "llm_cluster", "ag_id", "iteration"}
    present = new_params & set(sig.parameters)
    assert present == new_params, (
        f"missing new kwargs: {new_params - present}"
    )
    for name in new_params:
        assert sig.parameters[name].default is None, (
            f"kwarg {name!r} must default to None (back-compat)"
        )


def test_plan5_short_circuit_runs_when_flag_on_and_typed_evidence_present(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")
    cluster = {
        "cluster_id": "H001",
        "question_ids": ["gs_001", "gs_002"],
        "root_cause": "missing_top_n",
        "asi_failure_type": "missing_top_n",
        "asi_blame_set": ["sales.fact_sales.revenue"],
    }
    metadata = {
        "schema_columns": ["sales.fact_sales.revenue", "sales.fact_sales.region"],
        "instructions": {"example_question_sqls": []},
        "data_sources": {},
    }
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_success_envelope()),
    ):
        result = _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=rca,
            llm_cluster=_llm_cluster(),
            ag_id="AG3", iteration=2,
        )

    assert isinstance(result, list)
    assert len(result) == 1
    assert "example_question" in result[0]
    assert "example_sql" in result[0]
    assert "What are top 3 regions" in result[0]["example_question"]


def test_plan5_short_circuit_skipped_when_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "false")
    cluster = {"cluster_id": "H001", "question_ids": ["gs_001"]}
    metadata = {"instructions": {"example_question_sqls": []}}
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}

    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=rca,
            llm_cluster=_llm_cluster(),
            ag_id="AG3", iteration=2,
        )

    plan5_calls = [
        c for c in client.chat.completions.create.call_args_list
        if any(
            "repair_intent_synthesis" in str(m.get("content", ""))
            for m in c.kwargs.get("messages", [])
        )
    ]
    assert plan5_calls == []


def test_plan5_short_circuit_skipped_when_rca_evidence_typed_is_none(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")
    cluster = {"cluster_id": "H001", "question_ids": ["gs_001"]}
    metadata = {"instructions": {"example_question_sqls": []}}

    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=None,
            llm_cluster=None,
            ag_id="AG3", iteration=2,
        )

    plan5_calls = [
        c for c in client.chat.completions.create.call_args_list
        if any(
            "repair_intent_synthesis" in str(m.get("content", ""))
            for m in c.kwargs.get("messages", [])
        )
    ]
    assert plan5_calls == []


def test_plan5_short_circuit_falls_back_to_existing_path_when_llm_declines(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "schema_does_not_support_shape",
            "explanation": "x", "needed_evidence": [],
            "suggested_next_step": "x",
        },
    })
    cluster = {
        "cluster_id": "H001", "question_ids": ["gs_001"],
        "root_cause": "missing_top_n", "asi_failure_type": "missing_top_n",
        "asi_blame_set": ["sales.fact_sales.revenue"],
    }
    metadata = {
        "instructions": {"example_question_sqls": []},
        "schema_columns": ["sales.fact_sales.revenue"],
        "data_sources": {},
    }
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline),
    ):
        result = _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=rca,
            llm_cluster=_llm_cluster(),
            ag_id="AG3", iteration=2,
        )

    assert isinstance(result, list)
