"""Plan 5 Task 16 — end-to-end integration test.

Threads every Plan-5 component through the L5b dispatch path using the
real skill folder, the real cross-lever router, the real validator
chain, and the real stamping helper. Only the OpenAI client is stubbed;
every other component runs unmocked.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.llm_token_budget import (
    IterationTokenBudget,
    _REASONING_TOKEN_BUDGET,
)
from genie_space_optimizer.optimization.optimizer import (
    _dispatch_lever_5b_for_cluster,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
    extract_repair_intent_from_proposal,
)


def _ev(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure=f"failure {qid}",
        generated_sql_issue="x", expected_sql_shape="x",
        blame_set=("sales.fact_sales.revenue", "sales.fact_sales.region"),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _llm_cluster() -> LlmCluster:
    return LlmCluster(
        cluster_id="H001",
        semantic_theme="top-N revenue ranking missing",
        member_qids=("gs_001", "gs_002"),
        unifying_evidence="both miss LIMIT/ORDER BY",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=("sales.fact_sales.revenue",),
        confidence="high",
    )


def _make_client(envelope_json: str) -> MagicMock:
    client = MagicMock()
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=2800, completion_tokens=700, total_tokens=3500,
    )
    client.chat.completions.create.return_value = completion
    return client


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
                "example_question": "What are the top 3 regions by revenue this quarter?",
                "example_sql": "SELECT region, SUM(revenue) r FROM sales.fact_sales WHERE order_date >= DATE_TRUNC('quarter', CURRENT_DATE) GROUP BY region ORDER BY r DESC LIMIT 3",
                "usage_guidance": "use when the user asks for top-N",
                "parameters": [],
            },
            "blame_set": ["sales.fact_sales.revenue", "sales.fact_sales.region"],
        },
        "declined": None,
    })


def _cross_lever_envelope() -> str:
    return json.dumps({
        "result": {
            "intent_name": "revenue_by_region_windowed_total",
            "intent_description": "add reusable SQL snippet expression",
            "repair_shape": "sql_expression",
            "patch_type": "add_sql_snippet_expression",
            "rationale": "both qids expect SUM(revenue) OVER (PARTITION BY region)",
            "confidence": "high",
            "patch_body": {
                "name": "revenue_by_region_total",
                "sql_expression": "SUM(revenue) OVER (PARTITION BY region)",
                "usage_guidance": "use when the user asks for windowed revenue",
            },
            "blame_set": ["sales.fact_sales.revenue", "sales.fact_sales.region"],
        },
        "declined": None,
    })


def test_end_to_end_in_lane_proposal_lands_with_intent_stamp(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")
    cluster = {
        "cluster_id": "H001", "question_ids": ["gs_001", "gs_002"],
        "root_cause": "missing_top_n",
        "asi_failure_type": "missing_top_n",
        "asi_blame_set": ["sales.fact_sales.revenue"],
    }
    metadata = {
        "schema_columns": [
            "sales.fact_sales.revenue", "sales.fact_sales.region",
            "sales.fact_sales", "sales.fact_sales.order_date",
        ],
        "instructions": {"example_question_sqls": [
            {"question": "What is total revenue?", "sql": "SELECT 1"},
        ]},
        "data_sources": {},
    }
    rca = {"gs_001": _ev("gs_001"), "gs_002": _ev("gs_002")}

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_make_client(_success_envelope()),
    ):
        result = _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=rca, llm_cluster=_llm_cluster(),
            ag_id="AG3", iteration=2,
        )

    assert len(result) == 1
    proposal = result[0]
    assert "example_question" in proposal and "example_sql" in proposal
    intent = extract_repair_intent_from_proposal(proposal)
    assert intent is not None
    assert intent.intent_id == "intent_H001_AG3_001"
    assert intent.source == "llm_l5b_synthesis"


def test_end_to_end_cross_lever_override_routes_to_l6_with_event(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")
    cluster = {
        "cluster_id": "H001", "question_ids": ["gs_001", "gs_002"],
        "root_cause": "missing_window",
        "asi_failure_type": "missing_window",
        "asi_blame_set": ["sales.fact_sales.revenue"],
    }
    metadata = {
        "schema_columns": [
            "sales.fact_sales.revenue", "sales.fact_sales.region",
        ],
        "instructions": {"example_question_sqls": []},
        "data_sources": {},
    }
    rca = {"gs_001": _ev("gs_001"), "gs_002": _ev("gs_002")}

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_make_client(_cross_lever_envelope()),
    ):
        result = _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=rca, llm_cluster=_llm_cluster(),
            ag_id="AG3", iteration=2,
        )

    assert len(result) == 1
    proposal = result[0]
    assert proposal["name"] == "revenue_by_region_total"
    assert proposal["sql_expression"] == (
        "SUM(revenue) OVER (PARTITION BY region)"
    )
    assert "cross_lever_override" in proposal
    assert proposal["cross_lever_override"]["to_lever"] == (
        "lever-6-sql-expression"
    )


def test_end_to_end_budget_meter_records_actuals(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")
    budget = IterationTokenBudget(itpm_limit=200_000, otpm_limit=20_000)
    token = _REASONING_TOKEN_BUDGET.set(budget)
    try:
        cluster = {
            "cluster_id": "H001", "question_ids": ["gs_001", "gs_002"],
            "root_cause": "missing_top_n",
            "asi_failure_type": "missing_top_n",
            "asi_blame_set": ["sales.fact_sales.revenue"],
        }
        metadata = {
            "schema_columns": ["sales.fact_sales.revenue"],
            "instructions": {"example_question_sqls": []},
            "data_sources": {},
        }
        rca = {"gs_001": _ev("gs_001"), "gs_002": _ev("gs_002")}
        with patch.object(
            optimizer, "_get_openai_client",
            return_value=_make_client(_success_envelope()),
        ):
            _dispatch_lever_5b_for_cluster(
                cluster=cluster, metadata_snapshot=metadata,
                w=None, benchmark_corpus=None, benchmarks=None,
                rca_evidence_typed=rca, llm_cluster=_llm_cluster(),
                ag_id="AG3", iteration=2,
            )
        assert budget.actual_input_tokens == 2800
        assert budget.actual_output_tokens == 700
    finally:
        _REASONING_TOKEN_BUDGET.reset(token)


def test_end_to_end_flag_off_skips_plan5_dispatch(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "false")
    cluster = {"cluster_id": "H001", "question_ids": ["gs_001"]}
    metadata = {"instructions": {"example_question_sqls": []}}
    rca = {"gs_001": _ev("gs_001"), "gs_002": _ev("gs_002")}

    client = MagicMock()
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=rca, llm_cluster=_llm_cluster(),
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
