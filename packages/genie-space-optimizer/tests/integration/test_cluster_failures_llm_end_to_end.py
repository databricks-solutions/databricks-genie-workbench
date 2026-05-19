"""Plan 4 Task 15 — end-to-end test of failure-clustering stage.

Threads every Plan-4 component together using the real skill folder.
Only the OpenAI client is stubbed; every other component runs
unmocked.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_token_budget import (
    IterationTokenBudget,
    _REASONING_TOKEN_BUDGET,
)
from genie_space_optimizer.optimization.optimizer import cluster_failures
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType


def _ev(qid: str, family: str, blame: tuple[str, ...]) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure=f"failure for {qid}",
        generated_sql_issue="defect", expected_sql_shape="shape",
        blame_set=blame, suggested_repair_family=family,
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def _make_client(envelope_json: str) -> MagicMock:
    client = MagicMock(name="OpenAIClient")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=1800, completion_tokens=500, total_tokens=2300,
    )
    client.chat.completions.create.return_value = completion
    return client


def test_end_to_end_two_clusters_returned_as_legacy_dicts(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "true")

    rca = {
        "gs_001": _ev("gs_001", "top_n_with_ordering", ("sales.fact_sales.revenue",)),
        "gs_002": _ev("gs_002", "top_n_with_ordering", ("sales.fact_sales.revenue",)),
        "gs_003": _ev("gs_003", "join_addition", ("crm.customer.customer_id",)),
    }
    envelope = json.dumps({
        "result": {
            "clusters": [
                {
                    "semantic_theme": "top-N collapse",
                    "member_qids": ["gs_001", "gs_002"],
                    "unifying_evidence": "both miss LIMIT/ORDER BY",
                    "suggested_repair_shape": "top_n_by_metric",
                    "primary_blame_set": ["sales.fact_sales.revenue"],
                    "confidence": "high",
                },
                {
                    "semantic_theme": "missing join spec",
                    "member_qids": ["gs_003"],
                    "unifying_evidence": "cartesian product",
                    "suggested_repair_shape": "join_discovery",
                    "primary_blame_set": ["crm.customer.customer_id"],
                    "confidence": "high",
                },
            ],
        },
        "declined": None,
    })

    metadata = {
        "schema_columns": [
            "sales.fact_sales.revenue", "crm.customer.customer_id",
        ],
        "iteration": 4,
    }

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_make_client(envelope),
    ):
        clusters = cluster_failures(
            {"eval_results": []}, metadata,
            rca_evidence_typed=rca, namespace="H",
        )

    assert isinstance(clusters, list)
    assert len(clusters) == 2
    assert [c["cluster_id"] for c in clusters] == ["H001", "H002"]
    assert clusters[0]["semantic_theme"] == "top-N collapse"
    assert clusters[0]["suggested_repair_shape"] == "top_n_by_metric"
    assert clusters[0]["source"] == "llm"
    assert clusters[0]["question_ids"] == ["gs_001", "gs_002"]


def test_end_to_end_budget_meter_records_actuals(monkeypatch) -> None:
    """One LLM call per iteration updates the per-iteration budget."""
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "true")

    budget = IterationTokenBudget(itpm_limit=200_000, otpm_limit=20_000)
    token = _REASONING_TOKEN_BUDGET.set(budget)
    try:
        rca = {
            "gs_001": _ev("gs_001", "top_n", ("sales.fact_sales.revenue",)),
            "gs_002": _ev("gs_002", "top_n", ("sales.fact_sales.revenue",)),
        }
        envelope = json.dumps({
            "result": {
                "clusters": [
                    {
                        "semantic_theme": "top-N",
                        "member_qids": ["gs_001", "gs_002"],
                        "unifying_evidence": "x",
                        "suggested_repair_shape": "top_n_by_metric",
                        "primary_blame_set": ["sales.fact_sales.revenue"],
                        "confidence": "high",
                    }
                ],
            },
            "declined": None,
        })
        with patch.object(
            optimizer, "_get_openai_client",
            return_value=_make_client(envelope),
        ):
            cluster_failures(
                {"eval_results": []},
                {
                    "schema_columns": ["sales.fact_sales.revenue"],
                    "iteration": 1,
                },
                rca_evidence_typed=rca, namespace="H",
            )
        assert budget.actual_input_tokens == 1800
        assert budget.actual_output_tokens == 500
    finally:
        _REASONING_TOKEN_BUDGET.reset(token)


def test_end_to_end_decline_falls_back_to_prior_clusters(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "true")
    rca = {
        "gs_001": _ev("gs_001", "x", ()),
        "gs_002": _ev("gs_002", "x", ()),
    }
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "ambiguous_failure",
            "explanation": "test",
            "needed_evidence": [],
            "suggested_next_step": "x",
        },
    })
    prior = [{"cluster_id": "H001", "source": "prior_iteration"}]

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_make_client(decline),
    ):
        result = cluster_failures(
            {"eval_results": []}, {"iteration": 5},
            rca_evidence_typed=rca,
            prior_clusters=prior,
            namespace="H",
        )

    assert result == prior


def test_end_to_end_flag_off_runs_heuristic_path_only(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "0")
    rca = {
        "gs_001": _ev("gs_001", "x", ()),
        "gs_002": _ev("gs_002", "x", ()),
    }
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = cluster_failures(
            {"eval_results": []}, {},
            rca_evidence_typed=rca,
            namespace="H",
        )
    assert client.chat.completions.create.call_count == 0
    assert isinstance(result, list)
