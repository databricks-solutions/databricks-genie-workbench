"""Plan 5 Task 13 — RepairIntent stamped on every Plan-5-produced proposal.

The "proposal-slate materialization handoff fix" (gs_026 symptom):
when Plan 5 emits a proposal it MUST stamp Plan 1's RepairIntent via
stamp_repair_intent_on_proposal so downstream stages (IntentOutcome
carrier on AgOutcomeRecord, postmortem joining) can correlate the
intent to the proposal_dict in proposals_by_ag.

Validates the wire-in from Task 12.
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
    extract_repair_intent_from_proposal,
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


def _in_lane_envelope() -> str:
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


def _cross_lever_envelope() -> str:
    return json.dumps({
        "result": {
            "intent_name": "revenue_windowed_total",
            "intent_description": "x",
            "repair_shape": "sql_expression",
            "patch_type": "add_sql_snippet_expression",
            "rationale": "x", "confidence": "high",
            "patch_body": {
                "name": "revenue_by_region_total",
                "sql_expression": "SUM(revenue) OVER (PARTITION BY region)",
                "usage_guidance": "use for windowed",
            },
            "blame_set": ["sales.fact_sales.revenue"],
        },
        "declined": None,
    })


def test_in_lane_proposal_carries_repair_intent_stamp(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")
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
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_in_lane_envelope()),
    ):
        result = _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=rca, llm_cluster=_llm_cluster(),
            ag_id="AG3", iteration=2,
        )

    assert len(result) == 1
    proposal = result[0]
    assert "intent_id" in proposal
    assert proposal["intent_id"] == "intent_H001_AG3_001"
    intent = extract_repair_intent_from_proposal(proposal)
    assert intent is not None
    assert intent.intent_id == "intent_H001_AG3_001"
    assert intent.intent_name == "top_n_revenue_by_region"
    assert intent.patch_type is PatchType.ADD_EXAMPLE_SQL
    assert intent.source == "llm_l5b_synthesis"
    assert "cross_lever_override" not in proposal


def test_cross_lever_proposal_carries_override_event_and_stamp(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")
    cluster = {
        "cluster_id": "H001", "question_ids": ["gs_001", "gs_002"],
        "root_cause": "missing_window", "asi_failure_type": "missing_window",
        "asi_blame_set": ["sales.fact_sales.revenue"],
    }
    metadata = {
        "schema_columns": ["sales.fact_sales.revenue"],
        "instructions": {"example_question_sqls": []},
        "data_sources": {},
    }
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_cross_lever_envelope()),
    ):
        result = _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=rca, llm_cluster=_llm_cluster(),
            ag_id="AG3", iteration=2,
        )

    assert len(result) == 1
    proposal = result[0]
    assert "sql_expression" in proposal
    assert proposal["sql_expression"] == (
        "SUM(revenue) OVER (PARTITION BY region)"
    )
    intent = extract_repair_intent_from_proposal(proposal)
    assert intent is not None
    assert intent.patch_type is PatchType.ADD_SQL_SNIPPET_EXPRESSION
    assert "cross_lever_override" in proposal
    ovr = proposal["cross_lever_override"]
    assert ovr["from_lever"] == "lever-5b-example-sql"
    assert ovr["to_lever"] == "lever-6-sql-expression"
    assert ovr["intent_id"] == "intent_H001_AG3_001"


def test_intent_source_string_distinguishes_llm_vs_archetype_producer(
    monkeypatch,
) -> None:
    """Plan-5 LLM-produced intents carry source='llm_l5b_synthesis';
    Plan-1 intent_from_archetype intents carry source=
    'deterministic_archetype_adapter'. Postmortem groups by source."""
    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")
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
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_in_lane_envelope()),
    ):
        result = _dispatch_lever_5b_for_cluster(
            cluster=cluster, metadata_snapshot=metadata,
            w=None, benchmark_corpus=None, benchmarks=None,
            rca_evidence_typed=rca, llm_cluster=_llm_cluster(),
            ag_id="AG3", iteration=2,
        )
    intent = extract_repair_intent_from_proposal(result[0])
    assert intent is not None
    assert intent.source == "llm_l5b_synthesis"
