"""Plan 4 Task 10 — cluster_failures_llm abstain + error + collision fallback."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.cluster_llm import (
    cluster_failures_llm,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock()
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=1000, completion_tokens=200, total_tokens=1200,
    )
    client.chat.completions.create.return_value = completion
    return client


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="medium", quoted_evidence=(),
    )


def _two_qids() -> dict[str, PerQidRcaEvidence]:
    return {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}


def test_driver_returns_none_when_llm_declines() -> None:
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "insufficient_signal",
            "explanation": "every qid has empty observed_failure",
            "needed_evidence": ["observed_failure", "blame_set"],
            "suggested_next_step": "re_run_after_rca_extraction",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(decline),
    ):
        result = cluster_failures_llm(
            w=None, rca_evidence_typed=_two_qids(),
            schema_columns={"sales.fact_sales.revenue"},
            iteration=1, namespace="H",
        )
    assert result is None


def test_driver_returns_none_when_envelope_parse_fails() -> None:
    malformed = '{"not": "envelope shape"}'
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(malformed),
    ):
        result = cluster_failures_llm(
            w=None, rca_evidence_typed=_two_qids(),
            schema_columns={"sales.fact_sales.revenue"},
            iteration=1, namespace="H",
        )
    assert result is None


def test_driver_returns_none_when_http_call_fails() -> None:
    def _boom(*args, **kwargs):
        raise RuntimeError("Serving endpoint returned 429 after 3 retries")
    with patch.object(optimizer, "_traced_llm_call", side_effect=_boom):
        result = cluster_failures_llm(
            w=None, rca_evidence_typed=_two_qids(),
            schema_columns={"sales.fact_sales.revenue"},
            iteration=1, namespace="H",
        )
    assert result is None


def test_driver_returns_none_when_qid_collision_across_clusters() -> None:
    """Same qid in two clusters → entire LLM output discarded."""
    collision = json.dumps({
        "result": {
            "clusters": [
                {
                    "semantic_theme": "cluster A",
                    "member_qids": ["gs_001", "gs_002"],
                    "unifying_evidence": "x",
                    "suggested_repair_shape": "top_n_by_metric",
                    "primary_blame_set": ["sales.fact_sales.revenue"],
                    "confidence": "high",
                },
                {
                    "semantic_theme": "cluster B",
                    "member_qids": ["gs_002"],
                    "unifying_evidence": "x",
                    "suggested_repair_shape": "join_discovery",
                    "primary_blame_set": ["sales.fact_sales.revenue"],
                    "confidence": "medium",
                },
            ],
        },
        "declined": None,
    })
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(collision),
    ):
        result = cluster_failures_llm(
            w=None, rca_evidence_typed=_two_qids(),
            schema_columns={"sales.fact_sales.revenue"},
            iteration=1, namespace="H",
        )
    assert result is None


def test_driver_drops_clusters_with_unknown_member_qids_but_keeps_valid_ones() -> None:
    rca = {
        "gs_001": _evidence("gs_001"),
        "gs_002": _evidence("gs_002"),
        "gs_003": _evidence("gs_003"),
    }
    payload = json.dumps({
        "result": {
            "clusters": [
                {
                    "semantic_theme": "valid cluster",
                    "member_qids": ["gs_001", "gs_002"],
                    "unifying_evidence": "x",
                    "suggested_repair_shape": "top_n_by_metric",
                    "primary_blame_set": ["sales.fact_sales.revenue"],
                    "confidence": "high",
                },
                {
                    "semantic_theme": "invalid — references gs_999",
                    "member_qids": ["gs_003", "gs_999"],
                    "unifying_evidence": "x",
                    "suggested_repair_shape": "join_discovery",
                    "primary_blame_set": ["sales.fact_sales.revenue"],
                    "confidence": "high",
                },
            ],
        },
        "declined": None,
    })
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(payload),
    ):
        result = cluster_failures_llm(
            w=None, rca_evidence_typed=rca,
            schema_columns={"sales.fact_sales.revenue"},
            iteration=1, namespace="H",
        )
    assert result is not None
    assert len(result) == 1
    assert result[0].cluster_id == "H001"
    assert result[0].member_qids == ("gs_001", "gs_002")


def test_driver_drops_clusters_with_off_schema_blame_set_but_keeps_valid_ones() -> None:
    payload = json.dumps({
        "result": {
            "clusters": [
                {
                    "semantic_theme": "valid",
                    "member_qids": ["gs_001"],
                    "unifying_evidence": "x",
                    "suggested_repair_shape": "top_n_by_metric",
                    "primary_blame_set": ["sales.fact_sales.revenue"],
                    "confidence": "high",
                },
                {
                    "semantic_theme": "hallucinated_column",
                    "member_qids": ["gs_002"],
                    "unifying_evidence": "x",
                    "suggested_repair_shape": "join_discovery",
                    "primary_blame_set": ["bogus.schema.does_not_exist"],
                    "confidence": "high",
                },
            ],
        },
        "declined": None,
    })
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(payload),
    ):
        result = cluster_failures_llm(
            w=None, rca_evidence_typed=_two_qids(),
            schema_columns={"sales.fact_sales.revenue"},
            iteration=1, namespace="H",
        )
    assert result is not None
    assert len(result) == 1
    assert result[0].cluster_id == "H001"
    assert result[0].semantic_theme == "valid"
