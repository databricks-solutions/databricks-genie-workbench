"""Plan 4 Task 13 — cluster_failures prepended LLM short-circuit branch."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.optimizer import cluster_failures
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="medium", quoted_evidence=(),
    )


def _success_envelope() -> str:
    return json.dumps({
        "result": {
            "clusters": [
                {
                    "semantic_theme": "top-N collapse",
                    "member_qids": ["gs_001", "gs_002"],
                    "unifying_evidence": "both miss LIMIT/ORDER BY",
                    "suggested_repair_shape": "top_n_by_metric",
                    "primary_blame_set": ["sales.fact_sales.revenue"],
                    "confidence": "high",
                }
            ],
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
        prompt_tokens=1500, completion_tokens=300, total_tokens=1800,
    )
    client.chat.completions.create.return_value = completion
    return client


def test_cluster_failures_signature_carries_three_new_kwargs() -> None:
    import inspect
    sig = inspect.signature(cluster_failures)
    new_params = {"rca_evidence_typed", "prior_clusters", "w"}
    present = new_params & set(sig.parameters)
    assert present == new_params, (
        f"missing new kwargs: {new_params - present}"
    )
    for name in new_params:
        assert sig.parameters[name].default is None, (
            f"kwarg {name!r} must default to None (back-compat)"
        )


def test_dispatch_uses_llm_when_flag_on_and_typed_evidence_present(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "true")
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}

    eval_results = {"eval_results": []}
    metadata_snapshot = {"schema_columns": ["sales.fact_sales.revenue"]}

    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_success_envelope()),
    ):
        result = cluster_failures(
            eval_results, metadata_snapshot,
            rca_evidence_typed=rca,
            namespace="H",
        )

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["cluster_id"] == "H001"
    assert result[0]["semantic_theme"] == "top-N collapse"
    assert result[0]["suggested_repair_shape"] == "top_n_by_metric"
    assert result[0]["source"] == "llm"


def test_dispatch_skips_llm_when_flag_off(monkeypatch) -> None:
    # Plan 11 (default ON) would intercept this dispatch via the new
    # Stage 1/2 wiring; this test probes the legacy Plan 4 dispatch
    # specifically, so disable Plan 11 first.
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "0")
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "0")
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}
    eval_results = {"eval_results": []}
    metadata_snapshot = {}

    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = cluster_failures(
            eval_results, metadata_snapshot,
            rca_evidence_typed=rca, namespace="H",
        )

    assert client.chat.completions.create.call_count == 0
    assert isinstance(result, list)


def test_dispatch_falls_back_to_prior_clusters_when_llm_declines(
    monkeypatch,
) -> None:
    """Per roadmap: 'emit the prior iteration's clusters unchanged.'"""
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "true")
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "ambiguous_failure",
            "explanation": "x",
            "needed_evidence": [],
            "suggested_next_step": "x",
        },
    })
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}
    prior = [{
        "cluster_id": "H001",
        "question_ids": ["gs_001", "gs_002"],
        "root_cause": "from_prior_iteration",
        "source": "prior",
    }]

    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(decline),
    ):
        result = cluster_failures(
            {"eval_results": []}, {},
            rca_evidence_typed=rca,
            prior_clusters=prior,
            namespace="H",
        )

    assert result == prior


def test_dispatch_falls_through_to_heuristic_when_llm_declines_and_no_prior(
    monkeypatch,
) -> None:
    """LLM declines + no prior_clusters → heuristic body runs."""
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "true")
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "ambiguous_failure",
            "explanation": "x",
            "needed_evidence": [],
            "suggested_next_step": "x",
        },
    })
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}

    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(decline),
    ):
        result = cluster_failures(
            {"eval_results": []}, {},
            rca_evidence_typed=rca,
            prior_clusters=None,
            namespace="H",
        )

    assert isinstance(result, list)


def test_dispatch_skips_llm_when_rca_evidence_typed_is_empty(
    monkeypatch,
) -> None:
    """Empty typed-evidence dict → heuristic path (no LLM call)."""
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "true")
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = cluster_failures(
            {"eval_results": []}, {},
            rca_evidence_typed={},
            namespace="H",
        )
    assert client.chat.completions.create.call_count == 0
    assert isinstance(result, list)


def test_dispatch_skips_llm_when_only_one_qid_in_typed_evidence(
    monkeypatch,
) -> None:
    """<2 qids → cluster_failures_llm returns None internally."""
    # Plan 11 (default ON) clusters single-qid sets and would steal
    # this test from Plan 4. Disable Plan 11 so the test probes the
    # legacy Plan 4 size-gate it was written for.
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "0")
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "true")
    rca = {"gs_001": _evidence("gs_001")}
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        result = cluster_failures(
            {"eval_results": []}, {},
            rca_evidence_typed=rca, namespace="H",
        )
    assert client.chat.completions.create.call_count == 0
    assert isinstance(result, list)
