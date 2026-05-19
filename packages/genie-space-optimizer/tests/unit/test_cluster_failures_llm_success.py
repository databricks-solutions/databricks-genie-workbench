"""Plan 4 Task 9 — cluster_failures_llm (success path)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.cluster_llm import (
    _build_request,
    cluster_failures_llm,
)
from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=2000, completion_tokens=400, total_tokens=2400,
    )
    client.chat.completions.create.return_value = completion
    return client


def _evidence(
    qid: str, family: str = "top_n_with_ordering",
) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid,
        observed_failure=f"failure for {qid}",
        generated_sql_issue="defect",
        expected_sql_shape="shape",
        blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family=family,
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=(),
    )


def _success_envelope_two_clusters() -> str:
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


def test_driver_returns_stamped_typed_clusters_on_success() -> None:
    rca = {
        "gs_001": _evidence("gs_001"),
        "gs_002": _evidence("gs_002"),
        "gs_003": _evidence("gs_003", family="join_addition"),
    }
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(_success_envelope_two_clusters()),
    ):
        clusters = cluster_failures_llm(
            w=None,
            rca_evidence_typed=rca,
            schema_columns={
                "sales.fact_sales.revenue", "crm.customer.customer_id",
            },
            iteration=3,
            namespace="H",
        )
    assert clusters is not None
    assert len(clusters) == 2
    assert [c.cluster_id for c in clusters] == ["H001", "H002"]
    assert isinstance(clusters[0], LlmCluster)
    assert clusters[0].suggested_repair_shape is RepairShape.TOP_N_BY_METRIC
    assert clusters[1].suggested_repair_shape is RepairShape.JOIN_DISCOVERY
    assert clusters[0].member_qids == ("gs_001", "gs_002")


def test_driver_uses_correct_namespace_in_stamped_id() -> None:
    rca = {f"gs_{i:03d}": _evidence(f"gs_{i:03d}") for i in range(2)}
    envelope = json.dumps({
        "result": {
            "clusters": [
                {
                    "semantic_theme": "soft signal cluster",
                    "member_qids": list(rca.keys()),
                    "unifying_evidence": "x",
                    "suggested_repair_shape": "top_n_by_metric",
                    "primary_blame_set": ["sales.fact_sales.revenue"],
                    "confidence": "medium",
                }
            ],
        },
        "declined": None,
    })
    with patch.object(
        optimizer, "_get_openai_client", return_value=_stub_with(envelope),
    ):
        clusters = cluster_failures_llm(
            w=None, rca_evidence_typed=rca,
            schema_columns={"sales.fact_sales.revenue"},
            iteration=1, namespace="S",
        )
    assert clusters is not None
    assert clusters[0].cluster_id == "S001"


def test_driver_call_id_is_iteration_namespace_scoped() -> None:
    """The call_id format is 'failure_clustering.iter_{N}.{namespace}'."""
    rca = {"gs_001": _evidence("gs_001"), "gs_002": _evidence("gs_002")}
    request = _build_request(
        rca_evidence_typed=rca,
        schema_columns={"sales.fact_sales.revenue"},
        iteration=5,
        namespace="H",
    )
    assert request.call_id == "failure_clustering.iter_5.H"
    assert request.skill_id == "failure-clustering"
    assert request.max_tokens == 2000


def test_driver_returns_none_when_fewer_than_two_qids() -> None:
    """Deterministic early-abort: <2 qids → no LLM call → None."""
    rca = {"gs_001": _evidence("gs_001")}
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        clusters = cluster_failures_llm(
            w=None, rca_evidence_typed=rca,
            schema_columns={"sales.fact_sales.revenue"},
            iteration=1, namespace="H",
        )
    assert clusters is None
    assert client.chat.completions.create.call_count == 0


def test_driver_rendered_prompt_includes_all_per_qid_evidence_and_schema() -> None:
    rca = {
        "gs_001": _evidence("gs_001"),
        "gs_002": _evidence("gs_002"),
    }
    captured: list[dict] = []
    client = MagicMock()

    def _spy(**kwargs):
        captured.append(kwargs)
        choice = MagicMock()
        choice.message.content = _success_envelope_two_clusters()
        completion = MagicMock()
        completion.choices = [choice]
        completion.usage = MagicMock(
            prompt_tokens=1000, completion_tokens=300, total_tokens=1300,
        )
        return completion

    client.chat.completions.create.side_effect = _spy
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        cluster_failures_llm(
            w=None, rca_evidence_typed=rca,
            schema_columns={"sales.fact_sales.revenue"},
            iteration=2, namespace="H",
        )

    assert len(captured) == 1
    user_msg = next(
        m["content"] for m in captured[0]["messages"] if m["role"] == "user"
    )
    assert "gs_001" in user_msg
    assert "gs_002" in user_msg
    assert "sales.fact_sales.revenue" in user_msg
    assert '"iteration": 2' in user_msg
    assert '"namespace": "hard"' in user_msg
