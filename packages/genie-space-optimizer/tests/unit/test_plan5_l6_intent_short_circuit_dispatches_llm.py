"""Plan 8 Task 3 — when rca_evidence_typed + llm_cluster are present,
_generate_lever6_proposal delegates to lever6_intent_dispatch which
calls synthesize_repair_intent_for_cluster and stamps the typed
RepairIntent on the proposal."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)


def _evidence() -> dict:
    return {"q1": PerQidRcaEvidence(
        qid="q1", observed_failure="wrong_aggregation",
        generated_sql_issue="used count(*) instead of count distinct",
        expected_sql_shape="select count(distinct customer_id)",
        blame_set=("catalog.schema.fact_sales.customer_id",),
        suggested_repair_family="sql_snippet",
        repair_hint_patch_type=PatchType.ADD_SQL_SNIPPET_MEASURE,
        confidence="high",
        quoted_evidence=("expected distinct count",),
    )}


def _llm_cluster() -> LlmCluster:
    return LlmCluster(
        cluster_id="H001",
        member_qids=("q1",),
        semantic_theme="customer count uses raw count",
        suggested_repair_shape=RepairShape.SQL_EXPRESSION,
        primary_blame_set=("catalog.schema.fact_sales.customer_id",),
        unifying_evidence="needs a distinct-count measure snippet",
        confidence="high",
    )


def test_l6_intent_dispatch_returns_stamped_proposal():
    from genie_space_optimizer.optimization import lever6_intent_dispatch
    cluster = {"cluster_id": "H001", "question_ids": ["q1"],
                "root_cause": "wrong_aggregation",
                "asi_blame_set": ["catalog.schema.fact_sales.customer_id"]}
    metadata = {"schema_columns": ["catalog.schema.fact_sales.customer_id"],
                 "iteration": 1}

    fake_proposal = MagicMock()
    fake_proposal.patch_type = PatchType.ADD_SQL_SNIPPET_MEASURE
    fake_proposal.to_repair_intent = MagicMock()
    with patch(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        return_value=fake_proposal,
    ), patch(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "_generate_lever6_proposal_legacy",
        return_value={"snippet_type": "measure", "sql": "count(distinct x)",
                      "target": "fact_sales"},
    ), patch(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "stamp_repair_intent_on_proposal",
    ) as mock_stamp:
        out = lever6_intent_dispatch.dispatch_lever_6_with_intent(
            cluster=cluster, metadata_snapshot=metadata, w=None,
            rca_evidence_typed=_evidence(), llm_cluster=_llm_cluster(),
            ag_id="AG_X", iteration=1, spark=None, catalog="", gold_schema="",
            warehouse_id="", benchmarks=None, raw_evidence=(),
        )

    assert out is not None
    assert mock_stamp.called, "RepairIntent must be stamped on the returned proposal"


def test_l6_intent_dispatch_returns_none_when_synthesizer_declines():
    """When Plan 5 declines, dispatch returns None (caller falls back)."""
    from genie_space_optimizer.optimization import lever6_intent_dispatch
    with patch(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        return_value=None,
    ):
        out = lever6_intent_dispatch.dispatch_lever_6_with_intent(
            cluster={"cluster_id": "H001"}, metadata_snapshot={}, w=None,
            rca_evidence_typed=_evidence(), llm_cluster=_llm_cluster(),
            ag_id="AG_X", iteration=1,
        )
    assert out is None


def test_l6_intent_dispatch_returns_none_when_patch_type_not_l6():
    """When LLM picks a non-L6 patch_type (e.g. add_example_sql),
    dispatch returns None (cross-lever router will handle it via L5b)."""
    from genie_space_optimizer.optimization import lever6_intent_dispatch
    fake_proposal = MagicMock()
    fake_proposal.patch_type = PatchType.ADD_EXAMPLE_SQL
    with patch(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        return_value=fake_proposal,
    ):
        out = lever6_intent_dispatch.dispatch_lever_6_with_intent(
            cluster={"cluster_id": "H001"}, metadata_snapshot={}, w=None,
            rca_evidence_typed=_evidence(), llm_cluster=_llm_cluster(),
            ag_id="AG_X", iteration=1,
        )
    assert out is None
