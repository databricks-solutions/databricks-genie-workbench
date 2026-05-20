"""Plan 9 Task 6.1 — L6 dispatcher must call the finalizer after
to_proposal_dict() succeeds, so the proposal dict the applier
sees is the nested sql_snippet shape with validation_passed
stamped."""
from __future__ import annotations

from genie_space_optimizer.optimization.lever6_intent_dispatch import (
    dispatch_lever_6_with_intent,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType, RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind, TargetObject,
)


class _StubCluster:
    cluster_id = "C1"
    target_qids = ("q1", "q2")
    rca_card_id = "rca-1"


def _make_proposal() -> RepairProposal:
    return RepairProposal(
        intent_id="abc12345",
        intent_name="Total revenue measure",
        intent_description="Add a measure.",
        repair_shape=RepairShape.SQL_EXPRESSION,
        patch_type=PatchType.ADD_SQL_SNIPPET_MEASURE,
        rationale="Cluster lacks aggregation.",
        confidence=0.8,
        patch_body={
            "name": "total_revenue",
            "sql_expression": "SUM(orders.revenue)",
            "usage_guidance": "Total revenue.",
        },
        blame_set=("main.sales.orders",),
        target_objects=(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier="main.sales.orders",
                columns=("revenue",),
            ),
        ),
        required_constructs=(),
    )


def _make_metadata_snapshot() -> dict:
    return {
        "schema_columns": ["revenue", "orders.revenue"],
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.orders",
                    "columns": [{"name": "revenue", "type": "double"}],
                }
            ],
        },
        "sql_snippets": {"measures": []},
    }


def test_l6_dispatch_finalizer_produces_nested_sql_snippet(monkeypatch):
    """When to_proposal_dict() succeeds AND finalizer succeeds,
    the dispatcher returns a proposal dict with sql_snippet nested
    and provenance plan9_materialization_source=plan9_direct."""
    proposal = _make_proposal()

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        lambda **_: proposal,
    )

    out = dispatch_lever_6_with_intent(
        cluster={
            "cluster_id": "C1",
            "root_cause": "missing_measure",
            "question_ids": ["q1", "q2"],
            "question_traces": [{"qid": "q1"}],
        },
        llm_cluster=_StubCluster(),
        rca_evidence_typed={"q1": object()},
        ag_id="ag1",
        iteration=1,
        metadata_snapshot=_make_metadata_snapshot(),
        strategist_hints=None,
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
        benchmarks=None, raw_evidence=(),
    )

    assert out is not None
    assert "sql_snippet" in out
    assert out["sql_snippet"]["name"] == "total_revenue"
    assert out["sql_snippet"]["id"]
    assert "validation_passed" in out  # stamped (False here — no backend)
    assert out["provenance"]["plan9_materialization_source"] == "plan9_direct"


def test_l6_dispatch_finalizer_rejects_returns_legacy_fallback(monkeypatch):
    """When to_proposal_dict() succeeds BUT finalizer rejects (e.g.,
    invalid identifier), dispatcher must fall through to the
    safety-net legacy generator instead of returning the broken dict."""
    proposal = _make_proposal()
    proposal.patch_body["sql_expression"] = "SELECT revenue FROM nonexistent_table"

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        lambda **_: proposal,
    )

    legacy_called = {"n": 0}

    def _fake_legacy(**_):
        legacy_called["n"] += 1
        return {
            "patch_type": "add_sql_snippet_measure",
            "lever": 6,
            "sql_snippet": {"id": "legacy-1", "name": "legacy"},
            "validation_passed": True,
        }

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "_generate_lever6_proposal_legacy",
        _fake_legacy,
    )

    out = dispatch_lever_6_with_intent(
        cluster={
            "cluster_id": "C1",
            "root_cause": "missing_measure",
            "question_ids": ["q1"],
            "question_traces": [{"qid": "q1"}],
        },
        llm_cluster=_StubCluster(),
        rca_evidence_typed={"q1": object()},
        ag_id="ag1",
        iteration=1,
        metadata_snapshot=_make_metadata_snapshot(),
        strategist_hints=None,
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
        benchmarks=None, raw_evidence=(),
    )

    assert out is not None
    assert legacy_called["n"] == 1
    assert out["sql_snippet"]["id"] == "legacy-1"
    assert out["provenance"]["plan9_materialization_source"] == "plan9_legacy_fallback"
