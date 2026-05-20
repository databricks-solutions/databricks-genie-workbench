"""Plan 9 Task 6.1 — sql_snippet_finalizer tests.

The finalizer must:
  1. Wrap the flat to_proposal_dict() output into the nested
     sql_snippet shape the applier reads at applier.py:3162.
  2. Run _validate_sql_identifiers against the metadata allowlist;
     return None on failure (caller treats as decline).
  3. When (w, warehouse_id) are provided, run validate_sql_snippet
     and stamp validation_passed accordingly; otherwise stamp
     validation_passed=False (the applier-gate will drop it, which
     is the correct safe default for no-backend dev paths).
  4. Fabricate missing applier fields from the RepairProposal:
     snippet_type from patch_type, display_name from intent_name,
     target_table from first TABLE in target_objects, rationale
     from RepairProposal.rationale, etc.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.repair_intent import (
    PatchType, RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.sql_snippet_finalizer import (
    _first_table_identifier,
    finalize_sql_snippet_proposal_dict,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind, TargetObject,
)


def _make_proposal(patch_type: PatchType = PatchType.ADD_SQL_SNIPPET_MEASURE) -> RepairProposal:
    return RepairProposal(
        intent_id="abc12345",
        intent_name="Total revenue measure",
        intent_description="Add a measure for total revenue.",
        repair_shape=RepairShape.SQL_EXPRESSION,
        patch_type=patch_type,
        rationale="Cluster lacks aggregation primitive.",
        confidence=0.8,
        patch_body={
            "name": "total_revenue",
            "sql_expression": "SUM(orders.revenue)",
            "usage_guidance": "Use to compute total revenue across orders.",
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
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.orders",
                    "columns": [
                        {"name": "revenue", "type": "double"},
                        {"name": "order_id", "type": "string"},
                    ],
                }
            ],
        },
        "sql_snippets": {"measures": []},
    }


def _make_cluster() -> dict:
    return {
        "cluster_id": "C1",
        "root_cause": "missing_measure",
        "question_ids": ["q1", "q2"],
        "question_traces": [{"qid": "q1"}, {"qid": "q2"}],
    }


def test_finalizer_returns_nested_sql_snippet_shape():
    proposal = _make_proposal()
    base_dict = proposal.to_proposal_dict()
    out = finalize_sql_snippet_proposal_dict(
        proposal,
        base_dict,
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales",
        warehouse_id="",
    )
    assert out is not None
    # Applier reads patch["sql_snippet"]; nested shape is required.
    assert "sql_snippet" in out
    snippet = out["sql_snippet"]
    assert snippet["name"] == "total_revenue"
    assert snippet["sql"] == "SUM(orders.revenue)"
    assert snippet["id"]  # non-empty


def test_finalizer_stamps_snippet_type_from_patch_type():
    out = finalize_sql_snippet_proposal_dict(
        _make_proposal(PatchType.ADD_SQL_SNIPPET_MEASURE),
        _make_proposal(PatchType.ADD_SQL_SNIPPET_MEASURE).to_proposal_dict(),
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
    )
    assert out["snippet_type"] == "measure"

    out_f = finalize_sql_snippet_proposal_dict(
        _make_proposal(PatchType.ADD_SQL_SNIPPET_FILTER),
        _make_proposal(PatchType.ADD_SQL_SNIPPET_FILTER).to_proposal_dict(),
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
    )
    assert out_f["snippet_type"] == "filter"


def test_finalizer_validation_passed_false_when_no_backend():
    """No w/warehouse_id and no spark → cannot EXPLAIN/execute → must
    stamp validation_passed=False so the applier-gate drops the
    patch (safe default, matches legacy body line 14396)."""
    out = finalize_sql_snippet_proposal_dict(
        _make_proposal(),
        _make_proposal().to_proposal_dict(),
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
    )
    assert out["validation_passed"] is False


def test_finalizer_returns_none_on_invalid_identifier():
    """The LLM emitted an identifier not in the allowlist — the
    finalizer must reject (return None) so the dispatcher falls
    through to the safety-net legacy generator."""
    bad = _make_proposal()
    bad.patch_body["sql_expression"] = "SELECT revenue FROM nonexistent_table"
    out = finalize_sql_snippet_proposal_dict(
        bad,
        bad.to_proposal_dict(),
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
    )
    assert out is None


def test_first_table_identifier_prefers_table_target_object():
    proposal = _make_proposal()
    assert _first_table_identifier(proposal) == "main.sales.orders"


def test_first_table_identifier_falls_back_to_blame_set():
    proposal = _make_proposal()
    proposal = RepairProposal(
        intent_id=proposal.intent_id,
        intent_name=proposal.intent_name,
        intent_description=proposal.intent_description,
        repair_shape=proposal.repair_shape,
        patch_type=proposal.patch_type,
        rationale=proposal.rationale,
        confidence=proposal.confidence,
        patch_body=dict(proposal.patch_body),
        blame_set=("main.sales.orders",),
        target_objects=(),
        required_constructs=(),
    )
    assert _first_table_identifier(proposal) == "main.sales.orders"


def test_first_table_identifier_empty_when_no_table_or_blame():
    proposal = _make_proposal()
    proposal = RepairProposal(
        intent_id=proposal.intent_id,
        intent_name=proposal.intent_name,
        intent_description=proposal.intent_description,
        repair_shape=proposal.repair_shape,
        patch_type=proposal.patch_type,
        rationale=proposal.rationale,
        confidence=proposal.confidence,
        patch_body=dict(proposal.patch_body),
        blame_set=(),
        target_objects=(),
        required_constructs=(),
    )
    assert _first_table_identifier(proposal) == ""
