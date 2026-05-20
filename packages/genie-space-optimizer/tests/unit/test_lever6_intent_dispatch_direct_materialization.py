"""Plan 9 Task 6 — dispatch_lever_6_with_intent materializes
RepairProposal.patch_body directly via to_proposal_dict() instead
of delegating to _generate_lever6_proposal_legacy for SQL.

Legacy body is reused only as the safety-net validator when
to_proposal_dict() raises (missing required patch_body field).
"""
from genie_space_optimizer.optimization.lever6_intent_dispatch import (
    dispatch_lever_6_with_intent,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


def _make_synthesizer_proposal():
    return RepairProposal(
        intent_id="intent_h001_001",
        intent_name="add_revenue_expression",
        intent_description="Add a revenue SQL expression.",
        repair_shape=RepairShape.SQL_EXPRESSION,
        patch_type=PatchType.ADD_SQL_SNIPPET_EXPRESSION,
        rationale="Cluster blames missing revenue computation.",
        confidence="high",
        patch_body={
            "name": "revenue_per_order",
            "sql_expression": "amount * quantity",
        },
        blame_set=("main.sales.orders",),
        target_objects=(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier="main.sales.orders",
                columns=("amount", "quantity"),
            ),
        ),
    )


def test_dispatch_uses_to_proposal_dict_when_proposal_validates(monkeypatch):
    """When patch_body validates against to_proposal_dict()'s
    contract, the dispatcher uses RepairProposal.to_proposal_dict()
    and does NOT call _generate_lever6_proposal_legacy."""
    fake_proposal = _make_synthesizer_proposal()

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        lambda **kwargs: fake_proposal,
    )

    legacy_calls: list = []

    def fake_legacy(**kwargs):
        legacy_calls.append(kwargs)
        return None  # If called, the test should fail.

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "_generate_lever6_proposal_legacy",
        fake_legacy,
    )

    fake_cluster = {
        "cluster_id": "c_h001",
        "blame_set": ["main.sales.orders"],
    }
    fake_metadata = {
        "instructions": {"example_question_sqls": []},
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.orders",
                    "columns": [
                        {"name": "amount"},
                        {"name": "quantity"},
                    ],
                },
            ],
        },
        # Pre-populated so the dispatcher skips the rca_evidence blame
        # walk (the test stub uses plain object() sentinels there).
        "schema_columns": ["amount", "quantity"],
    }

    result = dispatch_lever_6_with_intent(
        cluster=fake_cluster,
        metadata_snapshot=fake_metadata,
        w=None,
        rca_evidence_typed={"q": object()},
        llm_cluster=object(),
        ag_id="AG_H001",
        iteration=1,
    )

    assert result is not None
    assert result["sql_expression"] == "amount * quantity"
    assert result["name"] == "revenue_per_order"
    # Stamp from to_repair_intent flow.
    assert result.get("intent_id") == "intent_h001_001"
    assert "repair_intent" in result
    # Critical: legacy generator was NOT invoked for SQL generation.
    assert len(legacy_calls) == 0


def test_dispatch_falls_back_to_legacy_when_proposal_validation_fails(monkeypatch):
    """If RepairProposal.to_proposal_dict() raises (missing required
    patch_body field), the dispatcher falls back to the legacy body
    so the cycle does not crash. The fallback is a SAFETY NET, not
    the primary path."""
    bad_proposal = RepairProposal(
        intent_id="intent_bad",
        intent_name="x",
        intent_description="...",
        repair_shape=RepairShape.SQL_EXPRESSION,
        patch_type=PatchType.ADD_SQL_SNIPPET_EXPRESSION,
        rationale="...",
        confidence="high",
        patch_body={},  # Missing required "name" + "sql_expression"
        blame_set=(),
    )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        lambda **kwargs: bad_proposal,
    )

    legacy_called: list = []

    def fake_legacy(**kwargs):
        legacy_called.append(1)
        return {
            "patch_type": "add_sql_snippet_expression",
            "name": "x",
            "sql_expression": "1",
            "lever": 6,
        }

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "_generate_lever6_proposal_legacy",
        fake_legacy,
    )

    result = dispatch_lever_6_with_intent(
        cluster={"cluster_id": "c", "blame_set": []},
        metadata_snapshot={
            "instructions": {"example_question_sqls": []},
            "schema_columns": ["x"],
        },
        w=None,
        rca_evidence_typed={"q": object()},
        llm_cluster=object(),
        ag_id="AG_X",
        iteration=1,
    )

    assert result is not None
    assert len(legacy_called) == 1
