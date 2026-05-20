"""Plan 9 Task 6 — _dispatch_lever_5b_for_cluster materializes
RepairProposal.patch_body directly via to_proposal_dict() instead
of calling the archetype-gated cluster_driven_synthesis.

Mirror of test_lever6_intent_dispatch_direct_materialization for
the L5b path.
"""
from genie_space_optimizer.optimization.optimizer import (
    _dispatch_lever_5b_for_cluster,
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


def test_l5b_uses_to_proposal_dict_for_add_example_sql(monkeypatch):
    """When Plan 5 synthesizes a typed RepairProposal with
    patch_type=ADD_EXAMPLE_SQL, the dispatcher materializes the
    proposal_dict directly without invoking the cross-lever
    generator's SQL synthesis."""
    typed = RepairProposal(
        intent_id="intent_top_n",
        intent_name="top_n_revenue_by_product",
        intent_description="Top-N revenue by product.",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="...",
        confidence="high",
        patch_body={
            "example_question": "What are the top 5 products by revenue?",
            "example_sql": (
                "SELECT product, SUM(amount) AS revenue "
                "FROM main.sales.orders "
                "GROUP BY product "
                "ORDER BY revenue DESC LIMIT 5"
            ),
        },
        blame_set=("main.sales.orders",),
        target_objects=(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier="main.sales.orders",
                columns=("product", "amount"),
            ),
        ),
        required_constructs=("SELECT", "GROUP_BY", "ORDER_BY", "LIMIT"),
    )

    # _dispatch_lever_5b_for_cluster lazy-imports these inside the
    # function, so we patch them at their source modules.
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.repair_intent_synthesizer."
        "synthesize_repair_intent_for_cluster",
        lambda **kwargs: typed,
    )

    legacy_generator_calls: list = []

    def spy_router(proposal):
        # When direct materialization succeeds, the dispatcher must
        # NOT invoke the per-lever generator at all (it's only
        # consulted for the override_event).
        def fake_generator(p):
            legacy_generator_calls.append(p)
            return {"patch_type": "wrong", "shouldnt_be_returned": True}
        return (fake_generator, None)

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.cross_lever_router."
        "route_to_per_lever_generator",
        spy_router,
    )

    class _FakeRcaEvidence:
        blame_set = ("main.sales.orders",)

    fake_cluster = {
        "cluster_id": "c_h001",
        "root_cause": "plural_top_n_collapse",
        "blame_set": ["main.sales.orders"],
        "affected_qids": ["q_001"],
    }
    fake_metadata = {
        "instructions": {"example_question_sqls": []},
        "data_sources": {
            "tables": [{
                "identifier": "main.sales.orders",
                "columns": [
                    {"name": "product"},
                    {"name": "amount"},
                ],
            }],
        },
        "schema_columns": ["product", "amount"],
    }

    proposals = _dispatch_lever_5b_for_cluster(
        cluster=fake_cluster,
        metadata_snapshot=fake_metadata,
        w=None,
        benchmark_corpus=None,
        benchmarks=None,
        rca_evidence_typed={"q_001": _FakeRcaEvidence()},
        llm_cluster=object(),
        ag_id="AG_H001",
        iteration=1,
    )

    assert len(proposals) == 1
    p = proposals[0]
    # to_proposal_dict() projected the typed patch_body into the
    # canonical add_example_sql shape (per repair_proposal_typed.py:123).
    assert "ORDER BY revenue DESC" in p["example_sql"]
    assert p["example_question"].startswith("What are the top 5")
    # Plan 9 stamp on provenance.
    prov = p.get("provenance") or {}
    assert prov.get("plan9_materialization_source") == "plan9_direct"
    # RepairIntent stamp must still flow.
    assert p.get("intent_id") == "intent_top_n"
    assert "repair_intent" in p
    # Critical: the per-lever generator was NOT invoked for SQL
    # generation when direct materialization succeeded.
    assert len(legacy_generator_calls) == 0
