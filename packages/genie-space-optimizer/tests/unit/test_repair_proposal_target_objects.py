"""Plan 9 Task 1 — RepairProposal.target_objects field.

Verifies that RepairProposal carries the typed slice through from
LLM output to materialization. to_repair_intent propagates
target_objects into the RepairIntent stamped on the proposal dict.
"""
from genie_space_optimizer.optimization.failure_cluster import FailureCluster
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


def _make_cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="c_test",
        target_qids=("q_001",),
        root_cause="plural_top_n_collapse",
        asi_failure_type="plural_top_n_collapse",
        failure_keys=("plural_top_n_collapse",),
        blame_set_raw=("catalog.schema.orders",),
        blame_set_normalized=("catalog.schema.orders",),
        rca_card_id="rca_test",
        rca_card_summary="needs top-n shape",
        is_grounded=True,
        semantic_theme="top_n_ranking",
    )


def test_repair_proposal_target_objects_defaults_empty():
    proposal = RepairProposal(
        intent_id="intent_001",
        intent_name="top_n_repair",
        intent_description="Add a top-N example.",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="Cluster blames plural-top-N collapse.",
        confidence="high",
        patch_body={
            "example_question": "What are the top 5 products by revenue?",
            "example_sql": (
                "SELECT product, SUM(amount) FROM orders "
                "GROUP BY product ORDER BY 2 DESC LIMIT 5"
            ),
        },
        blame_set=("catalog.schema.orders",),
    )
    assert proposal.target_objects == ()


def test_repair_proposal_target_objects_round_trips_via_json():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="catalog.schema.orders",
            columns=("product", "amount"),
        ),
    )
    proposal = RepairProposal(
        intent_id="intent_002",
        intent_name="top_n_repair",
        intent_description="Top-N example.",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="...",
        confidence="high",
        patch_body={
            "example_question": "Top 5 products by revenue?",
            "example_sql": (
                "SELECT product, SUM(amount) FROM orders "
                "GROUP BY product ORDER BY 2 DESC LIMIT 5"
            ),
        },
        blame_set=("catalog.schema.orders",),
        target_objects=targets,
    )
    payload = proposal.to_json()
    assert "target_objects" in payload
    reconstructed = RepairProposal.from_json(payload)
    assert reconstructed.target_objects == targets


def test_to_repair_intent_propagates_target_objects():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="catalog.schema.orders",
            columns=("product", "amount"),
        ),
    )
    proposal = RepairProposal(
        intent_id="intent_003",
        intent_name="top_n_repair",
        intent_description="...",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="...",
        confidence="high",
        patch_body={
            "example_question": "Top 5?",
            "example_sql": "SELECT 1",
        },
        blame_set=("catalog.schema.orders",),
        target_objects=targets,
    )
    intent = proposal.to_repair_intent(
        cluster=_make_cluster(),
        ag_id="AG_TEST",
    )
    assert intent.target_objects == targets
