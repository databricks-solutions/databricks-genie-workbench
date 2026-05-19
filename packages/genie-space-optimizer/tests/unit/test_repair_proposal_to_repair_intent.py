"""Plan 5 Task 15 — RepairProposal.to_repair_intent projector.

Pin the bridge from Plan-5's typed RepairProposal to Plan-1's typed
RepairIntent. Postmortem joining + Plan-1's IntentOutcome carrier on
AgOutcomeRecord both depend on a consistent intent_id and source string.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


def _cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H001", target_qids=("gs_001", "gs_002"),
        root_cause="missing_top_n", asi_failure_type="missing_top_n",
        failure_keys=("missing_top_n",),
        blame_set_raw=("sales.fact_sales.revenue",),
        blame_set_normalized=("sales.fact_sales.revenue",),
        rca_card_id="rca_card_42", rca_card_summary="x",
        is_grounded=True,
    )


def _proposal() -> RepairProposal:
    return RepairProposal(
        intent_id="intent_H001_AG3_001",
        intent_name="top_n_revenue_by_region",
        intent_description="add example_sql for top-N",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="both qids miss LIMIT/ORDER BY",
        confidence="high",
        patch_body={
            "example_question": "What are top 3 regions?",
            "example_sql": "SELECT 1",
        },
        blame_set=("sales.fact_sales.revenue",),
    )


def test_to_repair_intent_carries_intent_id_verbatim() -> None:
    intent = _proposal().to_repair_intent(cluster=_cluster(), ag_id="AG3")
    assert intent.intent_id == "intent_H001_AG3_001"


def test_to_repair_intent_source_is_llm_l5b_synthesis() -> None:
    intent = _proposal().to_repair_intent(cluster=_cluster(), ag_id="AG3")
    assert intent.source == "llm_l5b_synthesis"


def test_to_repair_intent_preserves_typed_enums() -> None:
    intent = _proposal().to_repair_intent(cluster=_cluster(), ag_id="AG3")
    assert intent.repair_shape is RepairShape.TOP_N_BY_METRIC
    assert intent.patch_type is PatchType.ADD_EXAMPLE_SQL


def test_to_repair_intent_carries_provenance_from_cluster_and_ag() -> None:
    intent = _proposal().to_repair_intent(cluster=_cluster(), ag_id="AG3")
    assert intent.cluster_id == "H001"
    assert intent.target_qids == ("gs_001", "gs_002")
    assert intent.rca_card_id == "rca_card_42"
    assert intent.ag_id == "AG3"


def test_to_repair_intent_blame_set_matches_proposal_blame_set() -> None:
    intent = _proposal().to_repair_intent(cluster=_cluster(), ag_id="AG3")
    assert intent.blame_set == ("sales.fact_sales.revenue",)


def test_to_repair_intent_round_trips_through_json() -> None:
    intent = _proposal().to_repair_intent(cluster=_cluster(), ag_id="AG3")
    payload = intent.to_json()
    rebuilt = RepairIntent.from_json(payload)
    assert rebuilt == intent
