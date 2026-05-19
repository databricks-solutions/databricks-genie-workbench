"""Plan 1 Task 7 — synthesizers stamp RepairIntent onto proposals.

Three live producers (cluster_driven_synthesis, synthesis (lean L5b),
three_stage_pipeline) get small public helpers that downstream
callers invoke at the emit boundary to attach a typed intent to a
batch of proposals.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.archetypes import ARCHETYPES
from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
    extract_repair_intent_from_proposal,
)


def _cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H001",
        target_qids=("gs_009",),
        root_cause="plural_top_n_collapse",
        asi_failure_type="plural_top_n_collapse",
        failure_keys=("plural_top_n_collapse",),
        blame_set_raw=("flights.carrier",),
        blame_set_normalized=("flights.carrier",),
        rca_card_id="rca_v1",
        rca_card_summary="needs top-n shape",
        is_grounded=True,
    )


def test_cluster_driven_synthesis_helper_stamps_intent_on_emitted_proposals() -> None:
    """``stamp_proposals_from_archetype`` attaches a typed intent to
    a batch of proposals at the emit boundary."""
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        stamp_proposals_from_archetype,
    )

    arch = next(a for a in ARCHETYPES if a.name == "top_n_by_metric")
    proposals: list[dict] = [
        {"proposal_id": "p1", "patch_type": "add_example_sql"},
        {"proposal_id": "p2", "patch_type": "add_example_sql"},
    ]
    stamp_proposals_from_archetype(
        proposals=proposals,
        archetype=arch,
        cluster=_cluster(),
        ag_id="AG_H001_L5",
    )
    a = extract_repair_intent_from_proposal(proposals[0])
    b = extract_repair_intent_from_proposal(proposals[1])
    assert a is not None and b is not None
    assert a.intent_name == "top_n_by_metric"
    assert a.repair_shape is RepairShape.TOP_N_BY_METRIC
    assert a.patch_type is PatchType.ADD_EXAMPLE_SQL
    assert a.intent_id != b.intent_id
    assert a.intent_id.endswith("_001")
    assert b.intent_id.endswith("_002")


def test_synthesis_lean_path_helper_stamps_intent() -> None:
    """The helper is re-exported from synthesis so the lean-path
    synthesizer can reuse it without a deeper import cycle."""
    from genie_space_optimizer.optimization.synthesis import (
        stamp_proposals_from_archetype as lean_stamp,
    )
    arch = next(a for a in ARCHETYPES if a.name == "ordered_list_by_metric")
    proposals: list[dict] = [
        {"proposal_id": "p_lean", "patch_type": "add_example_sql"}
    ]
    lean_stamp(
        proposals=proposals,
        archetype=arch,
        cluster=_cluster(),
        ag_id="AG_H001_L5",
    )
    intent = extract_repair_intent_from_proposal(proposals[0])
    assert intent is not None
    assert intent.repair_shape is RepairShape.ORDERED_LIST_BY_METRIC


def test_three_stage_pipeline_emits_intent_for_known_patch_types() -> None:
    """``intent_from_three_stage_emission`` produces a typed intent
    from the three_stage_pipeline's stage-2 emission context (no
    Archetype involved)."""
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        intent_from_three_stage_emission,
    )
    intent = intent_from_three_stage_emission(
        patch_type=PatchType.ADD_JOIN_SPEC,
        cluster=_cluster(),
        ag_id="AG_H001_L4",
        seq=1,
        rationale="L4 join discovery emit.",
    )
    assert intent.patch_type is PatchType.ADD_JOIN_SPEC
    assert intent.repair_shape is RepairShape.JOIN_DISCOVERY
    assert intent.intent_id == "intent_H001_AG_H001_L4_three_stage_add_join_spec_001"
    assert intent.source == "three_stage_pipeline_stage_2"
