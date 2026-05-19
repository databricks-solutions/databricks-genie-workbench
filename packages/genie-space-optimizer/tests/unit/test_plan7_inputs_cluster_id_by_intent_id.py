"""Plan 8 Task 9 — plan7_inputs.build_cluster_id_by_intent_id derives
the reverse map from ProposalSlate.repair_intents_by_id."""
from __future__ import annotations

from genie_space_optimizer.optimization.plan7_inputs import (
    build_cluster_id_by_intent_id,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType, RepairIntent, RepairShape,
)


def _intent(intent_id: str, cluster_id: str) -> RepairIntent:
    return RepairIntent(
        intent_id=intent_id, intent_name="n", intent_description="d",
        repair_shape=RepairShape.OTHER, patch_type=PatchType.ADD_INSTRUCTION,
        rationale="r", confidence="medium", source="src",
        cluster_id=cluster_id, target_qids=("q1",), blame_set=(),
        rca_card_id="", ag_id="AG_X",
    )


def test_cluster_id_by_intent_id_builds_map():
    intents = {"I001": _intent("I001", "H001"),
                "I002": _intent("I002", "H002")}
    out = build_cluster_id_by_intent_id(intents)
    assert out == {"I001": "H001", "I002": "H002"}
