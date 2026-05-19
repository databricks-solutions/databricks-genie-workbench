"""Plan 8 Task 8 — FailureCluster carries last_attempt_hypothesis and
hypothesis_history typed fields (defaults preserve byte-stability)."""
from __future__ import annotations

from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.rollback_hypothesis_typed import (
    NextAttemptHypothesis,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType, RepairShape,
)


def _hyp(cluster_id: str = "H001") -> NextAttemptHypothesis:
    return NextAttemptHypothesis(
        rolled_back_intent_id="I001",
        cluster_id=cluster_id,
        ag_id="AG_X",
        iteration=1,
        why_failed="overgeneralized",
        failure_mode="too_broad_filter",
        revised_repair_shape=RepairShape.FILTER_COMPOSE,
        revised_patch_type=PatchType.ADD_INSTRUCTION,
        revised_blame_set=None,
        additional_evidence_needed=(),
        forbidden_signatures=(),
        confidence="medium",
    )


def test_failure_cluster_defaults_to_none_hypothesis():
    fc = FailureCluster.from_legacy(
        {"cluster_id": "H001", "question_ids": ["q1"],
         "root_cause": "x", "asi_failure_type": "y"}
    )
    assert fc.last_attempt_hypothesis is None
    assert fc.hypothesis_history == ()


def test_failure_cluster_with_hypothesis_field():
    fc = FailureCluster(
        cluster_id="H001",
        target_qids=("q1",),
        root_cause="x", asi_failure_type="y",
        failure_keys=(),
        blame_set_raw=(), blame_set_normalized=(),
        rca_card_id="", rca_card_summary="", is_grounded=False,
        last_attempt_hypothesis=_hyp(),
    )
    assert fc.last_attempt_hypothesis is not None
    assert fc.last_attempt_hypothesis.cluster_id == "H001"
