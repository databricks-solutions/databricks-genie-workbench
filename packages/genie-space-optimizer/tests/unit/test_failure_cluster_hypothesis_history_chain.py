"""Plan 8 Task 8 — stamp_hypotheses_on_metadata_snapshot also
hydrates the typed FailureCluster fields when a typed cluster
record is available on metadata_snapshot. The history field
preserves prior iterations' hypotheses."""
from __future__ import annotations

from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.repair_intent import (
    PatchType, RepairShape,
)
from genie_space_optimizer.optimization.rollback_hypothesis_typed import (
    NextAttemptHypothesis,
)
from genie_space_optimizer.optimization.rollback_learning import (
    stamp_hypotheses_on_metadata_snapshot,
)


def _hyp(cluster_id: str, iteration: int, intent_id: str) -> NextAttemptHypothesis:
    return NextAttemptHypothesis(
        rolled_back_intent_id=intent_id,
        cluster_id=cluster_id,
        ag_id="AG_X",
        iteration=iteration,
        why_failed=f"iter{iteration}",
        failure_mode=f"mode{iteration}",
        revised_repair_shape=RepairShape.OTHER,
        revised_patch_type=PatchType.ADD_INSTRUCTION,
        revised_blame_set=None,
        additional_evidence_needed=(),
        forbidden_signatures=(),
        confidence="low",
    )


def test_stamp_hydrates_typed_cluster_record():
    fc = FailureCluster.from_legacy(
        {"cluster_id": "H001", "question_ids": ["q1"],
         "root_cause": "x", "asi_failure_type": "y"}
    )
    metadata = {"_failure_cluster_records_by_id": {"H001": fc}}
    stamp_hypotheses_on_metadata_snapshot(
        metadata, {"H001": _hyp("H001", 1, "I001")}
    )
    updated = metadata["_failure_cluster_records_by_id"]["H001"]
    assert updated.last_attempt_hypothesis is not None
    assert updated.last_attempt_hypothesis.iteration == 1
    assert len(updated.hypothesis_history) == 1


def test_stamp_chains_history_across_iterations():
    fc = FailureCluster.from_legacy(
        {"cluster_id": "H001", "question_ids": ["q1"],
         "root_cause": "x", "asi_failure_type": "y"}
    )
    metadata = {"_failure_cluster_records_by_id": {"H001": fc}}
    stamp_hypotheses_on_metadata_snapshot(
        metadata, {"H001": _hyp("H001", 1, "I001")}
    )
    stamp_hypotheses_on_metadata_snapshot(
        metadata, {"H001": _hyp("H001", 2, "I002")}
    )
    updated = metadata["_failure_cluster_records_by_id"]["H001"]
    assert updated.last_attempt_hypothesis.iteration == 2
    # History contains BOTH iteration 1 and iteration 2.
    assert len(updated.hypothesis_history) == 2
    assert {h.iteration for h in updated.hypothesis_history} == {1, 2}
