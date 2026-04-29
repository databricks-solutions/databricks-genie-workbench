from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    decide_quarantine_continuation,
)


def test_stop_when_patchable_hard_qids_are_quarantined_and_no_hard_clusters_remain() -> None:
    decision = decide_quarantine_continuation(
        quarantined_qids={"q001", "q009", "q021"},
        unresolved_patchable_qids={"q001", "q009", "q021"},
        hard_cluster_count_after_prune=0,
        soft_cluster_count_after_prune=2,
    )
    assert decision["action"] == "stop_for_human_review"
    assert decision["reason"] == "quarantined_patchable_hard_failures"
    assert decision["blocking_qids"] == ["q001", "q009", "q021"]


def test_continue_when_only_soft_quarantine_and_hard_clusters_remain() -> None:
    decision = decide_quarantine_continuation(
        quarantined_qids={"q004"},
        unresolved_patchable_qids=set(),
        hard_cluster_count_after_prune=1,
        soft_cluster_count_after_prune=3,
    )
    assert decision["action"] == "continue"
    assert decision["reason"] == "hard_clusters_remain"


def test_diagnostic_lane_when_hard_clusters_remain_but_patchable_qids_were_removed() -> None:
    decision = decide_quarantine_continuation(
        quarantined_qids={"q021"},
        unresolved_patchable_qids={"q021"},
        hard_cluster_count_after_prune=2,
        soft_cluster_count_after_prune=1,
    )
    assert decision["action"] == "diagnostic_lane"
    assert decision["blocking_qids"] == ["q021"]
