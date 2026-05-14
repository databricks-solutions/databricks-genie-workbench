"""Phase 5 fixture: after a rollback, the strategist input prioritizes the
regressed cluster ahead of the original target cluster.

Maps to user-text ``test_rollback_pivots_to_regressed_cluster``.
Anchor: ccf1d60d iter-1 -> iter-2 pivot.

Production API note (drift from plan stub):
    ``build_recovery_priority_list`` (Phase 1+2 Task 9, commit 9b2c14a1)
    takes pre-mapped cluster ids -- ``regressed_qids_to_cluster_id`` (a
    qid->cluster_id mapping), ``uncovered_cluster_ids`` (a sequence of
    cluster ids), and ``original_target_cluster_id`` (a single string).
    The plan's stub passed raw qid lists; the test below uses the
    canonical production signature so the assertion still pins the
    regressed-first ordering invariant.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.recovery_priority import (
    build_recovery_priority_list,
)

from tests.replay.fixtures.phase5._helpers import load


GS021_CLUSTER = "H002"
GS026_CLUSTER = "H001"


def test_recovery_priority_puts_regressed_cluster_first() -> None:
    iter1 = load("ccf1d60d_iter1.json")
    regressed_qids = iter1["out_of_target_regressed_qids"]
    target_qids = iter1["target_qids"]

    assert regressed_qids == ["gs_021"], (
        f"fixture invariant: ccf1d60d iter-1 has gs_021 as the lone "
        f"out-of-target regression; got {regressed_qids!r}"
    )
    assert target_qids == ["gs_026"], (
        f"fixture invariant: ccf1d60d iter-1 targets gs_026; "
        f"got {target_qids!r}"
    )

    priority = build_recovery_priority_list(
        regressed_qids_to_cluster_id={"gs_021": GS021_CLUSTER},
        uncovered_cluster_ids=(),
        original_target_cluster_id=GS026_CLUSTER,
    )

    h002_index = priority.index(GS021_CLUSTER)
    h001_index = priority.index(GS026_CLUSTER)
    assert h002_index < h001_index, (
        f"H002 (regressed gs_021) must precede H001 (original gs_026): "
        f"priority={priority}"
    )


def test_empty_regressed_qids_falls_back_to_original_target() -> None:
    """Defensive: with no regressions, original target leads the priority."""
    priority = build_recovery_priority_list(
        regressed_qids_to_cluster_id={},
        uncovered_cluster_ids=(),
        original_target_cluster_id=GS026_CLUSTER,
    )
    assert priority == (GS026_CLUSTER,)
