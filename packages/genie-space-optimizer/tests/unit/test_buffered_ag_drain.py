"""TDD coverage for selective buffered-AG drain (T1, T2, T3).

The three skip paths in harness.py at the dead-on-arrival,
pre-AG-snapshot-failure, and applier-rejection sites used to
unconditionally clear ``pending_action_groups`` when the *current* AG
failed. Cycle 9 demonstrated the cost: H001 dead-on-arrived in iter 1
and H002/H003 (which targeted unrelated clusters) were silently
discarded for iters 2-5. This regression test pins the new contract:
buffered AGs whose ``affected_questions`` are disjoint from the failed
AG survive.

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T1, T2, T3.
"""
from __future__ import annotations


from genie_space_optimizer.optimization.harness import (
    _drain_buffered_action_groups,
)


def _ag(ag_id: str, qids: list[str]) -> dict:
    return {"id": ag_id, "affected_questions": list(qids)}


def test_dead_on_arrival_keeps_disjoint_buffered_ags():
    failed = _ag("AG_DECOMPOSED_H001", ["gs_024"])
    buffered = [
        _ag("AG_DECOMPOSED_H002", ["gs_009"]),
        _ag("AG_DECOMPOSED_H003", ["gs_016"]),
    ]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="dead_on_arrival",
    )
    assert [a["id"] for a in survivors] == [
        "AG_DECOMPOSED_H002",
        "AG_DECOMPOSED_H003",
    ]
    assert dropped == []


def test_dead_on_arrival_drops_overlapping_buffered_ags():
    failed = _ag("AG_DECOMPOSED_H001", ["gs_024", "gs_025"])
    buffered = [
        _ag("AG_DECOMPOSED_H002", ["gs_025"]),
        _ag("AG_DECOMPOSED_H003", ["gs_016"]),
    ]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="dead_on_arrival",
    )
    assert [a["id"] for a in survivors] == ["AG_DECOMPOSED_H003"]
    assert [a["id"] for a in dropped] == ["AG_DECOMPOSED_H002"]


def test_drain_handles_failed_ag_with_no_affected_questions():
    failed = _ag("AG_BROAD", [])
    buffered = [_ag("AG_DECOMPOSED_H002", ["gs_009"])]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="dead_on_arrival",
    )
    # Empty failed-AG qids → conservatively keep all buffered (we can't
    # prove overlap; failure of an unscoped AG doesn't prove unrelated
    # AGs will fail).
    assert [a["id"] for a in survivors] == ["AG_DECOMPOSED_H002"]
    assert dropped == []


def test_pre_snapshot_failure_keeps_disjoint_buffered_ags():
    failed = _ag("AG_DECOMPOSED_H001", ["gs_024"])
    buffered = [_ag("AG_DECOMPOSED_H002", ["gs_009"])]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="pre_ag_snapshot_failed",
    )
    assert [a["id"] for a in survivors] == ["AG_DECOMPOSED_H002"]
    assert dropped == []


def test_applier_failure_keeps_disjoint_buffered_ags():
    failed = _ag("AG_DECOMPOSED_H001", ["gs_024"])
    buffered = [
        _ag("AG_DECOMPOSED_H002", ["gs_009"]),
        _ag("AG_DECOMPOSED_H003", ["gs_016"]),
    ]
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=buffered,
        reason="all_selected_patches_dropped_by_applier",
    )
    assert [a["id"] for a in survivors] == [
        "AG_DECOMPOSED_H002",
        "AG_DECOMPOSED_H003",
    ]
    assert dropped == []


def test_drain_handles_empty_buffered_list():
    failed = _ag("AG_DECOMPOSED_H001", ["gs_024"])
    survivors, dropped = _drain_buffered_action_groups(
        failed_ag=failed,
        buffered=[],
        reason="any_reason",
    )
    assert survivors == []
    assert dropped == []
