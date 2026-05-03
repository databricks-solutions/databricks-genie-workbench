"""TDD coverage for the dead-on-arrival ledger contract (T4).

Empty patch signatures (``()``) must never be cached as "already tried".
Otherwise a blast-radius drop in iter 1 short-circuits every subsequent
iteration before the strategist can change tack — the cycle 9 failure
mode where iters 2-5 each computed signature `()` independently and
immediately matched the cached `()` from iter 1.

Plan: ``docs/2026-05-03-cycle9-burndown-blast-radius-recovery-and-decision-trace-plan.md``
T4.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _record_dead_on_arrival_signature,
)


def test_empty_signature_is_not_recorded():
    seen: set[tuple[str, ...]] = set()
    _record_dead_on_arrival_signature(
        seen=seen,
        signature=(),
        reason="all_patches_dropped_by_blast_radius",
    )
    assert seen == set()


def test_non_empty_signature_is_recorded():
    seen: set[tuple[str, ...]] = set()
    _record_dead_on_arrival_signature(
        seen=seen,
        signature=("P001#1", "P002#1"),
        reason="all_selected_patches_dropped_by_applier",
    )
    assert seen == {("P001#1", "P002#1")}


def test_blast_radius_dropped_does_not_block_future_attempts():
    seen: set[tuple[str, ...]] = set()
    _record_dead_on_arrival_signature(
        seen=seen,
        signature=(),
        reason="all_patches_dropped_by_blast_radius",
    )
    # Future iter computes the same empty signature; should not match.
    assert () not in seen


def test_ledger_recording_is_idempotent_for_repeated_signatures():
    seen: set[tuple[str, ...]] = set()
    sig = ("P001#1",)
    _record_dead_on_arrival_signature(seen=seen, signature=sig, reason="r")
    _record_dead_on_arrival_signature(seen=seen, signature=sig, reason="r")
    # Set semantics — duplicate add is a no-op.
    assert seen == {sig}
