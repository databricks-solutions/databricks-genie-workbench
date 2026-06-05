"""Pure binding decisions for mechanism learning (e943 / d139 plan,
Phase 2 item #10).

Promotes the mechanism-coverage check and the RCA→mechanism route from
observe-only to *binding* (drop the inadequate proposal). The functions
here are pure index selectors over per-proposal verdict labels so they
can be unit-tested without the heavyweight ``RepairProposal`` /
synthesis machinery. ``synthesize.py`` calls them and applies the
returned survivor indices to its typed proposal list.

Critical safety invariant — NEVER empty the slate. Dropping the sole
surviving proposal re-creates the all-dropped flatline the synthesis
design explicitly guards against ("central design tension"). When a
binding filter would remove every proposal, it removes none and falls
back to observe-only (returns all indices as survivors).
"""
from __future__ import annotations

from collections.abc import Sequence


def coverage_survivor_indices(
    outcomes: Sequence[str | None],
    *,
    binding_enabled: bool,
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Select survivors for the mechanism-coverage binding.

    Args:
      outcomes: per-proposal coverage verdict labels aligned with the
        proposal list. Each is one of ``"covered"`` / ``"override"`` /
        ``"uncovered"`` / ``None`` (no mechanism mapped — not subject to
        the check). Only ``"uncovered"`` is droppable.
      binding_enabled: when False, no proposal is dropped (observe-only).

    Returns:
      ``(survivor_indices, dropped_indices)`` — both ascending tuples.

    Slate-safety: if every proposal is ``"uncovered"`` (or the list is
    empty), nothing is dropped.
    """
    n = len(outcomes)
    all_idx = tuple(range(n))
    if not binding_enabled or n == 0:
        return all_idx, ()
    droppable = tuple(i for i, o in enumerate(outcomes) if o == "uncovered")
    if not droppable:
        return all_idx, ()
    # Slate safety: at least one proposal must survive.
    if len(droppable) >= n:
        return all_idx, ()
    dropped = set(droppable)
    survivors = tuple(i for i in range(n) if i not in dropped)
    return survivors, droppable


def rca_route_survivor_indices(
    defaulted_flags: Sequence[bool],
    *,
    binding_enabled: bool,
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """Select survivors for the RCA→mechanism route binding.

    Args:
      defaulted_flags: per-proposal flag — True when the proposal
        ``defaulted to example_sql`` for an example-SQL-insufficient
        RCA (i.e. carries no fixing mechanism). Such proposals are
        behaviorally inert for the RCA and are the drop candidates.
      binding_enabled: when False, no proposal is dropped (observe-only).

    Returns:
      ``(survivor_indices, dropped_indices)`` — both ascending tuples.

    Slate-safety: if every proposal is defaulted (or the list is empty),
    nothing is dropped — falls back to the observe-and-route marker so
    the slate never flatlines.
    """
    n = len(defaulted_flags)
    all_idx = tuple(range(n))
    if not binding_enabled or n == 0:
        return all_idx, ()
    droppable = tuple(i for i, flag in enumerate(defaulted_flags) if flag)
    if not droppable or len(droppable) >= n:
        return all_idx, ()
    dropped = set(droppable)
    survivors = tuple(i for i in range(n) if i not in dropped)
    return survivors, droppable


__all__ = [
    "coverage_survivor_indices",
    "rca_route_survivor_indices",
]
