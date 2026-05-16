"""Thin bridge mapping ``RCA_REPAIR_MATRIX`` to lever-rotation primitives
the harness can consume.

Today ``_map_to_lever`` returns a single int — there is no rotation. When
that single lever's strategist returns empty proposals, the loop emits
``proposal_generation_empty`` and the next iteration computes the same
single int again, producing a stalemate.

This module exposes:

* :func:`lever_set_for_rca_kind` — the *ordered* lever priority for a
  typed ``RcaKind``, derived from the matrix's preferred-then-fallback
  pair order. Duplicate levers (e.g. JOIN_SPEC has two lever-4 pairs)
  collapse to a single entry preserving first-seen position.
* :func:`next_untried_repair` — the rotation primitive: given the
  ``RcaKind`` and the set of levers already tried-and-failed for this
  cluster in this run, return the next ``(lever, patch_type)`` pair
  from the matrix that has not been tried, or ``None`` if exhausted.
* :func:`resolve_rca_kind_for_cluster` — translate a cluster dict's
  ASI labels into a ``RcaKind`` using the existing ``_safe_rca_kind``
  vocabulary bridge (so the same translation runs on every consumer).

The bridge is intentionally pure: no harness imports, no I/O.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind
from genie_space_optimizer.optimization.rca_repair_coverage import (
    RCA_REPAIR_MATRIX,
    RepairPair,
)


def lever_set_for_rca_kind(rca_kind: RcaKind) -> tuple[int, ...]:
    """Return the ordered tuple of lever families for ``rca_kind``,
    derived from ``RCA_REPAIR_MATRIX`` by extracting the lever int from
    each ``RepairPair`` and de-duplicating while preserving order.

    Returns ``()`` for ``RcaKind.UNKNOWN`` (which is intentionally
    uncovered in the matrix).
    """
    pairs = RCA_REPAIR_MATRIX.get(rca_kind, ())
    seen: set[int] = set()
    result: list[int] = []
    for lever, _patch_type in pairs:
        if lever in seen:
            continue
        seen.add(lever)
        result.append(int(lever))
    return tuple(result)
