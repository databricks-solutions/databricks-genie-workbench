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


def next_untried_repair(
    rca_kind: RcaKind,
    *,
    tried: frozenset[int],
) -> RepairPair | None:
    """Return the next ``(lever, patch_type)`` pair from
    ``RCA_REPAIR_MATRIX[rca_kind]`` whose ``lever`` is not in ``tried``.

    Rotation is at the **lever family** granularity — when ``tried`` is
    ``{4}`` and the matrix has two consecutive lever-4 pairs, BOTH are
    skipped (the strategist already proved lever 4 cannot produce a
    proposal for this cluster's RCA card; the second lever-4 patch_type
    is unlikely to succeed where the first failed).

    Returns ``None`` when:

    * ``rca_kind`` is ``UNKNOWN`` (no matrix entry), OR
    * every lever in the matrix entry is already in ``tried``.
    """
    pairs = RCA_REPAIR_MATRIX.get(rca_kind, ())
    for lever, patch_type in pairs:
        if lever in tried:
            continue
        return (int(lever), str(patch_type))
    return None


def resolve_rca_kind_for_cluster(cluster: dict) -> RcaKind:
    """Return the typed ``RcaKind`` for ``cluster`` by consulting (in
    priority order):

    1. ``cluster["rca_card"]["rca_kind"]`` — set by RCA card construction
       when the card is fully grounded.
    2. ``cluster["asi_failure_type"]`` translated via the existing
       ``_safe_rca_kind`` vocabulary bridge.
    3. ``cluster["root_cause"]`` translated via ``_safe_rca_kind``.

    Returns ``RcaKind.UNKNOWN`` when nothing in the cluster resolves —
    the matrix has no entry for UNKNOWN so callers fall back to
    ``_map_to_lever``.

    This is the single point of truth for the vocabulary bridge so any
    drift in the ASI label set surfaces as a single helper test failure
    instead of bug-fanout across consumers.
    """
    from genie_space_optimizer.optimization.rca import _safe_rca_kind

    rca_card = cluster.get("rca_card") or {}
    if isinstance(rca_card, dict):
        kind_raw = rca_card.get("rca_kind")
        if kind_raw:
            try:
                return RcaKind(str(kind_raw))
            except ValueError:
                pass

    failure_type = str(cluster.get("asi_failure_type") or "").strip()
    if failure_type:
        kind = _safe_rca_kind(None, failure_type, cluster)
        if kind is not RcaKind.UNKNOWN:
            return kind

    root_cause = str(cluster.get("root_cause") or "").strip()
    if root_cause:
        return _safe_rca_kind(None, root_cause, cluster)

    return RcaKind.UNKNOWN
