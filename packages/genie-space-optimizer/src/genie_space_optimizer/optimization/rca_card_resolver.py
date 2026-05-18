"""WU-3.5 — resolve the full RCA card object for downstream intent reads.

``build_rca_card`` (rca.py:1864-1866) stamps the cluster with a thin
dict ``{"rca_id": card.card_id}`` and persists the full structured
card on ``metadata_snapshot["_rca_card_store"][card.card_id]``. The
harness's downstream intent readers (Best-of-N trigger at
harness.py:~23012, structural-repair-gate consumer at
harness.py:~26966-26977) call ``getattr(cluster["rca_card"], attr,
"")`` which ALWAYS returns the empty default on the thin dict.

This module bridges the dict-vs-attribute gap. ``resolve_full_rca_card``
returns the full card object (or ``None``) so callers can read
intent attributes correctly.

Pure read-path: no mutation of either input. Callers re-resolve on
each read to honor any in-flight regen.
"""
from __future__ import annotations

from typing import Any, Mapping


def _is_full_card(value: Any) -> bool:
    """Heuristic — a "full card" exposes ``intended_patch_shape``
    (Phase 2.3+) AND ``card_id`` as attributes. A thin dict like
    ``{"rca_id": "..."}`` returns False because dicts don't have
    those attributes."""
    if isinstance(value, Mapping):
        return False
    return (
        hasattr(value, "intended_patch_shape")
        and hasattr(value, "card_id")
    )


def _extract_rca_id(cluster: Mapping[str, Any]) -> str:
    """Return the card id from any of the three shapes the harness
    stamps:
      1. ``cluster["rca_card"]["rca_id"]`` (thin dict shape from rca.py:1866).
      2. ``cluster["rca_card"].card_id`` (full card object — legacy).
      3. ``cluster["rca_card_id"]`` (id field — rca.py:1865).
    Empty string when no id is resolvable.
    """
    card = cluster.get("rca_card")
    if isinstance(card, Mapping):
        rca_id = card.get("rca_id") or card.get("card_id")
        if rca_id:
            return str(rca_id)
    elif card is not None:
        card_id = getattr(card, "card_id", "") or ""
        if card_id:
            return str(card_id)
    rca_card_id = cluster.get("rca_card_id")
    return str(rca_card_id) if rca_card_id else ""


def resolve_full_rca_card(
    cluster: Mapping[str, Any],
    metadata_snapshot: Mapping[str, Any],
) -> Any:
    """Return the full RCA card object for ``cluster``, or ``None``.

    Resolution order:
      1. If ``cluster["rca_card"]`` is already a full card object
         (has ``intended_patch_shape`` AND ``card_id`` as attributes),
         return it as-is. Legacy / pre-Plan-4a paths stamp the full
         object directly.
      2. Otherwise, extract the card id (via ``_extract_rca_id``) and
         look up ``metadata_snapshot["_rca_card_store"][id]``. Return
         the value if present.
      3. Otherwise, return ``None``. Callers apply their own typed
         decline policy.

    Flag-gated by ``GSO_FULL_RCA_CARD_RESOLVER``. When OFF, returns
    ``None`` unconditionally so callers fall back to their legacy
    ``getattr`` path (byte-stable with pre-WU-3.5).

    Pure: does not mutate either input.
    """
    from genie_space_optimizer.common.config import (
        full_rca_card_resolver_enabled,
    )
    if not full_rca_card_resolver_enabled():
        return None
    if cluster is None:
        return None

    candidate = cluster.get("rca_card")
    if _is_full_card(candidate):
        return candidate

    rca_id = _extract_rca_id(cluster)
    if not rca_id:
        return None
    store = (metadata_snapshot or {}).get("_rca_card_store") or {}
    if not isinstance(store, Mapping):
        return None
    found = store.get(rca_id)
    return found if found is not None else None
