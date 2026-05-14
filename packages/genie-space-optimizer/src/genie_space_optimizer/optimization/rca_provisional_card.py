"""Phase 2.2 — provisional RCA cards from soft signals.

Today: ``dominant_root_cause`` reads only ``asi_by_qid`` (hard
failures); soft signals can only augment, not synthesize a card.

Phase 2.2: when no hard ASI exists but >= 3 soft signals in the same
cluster point to a consistent ``root_cause_hint``, synthesize a
provisional ``RcaCard``-shaped dict with ``provisional=True`` so
downstream gates apply stricter rules.
"""
from __future__ import annotations

from collections import Counter
from enum import StrEnum
from typing import Any, Mapping, Sequence


PROVISIONAL_QUORUM: int = 3


class ProvisionalCardEligibility(StrEnum):
    QUORUM_NOT_REACHED = "quorum_not_reached"
    INCONSISTENT_HINTS = "inconsistent_hints"
    QUORUM_REACHED = "quorum_reached"


def _classify(
    soft_signals_for_cluster: Sequence[Mapping[str, Any]],
) -> tuple[ProvisionalCardEligibility, str]:
    """Return (eligibility, dominant_root_cause_hint).

    Returns ``QUORUM_NOT_REACHED`` when fewer than
    :data:`PROVISIONAL_QUORUM` signals; ``INCONSISTENT_HINTS`` when
    no single root_cause_hint reaches the quorum; otherwise
    ``QUORUM_REACHED`` and the dominant hint.
    """
    if len(soft_signals_for_cluster) < PROVISIONAL_QUORUM:
        return ProvisionalCardEligibility.QUORUM_NOT_REACHED, ""

    counter: Counter[str] = Counter(
        str(s.get("root_cause_hint") or "")
        for s in soft_signals_for_cluster
        if s.get("root_cause_hint")
    )
    if not counter:
        return ProvisionalCardEligibility.INCONSISTENT_HINTS, ""

    most_common, count = counter.most_common(1)[0]
    if count >= PROVISIONAL_QUORUM:
        return ProvisionalCardEligibility.QUORUM_REACHED, most_common
    return ProvisionalCardEligibility.INCONSISTENT_HINTS, ""


def build_provisional_card(
    *,
    cluster_id: str,
    soft_signals_for_cluster: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Build a provisional RcaCard-shaped dict from soft signals.

    Returns ``None`` when no consistent hint reaches quorum.

    Provisional cards always have:
      - ``provisional=True``
      - ``intended_patch_shape="structural"`` (the stricter default —
        Phase 2.3 will reject metadata-only patches when shape is
        structural)
      - ``target_qids`` aggregated from the signals
    """
    eligibility, hint = _classify(soft_signals_for_cluster)
    if eligibility != ProvisionalCardEligibility.QUORUM_REACHED:
        return None

    target_qids = sorted({
        str(s.get("qid") or "")
        for s in soft_signals_for_cluster
        if s.get("qid")
    })
    return {
        "cluster_id": str(cluster_id or ""),
        "root_cause": hint,
        "target_qids": target_qids,
        "intended_patch_shape": "structural",
        "provisional": True,
        "source": "soft_signals_quorum",
        "signal_count": len(soft_signals_for_cluster),
    }
