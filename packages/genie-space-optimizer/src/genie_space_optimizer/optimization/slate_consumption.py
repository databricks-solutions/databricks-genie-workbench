"""Authoritative consumption of the AG slate.

Today the harness computes the action_groups stage slate
(``stages.action_groups.select``) and the admission trace
(``admission_trace_consumer.apply_admission_trace``) but does not
authoritatively consume them — the iteration body continues with the
original ``ag``. This module returns a typed decision that the
harness applies.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Sequence

from genie_space_optimizer.optimization.admission_trace_consumer import (
    AdmissionResult,
)


class SlateAction(str, Enum):
    PROCEED = "proceed"
    SKIP_AG = "skip_ag"
    PIVOT_ITERATION = "pivot_iteration"


@dataclass(frozen=True, slots=True)
class SlateDecision:
    action: SlateAction
    reason: str
    denied_ag_id: str


def _ag_id(ag: Mapping[str, object]) -> str:
    return str(ag.get("id") or ag.get("ag_id") or "")


def _ag_effective_source_clusters(ag: Mapping[str, object]) -> set[str]:
    """Return the effective source_cluster_ids set for an AG.

    Priority chain (mirrors the capture-side backfill in
    ``scripts/capture_tape_from_mlflow.py`` so live and replay views
    agree):
      1. explicit ``ag.source_cluster_ids`` (production shape)
      2. ``ag.patches[*].cluster_id`` (export-shape with patches but
         no explicit source_cluster_ids — historically the airline
         anchor exports)

    Returns the empty set when neither path produces ids.
    """
    raw = ag.get("source_cluster_ids") or []
    out = {str(c).strip() for c in raw if str(c).strip()}
    if out:
        return out
    patches = ag.get("patches") or []
    for p in patches:
        if not isinstance(p, Mapping):
            continue
        cid = str(p.get("cluster_id") or "").strip()
        if cid:
            out.add(cid)
    return out


def decide_slate_action(
    *,
    ag: Mapping[str, object],
    slate_admitted_ags: Sequence[Mapping[str, object]],
    admission_result: AdmissionResult,
    blocked_cluster_ids: Sequence[str],
) -> SlateDecision:
    """Decide whether ``ag`` proceeds, is skipped, or triggers a pivot.

    Pure. No I/O. The caller (harness) emits decision records and
    advances iteration_counter based on the returned action.

    Precedence:
      1. ``pivot_signal`` AND this ``ag`` matches ``first_ag_retired_id``
         → PIVOT_ITERATION (the strategist should pick a different
         cluster next iteration).
      2. ``ag.id in admission_result.denied_ag_ids``
         → SKIP_AG ("ag_denied_by_admission_trace").
      3. ``ag.source_cluster_ids`` intersects ``blocked_cluster_ids``
         → SKIP_AG ("cluster_blocked_no_rca").
      4. Otherwise → PROCEED.
    """
    aid = _ag_id(ag)
    denied = {str(x) for x in (admission_result.denied_ag_ids or ())}
    blocked = {str(x) for x in (blocked_cluster_ids or ())}
    src = _ag_effective_source_clusters(ag)

    if (
        admission_result.pivot_signal
        and str(admission_result.first_ag_retired_id or "") == aid
    ):
        return SlateDecision(
            action=SlateAction.PIVOT_ITERATION,
            reason="ag_retired_pivot",
            denied_ag_id=aid,
        )

    if aid in denied:
        return SlateDecision(
            action=SlateAction.SKIP_AG,
            reason="ag_denied_by_admission_trace",
            denied_ag_id=aid,
        )

    if src & blocked:
        return SlateDecision(
            action=SlateAction.SKIP_AG,
            reason="cluster_blocked_no_rca",
            denied_ag_id=aid,
        )

    return SlateDecision(
        action=SlateAction.PROCEED,
        reason="",
        denied_ag_id="",
    )
