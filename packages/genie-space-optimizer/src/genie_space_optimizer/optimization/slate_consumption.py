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
    src = {str(x) for x in (ag.get("source_cluster_ids") or ())}

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
