"""Trial 18 Step 5 — Plan 12 pivot ordering observability.

Sibling of ``plan12_ag_pivot_decided_marker`` in
``run_analysis_contract``. The DECIDED marker is emitted eagerly at
strategist-planning time when an AG is mutated by the pivot policy
(``pivot_applied=True``). The SKIPPED marker, emitted by the helper
below, fires at the iteration boundary when a pivoted AG never
reached the lever loop (e.g. an earlier AG terminated and the
buffered pivot was discarded by abort-ordering).

The split is **honesty-not-execution**:

  * The DECIDED marker stays as today — it accurately records the
    planning-time decision (the AG WAS mutated).
  * The SKIPPED marker tells postmortem readers "...but it never
    ran", so a pivot reported by the DECIDED stream is not mistaken
    for tested evidence.

Postmortem skill (gso-postmortem) gains a
``PLAN12_PIVOT_SKIPPED_BY_ABORT_ORDERING`` guardrail keyed on the
join ``(run_id, iteration, ag_id)``: when both a DECIDED row with
``pivot_applied=True`` and a SKIPPED row exist for the same AG-id in
the same iteration, the pivot is reported as NOT tested.

Gated by ``GSO_TRIAL18_ACCEPTANCE_OVERHAUL`` so emergency rollback
restores pre-Trial-18 silence (the DECIDED stream stays unchanged
either way).
"""
from __future__ import annotations

from typing import Iterable, Mapping

from genie_space_optimizer.optimization.run_analysis_contract import (
    plan12_pivot_skipped_marker,
)
from genie_space_optimizer.optimization.trial18_flags import (
    trial18_acceptance_overhaul_enabled,
)


def emit_plan12_pivot_skipped_for_unexecuted(
    *,
    optimization_run_id: str,
    iteration: int,
    pivoted_ag_ids: Mapping[str, str],
    executed_ag_ids: Iterable[str],
) -> tuple[str, ...]:
    """Emit one ``GSO_PLAN12_PIVOT_SKIPPED_V1`` marker per pivoted AG
    that was not in ``executed_ag_ids``.

    Args:
      optimization_run_id, iteration: passthrough to the marker.
      pivoted_ag_ids: mapping ``{ag_id: cluster_id}`` for every AG
        whose DECIDED marker carried ``pivot_applied=True`` in this
        iteration. The harness tracks these alongside the existing
        DECIDED emission.
      executed_ag_ids: AG-ids that actually entered the lever loop
        in this iteration (i.e. an ``ag_consumed`` checkpoint
        equivalent). AGs in ``pivoted_ag_ids`` but absent from this
        set are reported as ``pivot_skipped_due_to_abort_ordering``.

    Returns the AG-id tuple in stable (sorted) order so postmortem
    replays are deterministic. Returns ``()`` when no markers fired
    (either the flag is off, the pivoted set is empty, or every
    pivoted AG executed).
    """
    if not trial18_acceptance_overhaul_enabled():
        return ()
    if not pivoted_ag_ids:
        return ()
    executed = {str(a) for a in executed_ag_ids or ()}
    unexecuted_ids = tuple(
        sorted(
            ag_id for ag_id in pivoted_ag_ids
            if str(ag_id) not in executed
        )
    )
    for ag_id in unexecuted_ids:
        print(
            plan12_pivot_skipped_marker(
                optimization_run_id=str(optimization_run_id or ""),
                iteration=int(iteration or 0),
                ag_id=str(ag_id),
                cluster_id=str(pivoted_ag_ids.get(ag_id, "") or ""),
                reason="pivot_skipped_due_to_abort_ordering",
            ),
            flush=True,
        )
    return unexecuted_ids


__all__ = ["emit_plan12_pivot_skipped_for_unexecuted"]
