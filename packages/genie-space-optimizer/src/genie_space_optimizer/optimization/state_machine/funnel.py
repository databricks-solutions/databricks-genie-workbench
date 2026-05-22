"""FunnelStage: typed enumeration of optimizer pipeline stages.

The funnel is forward-only: state may advance to the next stage, cycle
back to PROPOSED for escalation, or terminate. It may not skip forward.
See design document section 3.
"""
from __future__ import annotations

from enum import StrEnum


class FunnelStage(StrEnum):
    HARD_QID_SEEN = "hard_qid_seen"
    DIAGNOSED = "diagnosed"
    CLUSTERED = "clustered"
    PROPOSED = "proposed"
    NORMALIZED = "normalized"
    APPLYABLE = "applyable"
    APPLIED = "applied"
    EVALUATED = "evaluated"
    ACCEPTED = "accepted"
    TERMINATED = "terminated"


_STAGE_ORDER: tuple[FunnelStage, ...] = (
    FunnelStage.HARD_QID_SEEN,
    FunnelStage.DIAGNOSED,
    FunnelStage.CLUSTERED,
    FunnelStage.PROPOSED,
    FunnelStage.NORMALIZED,
    FunnelStage.APPLYABLE,
    FunnelStage.APPLIED,
    FunnelStage.EVALUATED,
    FunnelStage.ACCEPTED,
    FunnelStage.TERMINATED,
)


def stage_index(stage: FunnelStage) -> int:
    return _STAGE_ORDER.index(stage)


def is_terminal(stage: FunnelStage) -> bool:
    return stage == FunnelStage.TERMINATED


# Stages from which backward-to-PROPOSED is a legal escalation cycle.
_ESCALATION_REJECTION_STAGES: frozenset[FunnelStage] = frozenset(
    {FunnelStage.NORMALIZED, FunnelStage.APPLYABLE}
)


def is_legal_transition(from_stage: FunnelStage, to_stage: FunnelStage) -> bool:
    """Return True iff the funnel allows ``from_stage -> to_stage``.

    Legal transitions:
      - Forward by exactly one stage in ``_STAGE_ORDER``.
      - Any non-terminal stage may transition to ``TERMINATED``.
      - From ``NORMALIZED`` or ``APPLYABLE`` back to ``PROPOSED`` (escalation cycle).
      - From any non-terminal stage to itself — "decoration gates" that
        enrich an existing record (e.g. Plan 12 routing gate at
        CLUSTERED rewrites ``effective_target_lever`` on the cluster
        record without advancing the funnel).

    Everything else is illegal.
    """
    if from_stage == FunnelStage.TERMINATED:
        return False
    if to_stage == FunnelStage.TERMINATED:
        return True
    if to_stage == FunnelStage.PROPOSED and from_stage in _ESCALATION_REJECTION_STAGES:
        return True
    if from_stage == to_stage:  # decoration gates (e.g., routing at CLUSTERED)
        return True
    return stage_index(to_stage) == stage_index(from_stage) + 1
