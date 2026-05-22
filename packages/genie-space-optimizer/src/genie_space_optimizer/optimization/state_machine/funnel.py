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
