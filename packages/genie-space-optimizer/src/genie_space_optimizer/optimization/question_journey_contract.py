"""Typed contract for the question-journey event graph.

This module is the source of truth for which stages a Lever Loop iteration may
emit, which terminal states a question may end an iteration in, and which
stage-to-stage transitions are legal. The validator and canonical serializer
also live here.

Producers (mostly harness.py) keep using QuestionJourneyEvent from
question_journey.py; consumers use this module to validate that every
evaluated qid has a complete, legal journey.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)


class JourneyStage(str, Enum):
    """Every legal stage a question may pass through in one Lever Loop iteration."""

    EVALUATED = "evaluated"
    CLUSTERED = "clustered"
    SOFT_SIGNAL = "soft_signal"
    GT_CORRECTION_CANDIDATE = "gt_correction_candidate"
    INTENT_COLLISION_DETECTED = "intent_collision_detected"
    ALREADY_PASSING = "already_passing"
    DIAGNOSTIC_AG = "diagnostic_ag"
    AG_ASSIGNED = "ag_assigned"
    PROPOSED = "proposed"
    DROPPED_AT_GROUNDING = "dropped_at_grounding"
    DROPPED_AT_NORMALIZE = "dropped_at_normalize"
    DROPPED_AT_APPLYABILITY = "dropped_at_applyability"
    DROPPED_AT_ALIGNMENT = "dropped_at_alignment"
    DROPPED_AT_REFLECTION = "dropped_at_reflection"
    DROPPED_AT_CAP = "dropped_at_cap"
    APPLIED = "applied"
    ROLLED_BACK = "rolled_back"
    ACCEPTED = "accepted"
    ACCEPTED_WITH_REGRESSION_DEBT = "accepted_with_regression_debt"
    POST_EVAL = "post_eval"


class JourneyTerminalState(str, Enum):
    """Every legal end-of-iteration outcome for a question."""

    ALREADY_PASSING = "already_passing"
    HARD_FAILURE_RESOLVED = "hard_failure_resolved"
    HARD_FAILURE_UNRESOLVED = "hard_failure_unresolved"
    SOFT_SIGNAL_ONLY = "soft_signal_only"
    GT_CORRECTION_CANDIDATE = "gt_correction_candidate"
    TERMINAL_UNACTIONABLE = "terminal_unactionable"
    ROLLED_BACK_NO_PROGRESS = "rolled_back_no_progress"


_DROP_STAGES: frozenset[JourneyStage] = frozenset({
    JourneyStage.DROPPED_AT_GROUNDING,
    JourneyStage.DROPPED_AT_NORMALIZE,
    JourneyStage.DROPPED_AT_APPLYABILITY,
    JourneyStage.DROPPED_AT_ALIGNMENT,
    JourneyStage.DROPPED_AT_REFLECTION,
    JourneyStage.DROPPED_AT_CAP,
})


_LEGAL_NEXT: dict[JourneyStage, frozenset[JourneyStage]] = {
    JourneyStage.EVALUATED: frozenset({
        JourneyStage.CLUSTERED,
        JourneyStage.SOFT_SIGNAL,
        JourneyStage.GT_CORRECTION_CANDIDATE,
        JourneyStage.ALREADY_PASSING,
    }),
    JourneyStage.CLUSTERED: frozenset({
        JourneyStage.AG_ASSIGNED,
        JourneyStage.DIAGNOSTIC_AG,
        JourneyStage.INTENT_COLLISION_DETECTED,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.SOFT_SIGNAL: frozenset({
        JourneyStage.AG_ASSIGNED,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.GT_CORRECTION_CANDIDATE: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.INTENT_COLLISION_DETECTED: frozenset({
        JourneyStage.AG_ASSIGNED,
        JourneyStage.DIAGNOSTIC_AG,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.ALREADY_PASSING: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DIAGNOSTIC_AG: frozenset({
        JourneyStage.PROPOSED,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.AG_ASSIGNED: frozenset({
        JourneyStage.PROPOSED,
        JourneyStage.POST_EVAL,
    }),
    JourneyStage.PROPOSED: frozenset(_DROP_STAGES | {JourneyStage.APPLIED}),
    JourneyStage.DROPPED_AT_GROUNDING: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_NORMALIZE: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_APPLYABILITY: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_ALIGNMENT: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_REFLECTION: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.DROPPED_AT_CAP: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.APPLIED: frozenset({
        JourneyStage.ACCEPTED,
        JourneyStage.ACCEPTED_WITH_REGRESSION_DEBT,
        JourneyStage.ROLLED_BACK,
    }),
    JourneyStage.ACCEPTED: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.ACCEPTED_WITH_REGRESSION_DEBT: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.ROLLED_BACK: frozenset({JourneyStage.POST_EVAL}),
    JourneyStage.POST_EVAL: frozenset(),
}


def is_legal_next_stage(*, prev: JourneyStage, nxt: JourneyStage) -> bool:
    """Return True if a question may transition from prev to nxt in one iteration."""
    return nxt in _LEGAL_NEXT.get(prev, frozenset())
