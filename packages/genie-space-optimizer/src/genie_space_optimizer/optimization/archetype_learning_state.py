"""Phase 2 Action 2.5 — Run-scoped state for in-loop archetype learning.

State is held in-memory and keyed by ``run_id``. There is no persistence
in this plan: cross-run promotion reads the decision-record stream
(via ``CROSS_RUN_PROMOTION_CANDIDATE_RECORDED``) rather than this state.

The state is intentionally a thin holder — all logic (signature
computation, candidate detection, trial-outcome recording) lives in
``archetype_learning.py`` so this module stays trivially testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from genie_space_optimizer.optimization.archetype_learning import (
    PatternCandidate,
    ProvisionalArchetype,
    UnmatchedPatternRecord,
)


@dataclass
class ArchetypeLearningRunState:
    run_id: str
    unmatched_pattern_records: list[UnmatchedPatternRecord] = field(default_factory=list)
    pattern_candidates: list[PatternCandidate] = field(default_factory=list)
    provisional_archetypes: list[ProvisionalArchetype] = field(default_factory=list)
    synthesis_calls_this_iteration: int = 0
    trials_this_iteration: int = 0


_STATES: dict[str, ArchetypeLearningRunState] = {}


def get_state(run_id: str) -> ArchetypeLearningRunState:
    state = _STATES.get(run_id)
    if state is None:
        state = ArchetypeLearningRunState(run_id=run_id)
        _STATES[run_id] = state
    return state


def reset_state(run_id: str) -> None:
    _STATES[run_id] = ArchetypeLearningRunState(run_id=run_id)


def reset_iteration_counters(run_id: str) -> None:
    """Called at the start of each iteration. Resets only the
    per-iteration counters (synthesis calls, trials)."""
    state = get_state(run_id)
    state.synthesis_calls_this_iteration = 0
    state.trials_this_iteration = 0
