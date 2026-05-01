"""Pin the typed contract for question-journey events."""

from __future__ import annotations

import pytest


def test_journey_stage_enum_covers_existing_stage_order() -> None:
    """Every stage in question_journey._STAGE_ORDER must have a JourneyStage member."""
    from genie_space_optimizer.optimization.question_journey import _STAGE_ORDER
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyStage,
    )

    legal = {s.value for s in JourneyStage}
    missing = [s for s in _STAGE_ORDER if s not in legal]
    assert not missing, (
        f"JourneyStage enum is missing stages emitted by the existing harness: {missing}. "
        "Update the enum in question_journey_contract.py."
    )


def test_terminal_state_enum_lists_seven_legal_outcomes() -> None:
    """The terminal-state enum is the closed set of allowed end-of-iteration outcomes."""
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyTerminalState,
    )

    expected = {
        "already_passing",
        "hard_failure_resolved",
        "hard_failure_unresolved",
        "soft_signal_only",
        "gt_correction_candidate",
        "terminal_unactionable",
        "rolled_back_no_progress",
    }
    actual = {s.value for s in JourneyTerminalState}
    assert actual == expected, (
        f"JourneyTerminalState drift: expected {expected}, got {actual}. "
        "Adding/removing terminal states requires updating validate_question_journeys."
    )


def test_legal_transitions_map_rejects_proposed_after_already_passing() -> None:
    """A passing question must not have a proposed event after the already_passing terminal."""
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyStage,
        is_legal_next_stage,
    )

    assert not is_legal_next_stage(
        prev=JourneyStage.ALREADY_PASSING,
        nxt=JourneyStage.PROPOSED,
    )


def test_legal_transitions_map_allows_clustered_after_evaluated() -> None:
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyStage,
        is_legal_next_stage,
    )

    assert is_legal_next_stage(
        prev=JourneyStage.EVALUATED,
        nxt=JourneyStage.CLUSTERED,
    )


def test_legal_transitions_map_allows_dropped_at_cap_after_proposed() -> None:
    from genie_space_optimizer.optimization.question_journey_contract import (
        JourneyStage,
        is_legal_next_stage,
    )

    assert is_legal_next_stage(
        prev=JourneyStage.PROPOSED,
        nxt=JourneyStage.DROPPED_AT_CAP,
    )
