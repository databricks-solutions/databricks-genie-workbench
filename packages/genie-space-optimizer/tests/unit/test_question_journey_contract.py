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


def test_validate_reports_missing_qids() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        validate_question_journeys,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(question_id="gs_001", stage="already_passing"),
        QuestionJourneyEvent(question_id="gs_001", stage="post_eval"),
    ]
    report = validate_question_journeys(
        events=events,
        eval_qids={"gs_001", "gs_002"},
    )

    assert report.is_valid is False
    assert "gs_002" in report.missing_qids
    assert "gs_001" not in report.missing_qids


def test_validate_reports_illegal_transition() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        validate_question_journeys,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(question_id="gs_001", stage="applied"),  # illegal
    ]
    report = validate_question_journeys(
        events=events,
        eval_qids={"gs_001"},
    )

    assert report.is_valid is False
    assert any(
        v.question_id == "gs_001" and v.kind == "illegal_transition"
        for v in report.violations
    )


def test_validate_reports_no_terminal_state() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        validate_question_journeys,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(question_id="gs_001", stage="clustered"),
        # no post_eval, no terminal
    ]
    report = validate_question_journeys(
        events=events,
        eval_qids={"gs_001"},
    )

    assert report.is_valid is False
    assert any(
        v.question_id == "gs_001" and v.kind == "no_terminal_state"
        for v in report.violations
    )


def test_validate_passes_for_complete_journey() -> None:
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )
    from genie_space_optimizer.optimization.question_journey_contract import (
        validate_question_journeys,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(question_id="gs_001", stage="clustered",
                             cluster_id="H001", root_cause="missing_filter"),
        QuestionJourneyEvent(question_id="gs_001", stage="ag_assigned",
                             ag_id="AG1"),
        QuestionJourneyEvent(question_id="gs_001", stage="proposed",
                             proposal_id="P1", patch_type="add_sql_snippet"),
        QuestionJourneyEvent(question_id="gs_001", stage="applied",
                             proposal_id="P1"),
        QuestionJourneyEvent(question_id="gs_001", stage="accepted"),
        QuestionJourneyEvent(question_id="gs_001", stage="post_eval",
                             was_passing=False, is_passing=True,
                             transition="fail_to_pass"),
    ]
    report = validate_question_journeys(
        events=events,
        eval_qids={"gs_001"},
    )

    assert report.is_valid is True
    assert report.violations == []
    assert report.terminal_state_by_qid["gs_001"].value == "hard_failure_resolved"
