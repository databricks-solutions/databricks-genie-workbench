"""Cover the per-iteration producer that fills `journey_validation` in the
fixture builder. The producer lives at ``harness.py:17542-17555``; we
exercise the same composition without spinning up the full harness.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _validate_journeys_at_iteration_end,
)
from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)


def test_iteration_end_validator_returns_populated_report():
    events = [
        QuestionJourneyEvent(question_id="q1", stage="evaluated"),
        QuestionJourneyEvent(question_id="q1", stage="already_passing"),
        QuestionJourneyEvent(
            question_id="q1",
            stage="post_eval",
            was_passing=True,
            is_passing=True,
            transition="hold_pass",
        ),
    ]
    report = _validate_journeys_at_iteration_end(
        events=events,
        eval_qids=["q1"],
        iteration=1,
        raise_on_violation=False,
    )
    assert report is not None
    payload = report.to_dict()
    assert payload["is_valid"] is True
    assert payload["violations"] == []
    assert payload["terminal_state_by_qid"]["q1"] == "already_passing"


def test_iteration_end_validator_flags_missing_qid():
    report = _validate_journeys_at_iteration_end(
        events=[],
        eval_qids=["q1"],
        iteration=1,
        raise_on_violation=False,
    )
    assert report is not None
    payload = report.to_dict()
    assert payload["is_valid"] is False
    assert "q1" in payload["missing_qids"]


def test_iteration_end_validator_handles_lane_aware_input():
    """Smoke: a multi-proposal-lane qid resolves to is_valid=True post-PR-C."""
    events = [
        QuestionJourneyEvent(question_id="q1", stage="evaluated"),
        QuestionJourneyEvent(question_id="q1", stage="clustered", cluster_id="C1"),
        QuestionJourneyEvent(question_id="q1", stage="ag_assigned", ag_id="AG_1"),
        QuestionJourneyEvent(question_id="q1", stage="proposed", proposal_id="P_A"),
        QuestionJourneyEvent(question_id="q1", stage="proposed", proposal_id="P_B"),
        QuestionJourneyEvent(question_id="q1", stage="applied", proposal_id="P_A"),
        QuestionJourneyEvent(
            question_id="q1", stage="dropped_at_grounding", proposal_id="P_B"
        ),
        QuestionJourneyEvent(question_id="q1", stage="rolled_back", ag_id="AG_1"),
        QuestionJourneyEvent(
            question_id="q1",
            stage="post_eval",
            was_passing=False,
            is_passing=False,
            transition="hold_fail",
        ),
    ]
    report = _validate_journeys_at_iteration_end(
        events=events,
        eval_qids=["q1"],
        iteration=2,
        raise_on_violation=False,
    )
    assert report is not None
    assert report.to_dict()["is_valid"] is True
