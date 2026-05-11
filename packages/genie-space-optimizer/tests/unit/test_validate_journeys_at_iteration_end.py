"""Pin the return contract of _validate_journeys_at_iteration_end."""
from __future__ import annotations


def test_validator_returns_report_on_clean_iteration() -> None:
    """When the iteration is clean, the helper must still return the report
    so the harness can persist it to the fixture and MLflow."""
    from genie_space_optimizer.optimization.harness import (
        _validate_journeys_at_iteration_end,
    )
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )

    events = [
        QuestionJourneyEvent(question_id="q1", stage="evaluated"),
        QuestionJourneyEvent(question_id="q1", stage="already_passing"),
        QuestionJourneyEvent(
            question_id="q1", stage="post_eval",
            was_passing=True, is_passing=True, transition="hold_pass",
        ),
    ]

    report = _validate_journeys_at_iteration_end(
        events=events,
        eval_qids=["q1"],
        iteration=1,
        raise_on_violation=False,
    )

    assert report is not None
    assert report.is_valid
    assert report.violations == []


def test_validator_returns_report_on_dirty_iteration_warn_only() -> None:
    """When raise_on_violation=False and the iteration is dirty, the helper
    must log warnings AND return the report.

    Cycle 17 T1: `evaluated → post_eval` is now legal
    ('iteration_terminal_with_acceptance'). Updated to use a still-illegal
    transition: `post_eval → evaluated` (going backward in the trunk).
    """
    from genie_space_optimizer.optimization.harness import (
        _validate_journeys_at_iteration_end,
    )
    from genie_space_optimizer.optimization.question_journey import (
        QuestionJourneyEvent,
    )

    # clustered -> soft_signal is still an illegal trunk transition (Cycle 17
    # T1 only legalizes evaluated -> post_eval and clustered -> already_passing;
    # clustered -> soft_signal remains illegal without the T2 producer fix).
    events = [
        QuestionJourneyEvent(question_id="q1", stage="evaluated"),
        QuestionJourneyEvent(question_id="q1", stage="clustered", cluster_id="H1"),
        QuestionJourneyEvent(question_id="q1", stage="soft_signal"),
        QuestionJourneyEvent(
            question_id="q1", stage="post_eval",
            was_passing=False, is_passing=False, transition="hold_fail",
        ),
    ]

    report = _validate_journeys_at_iteration_end(
        events=events,
        eval_qids=["q1"],
        iteration=1,
        raise_on_violation=False,
    )

    assert report is not None
    assert not report.is_valid
    assert any(v.kind == "illegal_transition" for v in report.violations)
