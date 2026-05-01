"""Pin that the iteration-end validator warns but does not raise."""

from __future__ import annotations

import logging

from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
)


def test_validate_journeys_at_iteration_end_logs_warning_for_violations(
    caplog,
) -> None:
    from genie_space_optimizer.optimization.harness import (
        _validate_journeys_at_iteration_end,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        # missing post_eval, no terminal
    ]

    with caplog.at_level(logging.WARNING):
        _validate_journeys_at_iteration_end(
            events=events,
            eval_qids=["gs_001"],
            iteration=2,
            raise_on_violation=False,
        )

    msgs = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("contract violations" in m for m in msgs)


def test_validate_journeys_does_not_warn_when_clean(caplog) -> None:
    from genie_space_optimizer.optimization.harness import (
        _validate_journeys_at_iteration_end,
    )

    events = [
        QuestionJourneyEvent(question_id="gs_001", stage="evaluated"),
        QuestionJourneyEvent(
            question_id="gs_001", stage="already_passing",
        ),
        QuestionJourneyEvent(
            question_id="gs_001", stage="post_eval",
            was_passing=True, is_passing=True, transition="hold_pass",
        ),
    ]

    with caplog.at_level(logging.WARNING):
        _validate_journeys_at_iteration_end(
            events=events,
            eval_qids=["gs_001"],
            iteration=1,
            raise_on_violation=False,
        )

    assert not any(
        "contract violations" in r.message for r in caplog.records
    )
