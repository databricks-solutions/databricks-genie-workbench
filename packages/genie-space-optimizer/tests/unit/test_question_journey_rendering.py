"""Pin stdout-first question-journey rendering on failed AG paths."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness
from genie_space_optimizer.optimization.question_journey import (
    QuestionJourneyEvent,
    render_question_journey_once,
)


def test_render_question_journey_once_prints_once(capsys) -> None:
    events = [
        QuestionJourneyEvent(
            question_id="gs_026",
            stage="clustered",
            cluster_id="H002",
            root_cause="plural_top_n_collapse",
        )
    ]
    state = {"rendered": False}

    assert render_question_journey_once(
        events=events,
        iteration=2,
        render_state=state,
    ) is True
    assert render_question_journey_once(
        events=events,
        iteration=2,
        render_state=state,
    ) is True

    out = capsys.readouterr().out
    assert out.count("QUESTION JOURNEY LEDGER") == 1
    assert "gs_026" in out


def test_render_question_journey_once_marks_rendered_when_empty(capsys) -> None:
    state = {"rendered": False}
    assert render_question_journey_once(
        events=[],
        iteration=3,
        render_state=state,
    ) is True
    assert state["rendered"] is True
    assert capsys.readouterr().out == ""


def test_run_lever_loop_calls_journey_render_before_rollback_continue() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    rollback_idx = src.index('if not gate_result.get("passed")')
    window = src[rollback_idx: rollback_idx + 900]
    assert "_render_current_journey()" in window, (
        "Rollback path must render the journey ledger before rollback reflection "
        "bookkeeping can continue to the next AG."
    )


def test_run_lever_loop_has_idempotent_journey_render_state() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    assert "_journey_render_state" in src
    assert "render_question_journey_once" in src
