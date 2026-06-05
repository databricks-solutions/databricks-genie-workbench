"""Pin stdout-first question-journey rendering on failed AG paths."""

from __future__ import annotations

from _harness_loop_source import lever_loop_source
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
    src = lever_loop_source()
    rollback_idx = src.index('if not gate_result.get("passed")')
    # Window expanded from 900 → 1200 chars to absorb the +4-space
    # iteration body indentation introduced by the Bug B fix's
    # ``try/finally`` wrap of the iteration loop body. Substring
    # contract is unchanged.
    window = src[rollback_idx: rollback_idx + 1200]
    assert "_render_current_journey()" in window, (
        "Rollback path must render the journey ledger before rollback reflection "
        "bookkeeping can continue to the next AG."
    )


def test_run_lever_loop_has_idempotent_journey_render_state() -> None:
    src = lever_loop_source()
    assert "_journey_render_state" in src
    assert "render_question_journey_once" in src
