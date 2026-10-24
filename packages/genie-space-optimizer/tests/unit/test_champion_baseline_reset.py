"""Champion selection honors a kept metric-view attach's re-baseline (MV-D114 d7).

The stored iteration-0 row keeps its pre-attach score (MV-D18); after a kept attach
the loop judged everything against the post-attach eval, so selection scores the
baseline row that eval was measured against at that value.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest
from genie_space_optimizer.optimization import models, mv_attach
from genie_space_optimizer.optimization.champion import (
    _KEPT_VERDICT,
    BaselineReset,
    apply_baseline_reset,
    baseline_reset_from_stage_rows,
    is_reset_row,
    select_champion_row,
)

BASELINE_EVAL = "eval-baseline-1"


def _row(iteration: int, accuracy: float, **over) -> dict:
    row = {
        "iteration": iteration, "eval_scope": "full", "rolled_back": False,
        "overall_accuracy": accuracy, "is_champion": False,
        "eval_run_id": BASELINE_EVAL if iteration == 0 else f"eval-lever-{iteration}",
    }
    row.update(over)
    return row


def test_the_baseline_row_is_scored_at_the_post_attach_accuracy() -> None:
    rows = [_row(0, 86.67)]

    champion = select_champion_row(rows, baseline_reset=BaselineReset(BASELINE_EVAL, 90.0))

    assert champion is not None
    assert (champion["iteration"], champion["overall_accuracy"]) == (0, 90.0)
    assert rows[0]["overall_accuracy"] == 86.67


def test_a_lever_accepted_above_a_lower_post_attach_baseline_beats_the_stale_baseline() -> None:
    """A kept attach can lower the suite; the stale pre-attach row must not win."""
    rows = [_row(0, 86.67), _row(1, 85.0, decision="accept")]

    champion = select_champion_row(rows, baseline_reset=BaselineReset(BASELINE_EVAL, 83.0))

    assert champion is not None
    assert champion["iteration"] == 1


def test_without_a_reset_selection_is_unchanged() -> None:
    rows = [_row(0, 86.67), _row(1, 85.0, decision="accept")]

    champion = select_champion_row(rows)

    assert champion is not None
    assert (champion["iteration"], champion["overall_accuracy"]) == (0, 86.67)


def test_a_restarts_own_baseline_row_keeps_its_score() -> None:
    """A restart whose baseline failed never re-ran the phase; its row is not reset."""
    rows = [_row(0, 86.67), _row(0, 0.0, eval_run_id="eval-baseline-restart")]

    champion = select_champion_row(rows, baseline_reset=BaselineReset(BASELINE_EVAL, 90.0))

    assert champion is not None
    assert (champion["eval_run_id"], champion["overall_accuracy"]) == (BASELINE_EVAL, 90.0)


def test_a_reset_naming_another_eval_changes_nothing() -> None:
    rows = [_row(0, 86.67)]

    champion = select_champion_row(rows, baseline_reset=BaselineReset("eval-other", 90.0))

    assert champion is not None
    assert champion["overall_accuracy"] == 86.67


def test_a_reset_without_an_eval_id_applies_to_the_baseline_row() -> None:
    rows = [_row(0, 86.67, eval_run_id=None)]

    champion = select_champion_row(rows, baseline_reset=BaselineReset("", 90.0))

    assert champion is not None
    assert champion["overall_accuracy"] == 90.0


def test_promote_best_model_stamps_the_post_attach_accuracy(monkeypatch) -> None:
    updates: list[dict] = []
    marked: list[int] = []
    monkeypatch.setattr(models, "load_run", lambda *a, **k: {"run_id": "run1"})
    monkeypatch.setattr(
        models, "load_iterations", lambda *a, **k: pd.DataFrame([_row(0, 86.67)]),
    )
    monkeypatch.setattr(
        models, "mark_champion_iteration",
        lambda spark, run_id, iteration, **k: marked.append(iteration),
    )
    monkeypatch.setattr(
        models, "update_run_status",
        lambda spark, run_id, catalog, schema, **k: updates.append(k),
    )

    promoted = models.promote_best_model(
        object(), "run1", "c", "s", baseline_reset=BaselineReset(BASELINE_EVAL, 90.0),
    )

    assert promoted == 0
    assert marked == [0]
    assert updates == [{"best_iteration": 0, "best_accuracy": 90.0}]


def _stage(verdict, *, accuracy=90.0, eval_id="eval-b", remaining=2, as_text=True):
    detail = {
        "phase": "mv_attach", "verdict": verdict, "post_attach_accuracy": accuracy,
        "baseline_eval_run_id": eval_id, "post_attach_remaining_failures": remaining,
    }
    return {"stage": "MV_ATTACH", "detail_json": json.dumps(detail) if as_text else detail}


def test_the_kept_verdict_is_the_attach_phases() -> None:
    assert _KEPT_VERDICT == mv_attach.VERDICT_ATTACHED


@pytest.mark.parametrize("as_text", [True, False])
def test_the_latest_kept_stage_row_is_the_reset(as_text) -> None:
    reset = baseline_reset_from_stage_rows([
        _stage("DETACHED", accuracy=70.0, as_text=as_text),
        _stage("ATTACHED", as_text=as_text),
    ])
    assert reset == ("eval-b", 90.0, 2)


def test_a_later_detached_row_cancels_the_reset() -> None:
    assert baseline_reset_from_stage_rows([_stage("ATTACHED"), _stage("DETACHED")]) is None


def test_other_stages_and_unparsable_rows_are_ignored() -> None:
    rows = [{"stage": "MV_ATTACH_RECONCILE", "detail_json": "{}"},
            {"stage": "MV_ATTACH", "detail_json": "not json"}, _stage("ATTACHED")]
    assert baseline_reset_from_stage_rows(rows).accuracy == 90.0


@pytest.mark.parametrize("remaining", [True, "2", None, -1])
def test_a_non_count_remaining_failures_is_dropped(remaining) -> None:
    reset = baseline_reset_from_stage_rows([_stage("ATTACHED", remaining=remaining)])
    assert reset.remaining_failures is None


def test_apply_baseline_reset_scores_only_the_measured_baseline_row() -> None:
    rows = [
        {"iteration": 0, "eval_scope": "full", "eval_run_id": "eval-b", "overall_accuracy": 86.67},
        {"iteration": 0, "eval_scope": "full", "eval_run_id": "eval-restart", "overall_accuracy": 80.0},
        {"iteration": 1, "eval_scope": "full", "eval_run_id": "eval-1", "overall_accuracy": 88.0},
    ]
    out = apply_baseline_reset(rows, BaselineReset("eval-b", 90.0))
    assert [r["overall_accuracy"] for r in out] == [90.0, 80.0, 88.0]
    assert rows[0]["overall_accuracy"] == 86.67
    assert is_reset_row(out[0], BaselineReset("eval-b", 90.0))
    assert apply_baseline_reset(rows, None) == rows
