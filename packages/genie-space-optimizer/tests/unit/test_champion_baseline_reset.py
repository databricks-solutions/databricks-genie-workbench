"""Champion selection honors a kept metric-view attach's re-baseline (MV-D114 d7).

The stored iteration-0 row keeps its pre-attach score (MV-D18); after a kept attach
the loop judged everything against the post-attach eval, so selection scores the
baseline row that eval was measured against at that value.
"""

from __future__ import annotations

import pandas as pd
from genie_space_optimizer.optimization import models
from genie_space_optimizer.optimization.champion import (
    BaselineReset,
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
