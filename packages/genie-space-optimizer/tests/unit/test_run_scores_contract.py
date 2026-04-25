"""Unit tests for ``compute_run_scores`` — the canonical baseline + optimized
pair that every UI surface must consume.

Locks down the regression where the UI's "Optimized" headline reported 100%
during Baseline Evaluation (step 2/6) because the workbench's
``/runs/{id}/status`` endpoint:

  1. Took ``max(...)`` over slice/p0/held-out probes (which routinely hit
     100% on a tiny subset).
  2. Did NOT filter rolled-back iterations.
  3. Defaulted ``optimized_score = baseline_score`` when no iterations
     existed, falsely implying optimization had completed.

The new contract pulls these three concerns into one place. Don't duplicate
the loop — call ``compute_run_scores``.
"""

from __future__ import annotations

from genie_space_optimizer.common.accuracy import RunScores, compute_run_scores


def _row(
    iteration: int,
    *,
    overall_accuracy: float,
    correct_count: int,
    evaluated_count: int,
    rolled_back: bool = False,
    eval_scope: str = "full",
) -> dict:
    return {
        "iteration": iteration,
        "eval_scope": eval_scope,
        "overall_accuracy": overall_accuracy,
        "correct_count": correct_count,
        "evaluated_count": evaluated_count,
        "rolled_back": rolled_back,
    }


# ---------------------------------------------------------------------------
# Empty / mid-run states
# ---------------------------------------------------------------------------


def test_empty_iter_rows_returns_all_none() -> None:
    """No iterations at all (run is mid-preflight) — every field is None."""
    assert compute_run_scores([]) == RunScores(None, None, None, None)
    assert compute_run_scores(None) == RunScores(None, None, None, None)


def test_no_full_scope_rows_returns_all_none() -> None:
    """Slice/p0 probes alone don't constitute a baseline — UI shows '—'."""
    rows = [
        _row(0, overall_accuracy=100.0, correct_count=2, evaluated_count=2,
             eval_scope="slice"),
        _row(1, overall_accuracy=100.0, correct_count=3, evaluated_count=3,
             eval_scope="p0"),
    ]
    assert compute_run_scores(rows) == RunScores(None, None, None, None)


def test_baseline_only_no_iter_gt_zero_yet() -> None:
    """Baseline Evaluation finished, no iter > 0 yet.

    The screenshot bug. Pre-fix the workbench reported optimized=100% after
    seeing a 100% slice probe row. Post-fix:
      * baseline = 80
      * optimized = baseline (floor)
      * best_iteration = 0 (frontend renders '—' + 'Optimization in
        progress' tooltip while running, 'Baseline retained' once terminal)
    """
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
    ]
    scores = compute_run_scores(rows)
    assert scores == RunScores(
        baseline=80.0, optimized=80.0,
        baseline_iteration=0, best_iteration=0,
    )


def test_baseline_only_with_noisy_slice_probe_does_not_inflate_optimized() -> None:
    """The exact screenshot bug: 100% slice probe alongside an 80% baseline.

    The slice row MUST NOT win. Optimized stays at baseline.
    """
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=100.0, correct_count=2, evaluated_count=2,
             eval_scope="slice"),
        _row(1, overall_accuracy=100.0, correct_count=3, evaluated_count=3,
             eval_scope="p0"),
        _row(1, overall_accuracy=100.0, correct_count=5, evaluated_count=5,
             eval_scope="held_out"),
    ]
    scores = compute_run_scores(rows)
    assert scores.baseline == 80.0
    assert scores.optimized == 80.0
    assert scores.best_iteration == 0


# ---------------------------------------------------------------------------
# Acceptance / rollback semantics
# ---------------------------------------------------------------------------


def test_accepted_iteration_strictly_above_baseline_wins() -> None:
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=90.0, correct_count=18, evaluated_count=20),
    ]
    scores = compute_run_scores(rows)
    assert scores == RunScores(
        baseline=80.0, optimized=90.0,
        baseline_iteration=0, best_iteration=1,
    )


def test_rolled_back_iteration_with_higher_accuracy_ignored() -> None:
    """Detect-regressions said no — the iteration is not deployed.

    It MUST NOT contribute to the Optimized headline either.
    """
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=95.0, correct_count=19, evaluated_count=20,
             rolled_back=True),
    ]
    scores = compute_run_scores(rows)
    assert scores.baseline == 80.0
    assert scores.optimized == 80.0
    assert scores.best_iteration == 0  # baseline retained


def test_iter_below_baseline_does_not_pull_optimized_down() -> None:
    """An accepted iter that ends up under baseline (shouldn't happen, but
    defensive) MUST NOT pull the headline below baseline.

    Floor-at-baseline invariant. This is the customer-visible constraint:
    "We shouldn't show regression here — cos regressions don't get posted."
    """
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=70.0, correct_count=14, evaluated_count=20),
    ]
    scores = compute_run_scores(rows)
    assert scores.baseline == 80.0
    assert scores.optimized == 80.0
    assert scores.best_iteration == 0  # baseline retained, not iter 1


def test_mixed_history_picks_highest_accepted_iter() -> None:
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=85.0, correct_count=17, evaluated_count=20),
        _row(2, overall_accuracy=92.0, correct_count=23, evaluated_count=25),
        _row(3, overall_accuracy=99.0, correct_count=24, evaluated_count=25,
             rolled_back=True),
    ]
    scores = compute_run_scores(rows)
    assert scores.baseline == 80.0
    assert scores.optimized == 92.0
    assert scores.best_iteration == 2


def test_tied_with_baseline_credits_baseline() -> None:
    """An iteration that exactly matches baseline doesn't get the credit.

    No improvement ⇒ baseline retained. UI shows 'Baseline retained'.
    """
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
    ]
    scores = compute_run_scores(rows)
    assert scores.optimized == 80.0
    assert scores.best_iteration == 0


def test_two_accepted_iterations_tie_picks_earliest() -> None:
    """Tie-break on lowest iteration number.

    Matches the existing ``promote_best_model`` behavior — the earliest
    accepted plateau gets the credit so the patch trail is shorter.
    """
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=90.0, correct_count=18, evaluated_count=20),
        _row(2, overall_accuracy=90.0, correct_count=18, evaluated_count=20),
    ]
    scores = compute_run_scores(rows)
    assert scores.optimized == 90.0
    assert scores.best_iteration == 1


# ---------------------------------------------------------------------------
# Defensive / scope filtering
# ---------------------------------------------------------------------------


def test_only_full_scope_iter_gt_zero_rows_considered() -> None:
    """Slice/p0/held_out probes for iter > 0 must be ignored."""
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=99.0, correct_count=4, evaluated_count=4,
             eval_scope="slice"),
        _row(1, overall_accuracy=99.0, correct_count=5, evaluated_count=5,
             eval_scope="p0"),
        _row(1, overall_accuracy=85.0, correct_count=17, evaluated_count=20),
    ]
    scores = compute_run_scores(rows)
    assert scores.baseline == 80.0
    assert scores.optimized == 85.0
    assert scores.best_iteration == 1


def test_baseline_kept_even_if_mislabeled_rolled_back() -> None:
    """A bug-stamped ``rolled_back=true`` on iter 0 must not drop baseline.

    Baseline is the floor. If iteration 0 itself is gone, there's nothing
    to anchor against — but a stamped flag should not erase it.
    """
    rows = [
        _row(0, overall_accuracy=85.0, correct_count=17, evaluated_count=20,
             rolled_back=True),
        _row(1, overall_accuracy=70.0, correct_count=14, evaluated_count=20,
             rolled_back=True),
    ]
    scores = compute_run_scores(rows)
    assert scores.baseline == 85.0
    assert scores.optimized == 85.0
    assert scores.best_iteration == 0


def test_legacy_rows_without_rolled_back_column_participate() -> None:
    """Pre-Tier-1.1-migration rows have no ``rolled_back`` column.

    They must participate in max() — otherwise the fix would erase historical
    dashboards.
    """
    rows = [
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 80.0,
         "correct_count": 16, "evaluated_count": 20},
        {"iteration": 1, "eval_scope": "full", "overall_accuracy": 90.0,
         "correct_count": 18, "evaluated_count": 20},
    ]
    scores = compute_run_scores(rows)
    assert scores.baseline == 80.0
    assert scores.optimized == 90.0
    assert scores.best_iteration == 1


def test_eval_scope_default_is_full_when_missing() -> None:
    """Rows without an ``eval_scope`` column are treated as full-scope.

    Defensive: legacy rows or freshly-written rows mid-migration must not
    silently disappear from the headline.
    """
    rows = [
        {"iteration": 0, "overall_accuracy": 80.0,
         "correct_count": 16, "evaluated_count": 20},
        {"iteration": 1, "overall_accuracy": 90.0,
         "correct_count": 18, "evaluated_count": 20},
    ]
    scores = compute_run_scores(rows)
    assert scores.baseline == 80.0
    assert scores.optimized == 90.0


# ---------------------------------------------------------------------------
# The headline invariant (the customer-visible contract)
# ---------------------------------------------------------------------------


def test_optimized_is_never_below_baseline_invariant() -> None:
    """For every legal mix of rows: ``optimized >= baseline``.

    This is the user's words verbatim: "we shouldn't show regression here
    — cos regressions don't get posted. So they should either stay as
    baseline or an improvement."
    """
    matrices = [
        # Baseline + accepted improvement
        [_row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
         _row(1, overall_accuracy=90.0, correct_count=18, evaluated_count=20)],
        # Baseline + rejected regression
        [_row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
         _row(1, overall_accuracy=70.0, correct_count=14, evaluated_count=20,
              rolled_back=True)],
        # Baseline + accepted regression (defensive)
        [_row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
         _row(1, overall_accuracy=70.0, correct_count=14, evaluated_count=20)],
        # Baseline + slice noise
        [_row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
         _row(1, overall_accuracy=100.0, correct_count=2, evaluated_count=2,
              eval_scope="slice")],
        # Baseline + tie
        [_row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
         _row(1, overall_accuracy=80.0, correct_count=16, evaluated_count=20)],
    ]
    for rows in matrices:
        scores = compute_run_scores(rows)
        assert scores.baseline is not None
        assert scores.optimized is not None
        assert scores.optimized >= scores.baseline, (
            f"Floor-at-baseline broken by rows={rows} → scores={scores}"
        )
