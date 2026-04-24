"""Unit tests for backend/routes/runs.py accuracy aggregation (Tier 1.2, 3.1).

Covers the regression where the UI's "OPTIMIZED" card displayed a rolled-back
iteration's accuracy instead of the current space state. The defect:
``_get_baseline_and_best_accuracy`` took ``max`` over every full-eval row in
``genie_opt_iterations``, including rolled-back iterations.

Tier 1.2 fix: exclude iterations with ``rolled_back == true`` from the
``scored`` pool, but always retain iteration 0 (baseline) as the floor.
"""

from __future__ import annotations

from genie_space_optimizer.backend.routes.runs import _get_baseline_and_best_accuracy


def _row(
    iteration: int,
    *,
    overall_accuracy: float,
    correct_count: int,
    evaluated_count: int,
    rolled_back: bool = False,
    eval_scope: str = "full",
) -> dict:
    """Build a Delta-shaped iteration row for tests."""
    return {
        "iteration": iteration,
        "eval_scope": eval_scope,
        "overall_accuracy": overall_accuracy,
        "correct_count": correct_count,
        "evaluated_count": evaluated_count,
        "rolled_back": rolled_back,
    }


def test_rolled_back_iterations_excluded_from_best():
    """When all lever iterations are rolled back, optimized == baseline.

    Mirrors the e9c0b491 run: baseline 100% (21/21), iter 1 rolled back at
    80% (16/20), iter 2 rolled back at 95% (19/20). The UI card must show
    100 / 100 / +0.0 — not 100 / 95 / -5.0.
    """
    rows = [
        _row(0, overall_accuracy=100.0, correct_count=21, evaluated_count=21),
        _row(1, overall_accuracy=80.0, correct_count=16, evaluated_count=20, rolled_back=True),
        _row(2, overall_accuracy=95.0, correct_count=19, evaluated_count=20, rolled_back=True),
    ]
    baseline, best = _get_baseline_and_best_accuracy(rows)
    assert baseline == 100.0
    assert best == 100.0


def test_accepted_iteration_wins_over_baseline():
    """When an iteration is accepted (not rolled back), it counts in max().

    Baseline 80%, iter 1 accepted at 90%. Optimised should be 90%.
    """
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=90.0, correct_count=18, evaluated_count=20, rolled_back=False),
    ]
    baseline, best = _get_baseline_and_best_accuracy(rows)
    assert baseline == 80.0
    assert best == 90.0


def test_rolled_back_iteration_with_higher_accuracy_ignored():
    """A rolled-back iteration that happened to eval higher must not win.

    Baseline 80%, iter 1 evaluated at 95% but rolled back (regression on a
    judge that detect_regressions caught). Optimised stays at baseline.
    """
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=95.0, correct_count=19, evaluated_count=20, rolled_back=True),
    ]
    baseline, best = _get_baseline_and_best_accuracy(rows)
    assert baseline == 80.0
    assert best == 80.0


def test_mixed_accepted_and_rolled_back():
    """Mixed history: iter 1 accepted (+5), iter 2 rolled back.

    Best should track iter 1's accuracy (the last accepted state).
    """
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=85.0, correct_count=17, evaluated_count=20, rolled_back=False),
        _row(2, overall_accuracy=75.0, correct_count=15, evaluated_count=20, rolled_back=True),
    ]
    baseline, best = _get_baseline_and_best_accuracy(rows)
    assert baseline == 80.0
    assert best == 85.0


def test_baseline_always_kept_even_if_mislabeled():
    """Iteration 0 never gets rolled back — if somehow stamped, still kept.

    Defensive: baseline is the floor. Even if a bug flipped rolled_back=true
    on iter 0, the function should not drop it (filter keeps iteration==0
    unconditionally).
    """
    rows = [
        _row(0, overall_accuracy=100.0, correct_count=21, evaluated_count=21, rolled_back=True),
        _row(1, overall_accuracy=80.0, correct_count=16, evaluated_count=20, rolled_back=True),
    ]
    baseline, best = _get_baseline_and_best_accuracy(rows)
    assert baseline == 100.0
    # All non-baseline iterations rolled back; best falls back to baseline.
    assert best == 100.0


def test_only_full_scope_rows_considered():
    """Non-full scopes (slice, p0, held_out) must not participate in max()."""
    rows = [
        _row(0, overall_accuracy=80.0, correct_count=16, evaluated_count=20),
        _row(1, overall_accuracy=95.0, correct_count=19, evaluated_count=20, eval_scope="slice"),
        _row(1, overall_accuracy=99.0, correct_count=19, evaluated_count=20, eval_scope="p0"),
        _row(1, overall_accuracy=85.0, correct_count=17, evaluated_count=20, eval_scope="full"),
    ]
    baseline, best = _get_baseline_and_best_accuracy(rows)
    assert baseline == 80.0
    assert best == 85.0


def test_missing_rolled_back_column_treated_as_not_rolled_back():
    """Legacy rows written before the Tier 1.1 migration have no column.

    They must participate in max() — otherwise deploying the fix would
    break historical dashboards.
    """
    rows = [
        {
            "iteration": 0, "eval_scope": "full",
            "overall_accuracy": 80.0, "correct_count": 16, "evaluated_count": 20,
        },
        {
            "iteration": 1, "eval_scope": "full",
            "overall_accuracy": 90.0, "correct_count": 18, "evaluated_count": 20,
        },
    ]
    baseline, best = _get_baseline_and_best_accuracy(rows)
    assert baseline == 80.0
    assert best == 90.0
