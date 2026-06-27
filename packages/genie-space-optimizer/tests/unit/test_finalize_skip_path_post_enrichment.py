"""Regression: finalize skip path must use the post-enrichment scorecard.

Production divergence:

* Baseline eval reported 83.33% accuracy — **below** the 85% threshold.
* Proactive enrichment (Task 3) raised accuracy to 86.67% — **above** the
  threshold — and published ``post_enrichment_thresholds_met=True``.
* The lever loop (Task 4) skipped with
  ``SKIPPED: post_enrichment_meets_thresholds`` and published the
  *resolved* post-enrichment scorecard into the ``lever_loop.scores``
  task value.
* Finalize (Task 5), on ``lever_skipped=True``, read ``baseline_eval``'s
  *stale* 83.33% scorecard instead of the lever_loop scorecard. The
  terminal-status verdict then evaluated thresholds against 83.33% (below
  target) with ``iteration_counter=0`` and finalized the run as
  ``{"status": "STALLED", "convergence_reason": "no_further_improvement"}``
  even though the live Genie Space was genuinely above threshold.

These tests lock the fix at both layers:

* :func:`select_finalize_skip_scores` — the skip path prefers the
  lever_loop (post-enrichment) scorecard, falling back to baseline only
  when no skip scores were published.
* :func:`_resolve_finalize_terminal_status` — the exact verdict
  ``_run_finalize`` applies. Wiring the correct scorecard through it must
  yield ``CONVERGED`` / ``threshold_met``, and the stale baseline
  scorecard must still reproduce the old ``STALLED`` verdict (proving the
  regression is score-source driven).
"""

from __future__ import annotations

from genie_space_optimizer.common.config import DEFAULT_THRESHOLDS
from genie_space_optimizer.jobs._handoff import select_finalize_skip_scores
from genie_space_optimizer.optimization.harness import (
    _resolve_finalize_terminal_status,
)

# The production scenario from the bug report. ``result_correctness`` is the
# accuracy carrier key that ``all_thresholds_met`` gates on (target 85.0).
_BASELINE_SCORES = {"result_correctness": 83.33, "accuracy": 83.33}
_POST_ENRICHMENT_SCORES = {"result_correctness": 86.67, "accuracy": 86.67}
_SKIP_ITERATION_COUNTER = 0
_MAX_ITERATIONS = 6


def test_threshold_fixture_brackets_target():
    """Guard: the fixture must straddle the gate so the test is meaningful."""
    target = DEFAULT_THRESHOLDS["result_correctness"]
    assert _BASELINE_SCORES["result_correctness"] < target
    assert _POST_ENRICHMENT_SCORES["result_correctness"] >= target


# ── Layer 1: skip-path score selection ────────────────────────────────


def test_skip_path_prefers_lever_loop_post_enrichment_scores():
    """When enrichment met thresholds, the lever_loop scorecard (post-
    enrichment) must win over the stale baseline scorecard."""
    resolved = select_finalize_skip_scores(
        lever_loop_scores=_POST_ENRICHMENT_SCORES,
        baseline_scores=_BASELINE_SCORES,
    )
    assert resolved == _POST_ENRICHMENT_SCORES


def test_skip_path_baseline_meets_thresholds_is_unchanged():
    """``baseline_meets_thresholds`` skips publish the baseline scorecard as
    ``lever_loop.scores`` — selection returns identical numbers, so the
    legacy skip case is preserved."""
    resolved = select_finalize_skip_scores(
        lever_loop_scores=_BASELINE_SCORES,
        baseline_scores=_BASELINE_SCORES,
    )
    assert resolved == _BASELINE_SCORES


def test_skip_path_falls_back_to_baseline_when_lever_loop_empty():
    """Recovery path: a skip that never published scores (lost task values,
    empty Delta) must fall back to the baseline scorecard rather than an
    empty dict."""
    assert (
        select_finalize_skip_scores(
            lever_loop_scores=None, baseline_scores=_BASELINE_SCORES
        )
        == _BASELINE_SCORES
    )
    assert (
        select_finalize_skip_scores(
            lever_loop_scores={}, baseline_scores=_BASELINE_SCORES
        )
        == _BASELINE_SCORES
    )


def test_skip_path_returns_empty_dict_when_both_missing():
    """Never returns ``None`` — finalize expects a dict to score against."""
    assert select_finalize_skip_scores(
        lever_loop_scores=None, baseline_scores=None
    ) == {}


# ── Layer 2: terminal-status verdict ──────────────────────────────────


def test_post_enrichment_scorecard_yields_converged():
    """The fixed wiring (lever_loop post-enrichment scorecard → verdict)
    must finalize as CONVERGED / threshold_met."""
    status, reason = _resolve_finalize_terminal_status(
        _POST_ENRICHMENT_SCORES, _SKIP_ITERATION_COUNTER, _MAX_ITERATIONS,
    )
    assert status == "CONVERGED"
    assert reason == "threshold_met"


def test_stale_baseline_scorecard_reproduces_old_stalled_verdict():
    """The pre-fix wiring (baseline scorecard → verdict) reproduces the
    reported STALLED / no_further_improvement bug — confirming the verdict
    flip is driven purely by the scorecard source."""
    status, reason = _resolve_finalize_terminal_status(
        _BASELINE_SCORES, _SKIP_ITERATION_COUNTER, _MAX_ITERATIONS,
    )
    assert status == "STALLED"
    assert reason == "no_further_improvement"


def test_end_to_end_skip_path_baseline_below_post_enrichment_above_converges():
    """End-to-end on the skip path: baseline below threshold + post-
    enrichment above threshold must yield CONVERGED.

    Routes the published scorecards through the real selection helper and
    the real verdict resolver — the two functions that together govern the
    finalize skip-path terminal status.
    """
    prev_scores = select_finalize_skip_scores(
        lever_loop_scores=_POST_ENRICHMENT_SCORES,
        baseline_scores=_BASELINE_SCORES,
    )
    status, reason = _resolve_finalize_terminal_status(
        prev_scores, _SKIP_ITERATION_COUNTER, _MAX_ITERATIONS,
    )
    assert (status, reason) == ("CONVERGED", "threshold_met"), (
        "post-enrichment skip path must converge, not stall on the stale "
        "baseline scorecard"
    )


def test_verdict_max_iterations_preserved_for_non_skip_path():
    """Non-skip behavior guard: a below-threshold scorecard with the
    iteration budget exhausted is MAX_ITERATIONS, not STALLED — the
    extraction must preserve the full verdict ladder."""
    status, reason = _resolve_finalize_terminal_status(
        _BASELINE_SCORES, _MAX_ITERATIONS, _MAX_ITERATIONS,
    )
    assert status == "MAX_ITERATIONS"
    assert reason == "max_iterations"


def test_verdict_perfect_accuracy_is_arbiter_objective_met():
    """A 100% scorecard converges via the post-arbiter objective branch,
    ahead of the plain threshold branch."""
    status, reason = _resolve_finalize_terminal_status(
        {"result_correctness": 100.0, "accuracy": 100.0},
        _SKIP_ITERATION_COUNTER,
        _MAX_ITERATIONS,
    )
    assert status == "CONVERGED"
    assert reason == "post_arbiter_objective_met"
