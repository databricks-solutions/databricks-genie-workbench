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

These tests lock the fix at three layers:

* :func:`select_finalize_skip_scores` — the pure priority core: prefer
  the lever_loop (post-enrichment) scorecard, fall back to baseline only
  when no skip scores were published.
* :func:`resolve_finalize_skip_scores` — the wired resolver. On the happy
  path it returns the authoritative published task value; on a **repair**
  run where task values were lost (so ``lever_loop.scores`` fell back to
  the stale baseline *full* row), it re-reads the skip-aware Delta state
  row (``eval_scope IN ('full', 'enrichment')``, enrichment preferred) so
  a ``post_enrichment_meets_thresholds`` skip still resolves to the
  post-enrichment scorecard.
* :func:`_resolve_finalize_terminal_status` — the exact verdict
  ``_run_finalize`` applies. Wiring the correct scorecard through it must
  yield ``CONVERGED`` / ``threshold_met``, and the stale baseline
  scorecard must still reproduce the old ``STALLED`` verdict (proving the
  regression is score-source driven).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.common.config import DEFAULT_THRESHOLDS
from genie_space_optimizer.jobs._handoff import (
    HandoffSource,
    HandoffValue,
    resolve_finalize_skip_scores,
    select_finalize_skip_scores,
)
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


# ── Layer 3: wired skip-score resolver (incl. repair / Delta path) ────


def _scores_hv(value, source):
    """Build a ``lever_loop.scores`` HandoffValue for the resolver."""
    return HandoffValue(key="scores", value=value, source=source)


def _patch_state_row(scores_json):
    """Patch ``load_latest_state_iteration`` to return a Delta state row
    (or ``None``) with the given ``scores_json``."""
    row = None if scores_json is None else {"iteration": 0, "scores_json": scores_json}
    return patch(
        "genie_space_optimizer.jobs._handoff.load_latest_state_iteration",
        return_value=row,
    )


def test_resolver_authoritative_task_value_short_circuits_delta_read():
    """Happy path: when ``lever_loop.scores`` came from task values it is
    authoritative — the resolver returns it without touching Delta."""
    hv = _scores_hv(_POST_ENRICHMENT_SCORES, HandoffSource.TASK_VALUES)
    with patch(
        "genie_space_optimizer.jobs._handoff.load_latest_state_iteration",
    ) as mock_state:
        resolved = resolve_finalize_skip_scores(
            MagicMock(),
            run_id="r", catalog="c", schema="s",
            lever_loop_scores=hv,
            baseline_scores=_BASELINE_SCORES,
        )
    mock_state.assert_not_called()
    assert resolved == _POST_ENRICHMENT_SCORES


def test_resolver_repair_path_prefers_enrichment_state_over_stale_baseline():
    """Repair / Delta-fallback regression (the cross-review blocker):

    lever_loop task values were lost, so ``get_lever_loop_outputs``
    resolved ``lever_loop.scores`` from ``load_latest_full_iteration`` —
    the stale baseline *full* row (83.33%, below threshold). The skip-
    aware resolver must instead pick the enrichment state row (86.67%,
    above threshold) so finalize converges rather than stalls.
    """
    # ll["scores"] is the stale baseline full row, NOT from task values.
    stale_ll = _scores_hv(_BASELINE_SCORES, HandoffSource.DELTA_FALLBACK)
    with _patch_state_row(_POST_ENRICHMENT_SCORES):
        resolved = resolve_finalize_skip_scores(
            MagicMock(),
            run_id="r", catalog="c", schema="s",
            lever_loop_scores=stale_ll,
            baseline_scores=_BASELINE_SCORES,
        )
    assert resolved == _POST_ENRICHMENT_SCORES

    status, reason = _resolve_finalize_terminal_status(
        resolved, _SKIP_ITERATION_COUNTER, _MAX_ITERATIONS,
    )
    assert (status, reason) == ("CONVERGED", "threshold_met"), (
        "repair path must converge on the enrichment scorecard, not stall "
        "on the stale baseline full row"
    )


def test_resolver_repair_path_baseline_meets_thresholds_unchanged():
    """``baseline_meets_thresholds`` skip on a repair: enrichment was
    skipped so there is no enrichment row, and ``load_latest_state_iteration``
    returns the baseline full row. The resolver returns those baseline
    scores — the legacy skip case is preserved."""
    baseline_above = {"result_correctness": 90.0, "accuracy": 90.0}
    stale_ll = _scores_hv(None, HandoffSource.MISSING)
    with _patch_state_row(baseline_above):
        resolved = resolve_finalize_skip_scores(
            MagicMock(),
            run_id="r", catalog="c", schema="s",
            lever_loop_scores=stale_ll,
            baseline_scores=baseline_above,
        )
    assert resolved == baseline_above


def test_resolver_repair_path_falls_back_to_baseline_when_state_empty():
    """Last resort: task values lost AND no Delta state row at all → fall
    back to the baseline scorecard rather than an empty dict."""
    stale_ll = _scores_hv(None, HandoffSource.MISSING)
    with _patch_state_row(None):
        resolved = resolve_finalize_skip_scores(
            MagicMock(),
            run_id="r", catalog="c", schema="s",
            lever_loop_scores=stale_ll,
            baseline_scores=_BASELINE_SCORES,
        )
    assert resolved == _BASELINE_SCORES


def test_resolver_empty_authoritative_value_falls_through_to_state_read():
    """Defensive: a TASK_VALUES source carrying an empty scorecard must not
    short-circuit — the resolver falls through to the skip-aware state
    read so an empty published value never wins over real Delta scores."""
    empty_auth = _scores_hv({}, HandoffSource.TASK_VALUES)
    with _patch_state_row(_POST_ENRICHMENT_SCORES):
        resolved = resolve_finalize_skip_scores(
            MagicMock(),
            run_id="r", catalog="c", schema="s",
            lever_loop_scores=empty_auth,
            baseline_scores=_BASELINE_SCORES,
        )
    assert resolved == _POST_ENRICHMENT_SCORES
