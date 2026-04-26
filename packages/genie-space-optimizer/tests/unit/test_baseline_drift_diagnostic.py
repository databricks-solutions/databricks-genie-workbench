"""Unit tests for the post-hoc baseline drift diagnostic.

The drift diagnostic is the safety net under Option D: if the previous
iteration accepted on a "lucky" eval, the *current* iteration's post-
arbiter accuracy will land materially below the carried baseline that
was in force at the start of the previous iteration. We log a
``suspected_stale_baseline`` decision-audit row so an operator can
review — there is **no auto-rollback**.

These tests pin the pure-function semantics. The harness wires the
function into ``_run_gate_checks`` and writes the audit row; the
wiring is exercised in the integration harness test, not here.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.acceptance_policy import (
    SUSPECTED_STALE_BASELINE,
    BaselineDriftDiagnostic,
    decide_baseline_drift,
)


def test_no_diagnostic_on_first_iteration() -> None:
    """When there is no prior iteration, ``prev_iter_pre_accept_baseline``
    is ``None``. The diagnostic must not fire.
    """
    drift = decide_baseline_drift(
        post_arbiter_current=72.0,
        prev_iter_pre_accept_baseline=None,
        threshold_pp=4.0,
    )

    assert drift.triggered is False
    assert drift.reason_code is None
    assert drift.delta_pp is None
    assert drift.prev_iter_pre_accept_baseline is None


def test_drift_fires_when_post_arbiter_falls_below_threshold() -> None:
    """Iter N accepted at 78.0 (carried baseline at start of iter N was
    72.0). At iter N+1 we land at 67.0 — that is 5pp below the 72.0 we
    carried into iter N, exceeding the 4.0pp drift threshold.
    """
    drift = decide_baseline_drift(
        post_arbiter_current=67.0,
        prev_iter_pre_accept_baseline=72.0,
        threshold_pp=4.0,
    )

    assert drift.triggered is True
    assert drift.reason_code == SUSPECTED_STALE_BASELINE
    assert drift.delta_pp == pytest.approx(-5.0, abs=1e-9)
    assert drift.threshold_pp == pytest.approx(4.0, abs=1e-9)


def test_drift_silent_when_within_threshold() -> None:
    """A 2pp slip is below the 4pp threshold — no diagnostic."""
    drift = decide_baseline_drift(
        post_arbiter_current=70.0,
        prev_iter_pre_accept_baseline=72.0,
        threshold_pp=4.0,
    )

    assert drift.triggered is False
    assert drift.reason_code is None
    assert drift.delta_pp == pytest.approx(-2.0, abs=1e-9)


def test_drift_boundary_at_negative_threshold_triggers() -> None:
    """Exactly at ``-threshold_pp`` triggers the diagnostic. The check
    is inclusive on the lower bound so we don't quietly miss a clean
    -4.0pp drop when the threshold is 4.0.
    """
    drift = decide_baseline_drift(
        post_arbiter_current=68.0,
        prev_iter_pre_accept_baseline=72.0,
        threshold_pp=4.0,
    )

    assert drift.triggered is True
    assert drift.reason_code == SUSPECTED_STALE_BASELINE
    assert drift.delta_pp == pytest.approx(-4.0, abs=1e-9)


def test_drift_does_not_fire_on_improvement() -> None:
    """A post-arbiter increase from the prior baseline is fine — the
    drift diagnostic only fires on regressions.
    """
    drift = decide_baseline_drift(
        post_arbiter_current=80.0,
        prev_iter_pre_accept_baseline=72.0,
        threshold_pp=4.0,
    )

    assert drift.triggered is False
    assert drift.reason_code is None
    assert drift.delta_pp == pytest.approx(8.0, abs=1e-9)


def test_diagnostic_is_frozen_dataclass() -> None:
    drift = decide_baseline_drift(
        post_arbiter_current=80.0,
        prev_iter_pre_accept_baseline=72.0,
        threshold_pp=4.0,
    )

    assert isinstance(drift, BaselineDriftDiagnostic)
    with pytest.raises((AttributeError, TypeError)):
        drift.triggered = True  # type: ignore[misc]


def test_drift_returns_rounded_metrics() -> None:
    """Audit rows compare across iterations, so we round to 1 decimal
    on the way out — same convention as ``decide_acceptance``.
    """
    drift = decide_baseline_drift(
        post_arbiter_current=67.39,
        prev_iter_pre_accept_baseline=72.04,
        threshold_pp=4.0,
    )

    assert drift.post_arbiter_current == pytest.approx(67.4, abs=1e-9)
    assert drift.prev_iter_pre_accept_baseline == pytest.approx(
        72.0, abs=1e-9,
    )
    assert drift.delta_pp == pytest.approx(-4.7, abs=1e-9)
