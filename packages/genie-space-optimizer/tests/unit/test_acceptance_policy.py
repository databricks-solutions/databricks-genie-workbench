"""Unit tests for the single-criterion lever-loop acceptance gate.

These cases replay the retail_store_sales_analytics AG1 / AG2 numbers
that motivated the rewrite, plus a clean-pass and the boundary case
where ``delta == min_gain_pp`` exactly.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.acceptance_policy import (
    ACCEPTED,
    REJECTED_INSUFFICIENT_GAIN,
    REJECTED_REGRESSION,
    AcceptanceDecision,
    decide_acceptance,
)


def test_retail_ag2_regression_is_rejected() -> None:
    """AG2 in the retail run regressed by 4.6pp post-arbiter and was
    incorrectly accepted under the old K-of-N + variance-widened path.
    Under the new single-criterion gate it must reject with the
    ``rejected_regression`` reason.
    """
    decision = decide_acceptance(
        post_arbiter_candidate=67.4,
        post_arbiter_baseline=72.0,
        min_gain_pp=2.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == REJECTED_REGRESSION
    assert decision.delta_pp == pytest.approx(-4.6, abs=1e-9)
    assert decision.post_arbiter_candidate == pytest.approx(67.4, abs=1e-9)
    assert decision.post_arbiter_baseline == pytest.approx(72.0, abs=1e-9)
    assert decision.min_gain_pp == pytest.approx(2.0, abs=1e-9)


def test_retail_ag1_insufficient_gain_is_rejected() -> None:
    """AG1's 1.2pp post-arbiter improvement is below the 2.0pp floor.
    Must reject with ``rejected_insufficient_gain`` (not regression).
    """
    decision = decide_acceptance(
        post_arbiter_candidate=73.2,
        post_arbiter_baseline=72.0,
        min_gain_pp=2.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == REJECTED_INSUFFICIENT_GAIN
    assert decision.delta_pp == pytest.approx(1.2, abs=1e-9)


def test_clean_pass_is_accepted() -> None:
    """A 6pp post-arbiter improvement clears the 2.0pp floor."""
    decision = decide_acceptance(
        post_arbiter_candidate=78.0,
        post_arbiter_baseline=72.0,
        min_gain_pp=2.0,
    )

    assert decision.accepted is True
    assert decision.reason_code == ACCEPTED
    assert decision.delta_pp == pytest.approx(6.0, abs=1e-9)


def test_boundary_delta_equals_min_gain_is_accepted() -> None:
    """Exact equality (delta == min_gain_pp) accepts. The gate is
    inclusive on the lower bound so a candidate that lands precisely at
    ``baseline + min_gain_pp`` is treated as a real improvement.
    """
    decision = decide_acceptance(
        post_arbiter_candidate=74.0,
        post_arbiter_baseline=72.0,
        min_gain_pp=2.0,
    )

    assert decision.accepted is True
    assert decision.reason_code == ACCEPTED
    assert decision.delta_pp == pytest.approx(2.0, abs=1e-9)


def test_zero_delta_is_insufficient_gain_not_regression() -> None:
    """No change is not a regression — make sure the classification
    bucket is ``rejected_insufficient_gain``, not ``rejected_regression``.
    """
    decision = decide_acceptance(
        post_arbiter_candidate=72.0,
        post_arbiter_baseline=72.0,
        min_gain_pp=2.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == REJECTED_INSUFFICIENT_GAIN
    assert decision.delta_pp == pytest.approx(0.0, abs=1e-9)


def test_decision_is_frozen_dataclass() -> None:
    """``AcceptanceDecision`` is intentionally immutable so audit rows
    can be passed around without worrying about callers mutating the
    decision after the fact.
    """
    decision = decide_acceptance(
        post_arbiter_candidate=78.0,
        post_arbiter_baseline=72.0,
        min_gain_pp=2.0,
    )

    with pytest.raises((AttributeError, TypeError)):
        decision.accepted = False  # type: ignore[misc]


def test_zero_min_gain_rejects_zero_delta() -> None:
    """With ``min_gain_pp=0.0`` the gate still rejects a zero delta.

    Causal target-qid checks in ``control_plane.decide_control_plane_acceptance``
    are the anti-noise guard now; the post-arbiter gate requires strictly
    positive movement so a no-op iteration cannot be accepted.
    """
    decision = decide_acceptance(
        post_arbiter_candidate=72.0,
        post_arbiter_baseline=72.0,
        min_gain_pp=0.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == REJECTED_INSUFFICIENT_GAIN


def test_returns_acceptance_decision_instance() -> None:
    """Sanity: the public surface is the ``AcceptanceDecision`` dataclass."""
    decision = decide_acceptance(
        post_arbiter_candidate=78.0,
        post_arbiter_baseline=72.0,
        min_gain_pp=2.0,
    )

    assert isinstance(decision, AcceptanceDecision)


def test_decide_acceptance_accepts_any_positive_arbiter_gain() -> None:
    from genie_space_optimizer.optimization.acceptance_policy import (
        ACCEPTED,
        decide_acceptance,
    )

    decision = decide_acceptance(
        post_arbiter_candidate=96.0,
        post_arbiter_baseline=95.5,
        min_gain_pp=0.0,
    )

    assert decision.accepted is True
    assert decision.reason_code == ACCEPTED
    assert decision.delta_pp == 0.5


def test_arbiter_objective_is_complete_at_100_percent() -> None:
    from genie_space_optimizer.optimization.acceptance_policy import (
        arbiter_objective_complete,
    )

    assert arbiter_objective_complete(100.0) is True
    assert arbiter_objective_complete(99.99) is False
