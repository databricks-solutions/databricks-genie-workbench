"""Tests for Task 2: strict full-eval acceptance policy.

The decoded retail run accepted AG2 because pre-arbiter improved 50→68%
while post-arbiter dropped 87.5→82.9% — a 4.6pp regression that slipped
under the previous 5.0pp guardrail. These tests pin the contract that:

* Any confirmation run that violates the post-arbiter guardrail rejects
  the bundle, regardless of the average.
* The post-arbiter guardrail fires on raw post-arbiter even when the
  optimization objective is ``blended`` — a pre-arbiter-driven blend
  cannot mask a post-arbiter regression.
* Variance widening can only tighten the guardrail (protect against
  noise), never relax it (which would re-create the AG2 failure mode).
* Acceptance requires every confirmation run to clear the primary-gain
  floor — K-of-N strict, not averaging.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.acceptance_policy import (
    AcceptanceDecision,
    decide_full_eval_acceptance,
)


# ── AG2 rejection (the headline failure mode) ─────────────────────────


def test_rejects_when_any_confirmation_run_violates_post_arbiter_guardrail():
    """Replays the AG2 metrics from the retail run."""
    decision = decide_full_eval_acceptance(
        objective="pre_arbiter",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[65.2, 70.8],
        run_post_arbiter=[78.3, 87.5],
        min_primary_gain_pp=0.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=5.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "post_arbiter_guardrail"
    assert decision.primary_delta_pp == pytest.approx(18.0, abs=0.1)
    assert decision.secondary_delta_pp == pytest.approx(-4.6, abs=0.1)
    # min_run_post_arbiter is 78.3, which is 9.2pp below baseline 87.5,
    # well past the effective guardrail.
    assert decision.min_run_post_arbiter == pytest.approx(78.3, abs=0.1)


def test_blended_objective_still_fires_post_arbiter_guardrail_on_raw_post_arbiter():
    """Blended primary cannot mask a post-arbiter regression."""
    decision = decide_full_eval_acceptance(
        objective="blended",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[68.0, 68.0],
        run_post_arbiter=[82.9, 82.9],
        min_primary_gain_pp=0.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=5.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "post_arbiter_guardrail"


def test_post_arbiter_objective_uses_post_arbiter_as_primary():
    decision = decide_full_eval_acceptance(
        objective="post_arbiter",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[80.0, 80.0],
        run_post_arbiter=[82.9, 82.9],
        min_primary_gain_pp=0.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=5.0,
    )

    # Post-arbiter is below baseline → primary not improved AND guardrail trips.
    assert decision.accepted is False
    assert decision.reason_code in {
        "primary_not_improved_in_every_run",
        "post_arbiter_guardrail",
    }


# ── Variance widening composition ─────────────────────────────────────


def test_variance_widening_can_tighten_but_not_relax_guardrail():
    """Effective guardrail = min(max_post_arbiter_drop_pp, variance_widened_tol_pp).

    A 1.5pp drop must reject when variance is tame (1.0pp tolerance).
    """
    decision = decide_full_eval_acceptance(
        objective="pre_arbiter",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[66.0, 66.0],
        run_post_arbiter=[86.0, 86.0],
        min_primary_gain_pp=0.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=1.0,
    )

    assert decision.accepted is False
    assert decision.effective_guardrail_pp == pytest.approx(1.0)


def test_variance_widening_does_not_loosen_default_guardrail():
    """When variance is large, the default 2.0pp cap still applies."""
    decision = decide_full_eval_acceptance(
        objective="pre_arbiter",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[70.0, 70.0],
        run_post_arbiter=[85.0, 85.0],
        min_primary_gain_pp=0.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=10.0,
    )

    # Effective guardrail is min(2.0, 10.0) = 2.0; 2.5pp drop rejects.
    assert decision.accepted is False
    assert decision.effective_guardrail_pp == pytest.approx(2.0)


# ── K-of-N strict acceptance ─────────────────────────────────────────


def test_accepts_only_when_every_run_satisfies_primary_and_guardrail():
    decision = decide_full_eval_acceptance(
        objective="pre_arbiter",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[66.0, 67.0],
        run_post_arbiter=[86.0, 86.5],
        min_primary_gain_pp=1.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=5.0,
    )

    assert decision.accepted is True
    assert decision.reason_code == "accepted"


def test_one_bad_run_blocks_acceptance_even_with_great_average():
    """K-of-N, not averaging: one run below baseline kills the bundle."""
    decision = decide_full_eval_acceptance(
        objective="pre_arbiter",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[40.0, 80.0],   # avg 60.0 ≥ baseline+1.0, but min < baseline
        run_post_arbiter=[86.0, 86.0],
        min_primary_gain_pp=1.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=5.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "primary_not_improved_in_every_run"


def test_min_primary_gain_floor_enforced_per_run():
    """Without the per-run floor, a 0.1pp gain on a 1pp budget would pass."""
    decision = decide_full_eval_acceptance(
        objective="pre_arbiter",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[50.5, 51.0],   # +0.5 / +1.0 against floor of 1.0pp
        run_post_arbiter=[87.5, 87.5],
        min_primary_gain_pp=1.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=5.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "primary_not_improved_in_every_run"


# ── Defensive paths ────────────────────────────────────────────────


def test_rejects_when_no_confirmation_runs_provided():
    decision = decide_full_eval_acceptance(
        objective="pre_arbiter",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[],
        run_post_arbiter=[],
        min_primary_gain_pp=0.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=5.0,
    )

    assert decision.accepted is False
    assert decision.reason_code == "missing_confirmation_runs"


def test_decision_is_a_frozen_dataclass():
    decision = decide_full_eval_acceptance(
        objective="pre_arbiter",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=87.5,
        run_pre_arbiter=[60.0, 60.0],
        run_post_arbiter=[88.0, 88.0],
        min_primary_gain_pp=0.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=5.0,
    )

    assert isinstance(decision, AcceptanceDecision)
    with pytest.raises(Exception):
        decision.accepted = False  # type: ignore[misc]


def test_blended_uses_simple_mean_for_primary_value():
    decision = decide_full_eval_acceptance(
        objective="blended",
        previous_pre_arbiter=50.0,
        previous_post_arbiter=80.0,
        run_pre_arbiter=[60.0, 60.0],   # blended avg = (60+85)/2 = 72.5 → +7.5
        run_post_arbiter=[85.0, 85.0],
        min_primary_gain_pp=0.0,
        max_post_arbiter_drop_pp=2.0,
        variance_widened_tol_pp=5.0,
    )

    assert decision.accepted is True
    assert decision.reason_code == "accepted"
    # Primary delta computed from blended mean (~72.5 vs 65.0)
    assert decision.primary_delta_pp == pytest.approx(7.5, abs=0.1)
