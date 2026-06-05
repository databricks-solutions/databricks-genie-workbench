"""Trial 20 Workstream A2 — post-arbiter-gain absorbs pre-arbiter regression.

Pins:

* Airline iter-2 shape (post-arbiter delta +4.2pp, pre-arbiter delta
  -12.5pp, no out-of-target hard regression, target_fixed empty)
  accepts under ``trial20_pre_arbiter_veto_fix_enabled``.
* Same shape rejects when the flag is OFF (byte-stable Trial 19
  replay).
* Post-arbiter delta == 0 with empty target_fixed still rejects
  (no symmetric absorption).
* Out-of-target hard regression blocks the absorb-branch.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.control_plane import (
    decide_pre_arbiter_regression_guardrail,
)


@pytest.fixture
def trial20_enforce_on(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL20_ENFORCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL20_PRE_ARBITER_VETO_FIX", raising=False)


@pytest.fixture
def trial20_enforce_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL20_ENFORCE", "0")


def test_airline_shape_accepts_under_a2(trial20_enforce_on):
    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=87.5,
        candidate_pre_arbiter_accuracy=75.0,
        target_fixed_qids=(),
        post_arbiter_delta_pp=4.2,
        out_of_target_hard_regressions=0,
    )
    assert decision.accepted is True
    assert (
        decision.reason_code
        == "accepted_post_arbiter_gain_absorbs_pre_arbiter_regression"
    )
    assert decision.delta_pp == -12.5


def test_airline_shape_rejects_when_flag_off(trial20_enforce_off):
    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=87.5,
        candidate_pre_arbiter_accuracy=75.0,
        target_fixed_qids=(),
        post_arbiter_delta_pp=4.2,
        out_of_target_hard_regressions=0,
    )
    assert decision.accepted is False
    assert decision.reason_code == "pre_arbiter_regression_without_target_fix"


def test_post_arbiter_zero_does_not_absorb(trial20_enforce_on):
    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=87.5,
        candidate_pre_arbiter_accuracy=75.0,
        target_fixed_qids=(),
        post_arbiter_delta_pp=0.0,
        out_of_target_hard_regressions=0,
    )
    assert decision.accepted is False
    assert decision.reason_code == "pre_arbiter_regression_without_target_fix"


def test_out_of_target_regression_blocks_absorb(trial20_enforce_on):
    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=87.5,
        candidate_pre_arbiter_accuracy=75.0,
        target_fixed_qids=(),
        post_arbiter_delta_pp=4.2,
        out_of_target_hard_regressions=1,
    )
    assert decision.accepted is False
    assert decision.reason_code == "pre_arbiter_regression_without_target_fix"


def test_target_fixed_still_short_circuits(trial20_enforce_on):
    """target_fixed_qids non-empty always wins (legacy branch)."""
    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=87.5,
        candidate_pre_arbiter_accuracy=75.0,
        target_fixed_qids=("gs_009",),
        post_arbiter_delta_pp=4.2,
        out_of_target_hard_regressions=0,
    )
    assert decision.accepted is True
    assert decision.reason_code == "target_fixed"


def test_within_budget_accepts_without_post_arbiter_data(trial20_enforce_on):
    """Small pre-arbiter drop within budget accepts as today."""
    decision = decide_pre_arbiter_regression_guardrail(
        baseline_pre_arbiter_accuracy=87.5,
        candidate_pre_arbiter_accuracy=85.0,
        target_fixed_qids=(),
    )
    assert decision.accepted is True
    assert decision.reason_code == "within_pre_arbiter_regression_budget"
