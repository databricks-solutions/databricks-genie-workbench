"""Unit tests for the eval-run budget guard (GSO v2 Phase 1 §3.4)."""

from __future__ import annotations

import pytest

from genie_space_optimizer.common import config
from genie_space_optimizer.optimization.eval_budget import (
    EvalBudget,
    WorkingSetAdvice,
    assess_working_set,
    estimate_eval_run_seconds,
    estimate_three_gate_seconds,
)


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


# ── estimation ──────────────────────────────────────────────────────────────
def test_estimate_eval_run_seconds() -> None:
    assert estimate_eval_run_seconds(0) == 0.0
    assert estimate_eval_run_seconds(-5) == 0.0
    assert estimate_eval_run_seconds(30, per_question_seconds=34) == pytest.approx(1020.0)


def test_estimate_three_gate_clamps_to_working_set() -> None:
    # ws=30, slice cap 10, p0 cap 15 ⇒ (10+15+30)*34 = 1870
    assert estimate_three_gate_seconds(
        working_set_size=30, slice_size=10, p0_size=15, per_question_seconds=34
    ) == pytest.approx(1870.0)
    # Small working set clamps slice/p0 down to ws.
    assert estimate_three_gate_seconds(
        working_set_size=5, slice_size=10, p0_size=15, per_question_seconds=34
    ) == pytest.approx((5 + 5 + 5) * 34)


def test_estimate_three_gate_without_full() -> None:
    # run_full=False drops the full-benchmark term (subset-only iterations).
    only_subsets = estimate_three_gate_seconds(
        working_set_size=30, slice_size=10, p0_size=15, per_question_seconds=34, run_full=False
    )
    assert only_subsets == pytest.approx((10 + 15) * 34)


# ── budget accounting ────────────────────────────────────────────────────────
def test_budget_records_and_reserves() -> None:
    clock = _Clock()
    budget = EvalBudget(hard_wall_seconds=1000, finalize_reserve_seconds=200, clock=clock)
    assert budget.spent() == 0.0
    assert budget.remaining() == 1000
    assert budget.remaining_after_reserve() == 800

    budget.record(300)
    assert budget.spent() == 300
    assert budget.remaining() == 700
    assert budget.remaining_after_reserve() == 500


def test_budget_can_afford_respects_finalize_reserve() -> None:
    budget = EvalBudget(hard_wall_seconds=1000, finalize_reserve_seconds=200, clock=_Clock())
    budget.record(300)  # remaining_after_reserve == 500
    assert budget.can_afford(500) is True
    assert budget.can_afford(501) is False


def test_budget_record_is_cumulative_and_nonnegative() -> None:
    budget = EvalBudget(hard_wall_seconds=10_000, finalize_reserve_seconds=0, clock=_Clock())
    budget.record(100)
    budget.record(-50)  # clamped to 0
    budget.record(25)
    assert budget.spent() == pytest.approx(125.0)


def test_budget_remaining_floors_at_zero() -> None:
    budget = EvalBudget(hard_wall_seconds=100, finalize_reserve_seconds=50, clock=_Clock())
    budget.record(1000)
    assert budget.remaining() == 0.0
    assert budget.remaining_after_reserve() == 0.0
    assert budget.can_afford(1) is False


def test_budget_from_config(monkeypatch) -> None:
    monkeypatch.setattr(config, "EVAL_JOB_WALL_SECONDS", 7200)
    monkeypatch.setattr(config, "EVAL_FINALIZE_RESERVE_SECONDS", 1200)
    budget = EvalBudget.from_config(clock=_Clock())
    assert budget.hard_wall_seconds == 7200
    assert budget.finalize_reserve_seconds == 1200
    assert budget.remaining_after_reserve() == 6000


def test_budget_elapsed_uses_clock() -> None:
    clock = _Clock()
    budget = EvalBudget(hard_wall_seconds=10, finalize_reserve_seconds=0, clock=clock)
    clock.t = 42.0
    assert budget.elapsed() == pytest.approx(42.0)


# ── working-set advice ───────────────────────────────────────────────────────
def test_assess_working_set_recommendations() -> None:
    prune = assess_working_set(45, lo=30, hi=40)
    assert isinstance(prune, WorkingSetAdvice)
    assert prune.recommendation == "prune"
    assert prune.within_bounds is False
    assert "5" in prune.detail  # 45 - 40 = 5 to prune

    topup = assess_working_set(22, lo=30, hi=40)
    assert topup.recommendation == "topup"
    assert topup.within_bounds is False

    ok = assess_working_set(35, lo=30, hi=40)
    assert ok.recommendation == "ok"
    assert ok.within_bounds is True

    # Boundaries are inclusive.
    assert assess_working_set(30, lo=30, hi=40).recommendation == "ok"
    assert assess_working_set(40, lo=30, hi=40).recommendation == "ok"


def test_assess_working_set_defaults_from_config(monkeypatch) -> None:
    monkeypatch.setattr(config, "WORKING_SET_MIN", 30)
    monkeypatch.setattr(config, "WORKING_SET_MAX", 40)
    assert assess_working_set(50).recommendation == "prune"
