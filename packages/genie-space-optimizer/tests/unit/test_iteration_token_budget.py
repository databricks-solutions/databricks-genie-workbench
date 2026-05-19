"""Plan 2 Task 7 — IterationTokenBudget basic accounting."""
from __future__ import annotations

from genie_space_optimizer.optimization.llm_token_budget import (
    IterationTokenBudget,
)


def test_budget_starts_empty() -> None:
    b = IterationTokenBudget(itpm_limit=1000, otpm_limit=500)
    assert b.reserved_input_tokens == 0
    assert b.reserved_output_tokens == 0
    assert b.actual_input_tokens == 0
    assert b.actual_output_tokens == 0


def test_reserve_then_reconcile_tracks_actual_usage() -> None:
    b = IterationTokenBudget(itpm_limit=1000, otpm_limit=500)
    reservation = b.reserve(input_tokens=80, max_output_tokens=200)
    assert b.reserved_input_tokens == 80
    assert b.reserved_output_tokens == 200
    b.reconcile(reservation, actual_input=85, actual_output=140)
    assert b.actual_input_tokens == 85
    assert b.actual_output_tokens == 140
    assert b.reserved_input_tokens == 85
    assert b.reserved_output_tokens == 140


def test_would_exceed_input_returns_true_when_reservation_overflows() -> None:
    b = IterationTokenBudget(itpm_limit=100, otpm_limit=500)
    b.reserve(input_tokens=80, max_output_tokens=50)
    assert b.would_exceed(input_tokens=30, max_output_tokens=10) is True


def test_would_exceed_output_returns_true_when_reservation_overflows() -> None:
    b = IterationTokenBudget(itpm_limit=1000, otpm_limit=100)
    b.reserve(input_tokens=10, max_output_tokens=80)
    assert b.would_exceed(input_tokens=5, max_output_tokens=30) is True


def test_would_exceed_returns_false_when_within_budget() -> None:
    b = IterationTokenBudget(itpm_limit=1000, otpm_limit=500)
    b.reserve(input_tokens=100, max_output_tokens=200)
    assert b.would_exceed(input_tokens=50, max_output_tokens=100) is False


def test_reset_clears_all_accounting() -> None:
    b = IterationTokenBudget(itpm_limit=1000, otpm_limit=500)
    res = b.reserve(input_tokens=100, max_output_tokens=200)
    b.reconcile(res, actual_input=100, actual_output=150)
    b.reset()
    assert b.reserved_input_tokens == 0
    assert b.reserved_output_tokens == 0
    assert b.actual_input_tokens == 0
    assert b.actual_output_tokens == 0


def test_credit_back_when_actual_less_than_reserved() -> None:
    """Per Databricks limits doc: 'if actual is less than reserved
    max_tokens, the system credits the difference back'."""
    b = IterationTokenBudget(itpm_limit=1000, otpm_limit=500)
    res = b.reserve(input_tokens=50, max_output_tokens=300)
    assert b.reserved_output_tokens == 300
    b.reconcile(res, actual_input=50, actual_output=120)
    assert b.reserved_output_tokens == 120


def test_context_var_returns_no_op_budget_when_unset() -> None:
    """Outside an iteration, the ContextVar returns a meter with
    effectively-infinite limits so non-iteration callers don't
    crash."""
    from genie_space_optimizer.optimization.llm_token_budget import (
        _REASONING_TOKEN_BUDGET,
    )
    b = _REASONING_TOKEN_BUDGET.get()
    assert b.itpm_limit >= 10**9
    assert b.otpm_limit >= 10**9
