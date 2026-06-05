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


def test_set_iteration_budget_installs_active_meter() -> None:
    """Phase 0 P0.1 — ``set_iteration_budget`` installs a real budget
    on the ContextVar so callers see finite limits."""
    from genie_space_optimizer.optimization.llm_token_budget import (
        _REASONING_TOKEN_BUDGET,
        clear_iteration_budget,
        set_iteration_budget,
    )
    budget, token = set_iteration_budget(itpm_limit=120_000, otpm_limit=12_000)
    try:
        active = _REASONING_TOKEN_BUDGET.get()
        assert active is budget
        assert active.itpm_limit == 120_000
        assert active.otpm_limit == 12_000
    finally:
        clear_iteration_budget(token)
    restored = _REASONING_TOKEN_BUDGET.get()
    assert restored.itpm_limit >= 10**9


def test_set_and_clear_round_trip_per_iteration() -> None:
    """Phase 0 P0.1 — paired set/clear is the iteration-boundary
    contract; a fresh ``set`` between iterations yields a fresh meter
    with empty accounting."""
    from genie_space_optimizer.optimization.llm_token_budget import (
        _REASONING_TOKEN_BUDGET,
        clear_iteration_budget,
        set_iteration_budget,
    )
    b1, t1 = set_iteration_budget(itpm_limit=1_000, otpm_limit=500)
    b1.reserve(input_tokens=300, max_output_tokens=100)
    assert b1.reserved_input_tokens == 300
    clear_iteration_budget(t1)

    b2, t2 = set_iteration_budget(itpm_limit=1_000, otpm_limit=500)
    try:
        assert _REASONING_TOKEN_BUDGET.get() is b2
        assert b2.reserved_input_tokens == 0
        assert b2.actual_input_tokens == 0
    finally:
        clear_iteration_budget(t2)


def test_get_active_budget_returns_the_active_meter() -> None:
    """Phase 0 P0.1 — ``get_active_budget`` is the read-only accessor
    used by postmortem markers; it returns whatever ``ContextVar.get``
    would, without mutation."""
    from genie_space_optimizer.optimization.llm_token_budget import (
        clear_iteration_budget,
        get_active_budget,
        set_iteration_budget,
    )
    budget, token = set_iteration_budget(itpm_limit=42, otpm_limit=7)
    try:
        assert get_active_budget() is budget
        assert get_active_budget().itpm_limit == 42
    finally:
        clear_iteration_budget(token)
    assert get_active_budget().itpm_limit >= 10**9
