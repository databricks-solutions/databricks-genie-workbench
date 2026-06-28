"""Regression — closes Bug B, the cross-scope ``UnboundLocalError``
on ``full_accuracy`` at ``harness.py:_run_lever_loop`` (line ``:13779``
post-Bug-A-fix; ``:13751`` pre-fix as named in the deployed traceback).

Cycle 11's typed ``PRODUCER_EXCEPTION`` decision record from run
``90000000000004`` (airline, parent run
``11110002-0000-4000-8000-000000000002``) named the bug:

    UnboundLocalError("cannot access local variable 'full_accuracy'
    where it is not associated with a value")

Why it fires only on rollback-only plateaus: ``full_accuracy`` is
assigned inside ``_run_lever_loop`` exclusively in the *acceptance*
branch (``full_accuracy = gate_result["full_accuracy"]`` at the
post-acceptance update). Python's compiler sees it as local because
of that assignment. On runs where every iteration is rolled back,
the assignment never executes, and the next iteration's
plateau-termination F9 learning stage reads the unbound local.

The fix extracts a small pure helper that mirrors the
``_candidate_pre_arbiter_from_gate`` shape from the Bug A commit:
source the per-iteration delta from ``gate_result.full_accuracy``
when present, fall back to ``0.0`` when no candidate was evaluated
this iteration (the rollback-only plateau case).
"""

from __future__ import annotations


def test_f9_accuracy_delta_falls_back_to_zero_when_gate_result_missing() -> None:
    """The plateau-termination F9 call site fires even when no
    candidate was evaluated this iteration (rollback-only plateau).
    On that path the helper must return ``0.0`` rather than reading
    a name that lives in a sibling function's scope."""
    from genie_space_optimizer.optimization.harness import (
        _f9_accuracy_delta_safe,
    )

    assert _f9_accuracy_delta_safe(None, best_accuracy=83.3) == 0.0
    assert _f9_accuracy_delta_safe({}, best_accuracy=83.3) == 0.0
    assert _f9_accuracy_delta_safe(
        {"full_accuracy": None}, best_accuracy=83.3
    ) == 0.0


def test_f9_accuracy_delta_subtracts_best_when_gate_full_accuracy_present() -> None:
    """When a candidate was evaluated this iteration the delta is
    just ``candidate_full_accuracy - best_accuracy``. The helper
    matches the original (pre-bug) intent for code paths where
    ``gate_result`` is well-formed."""
    from genie_space_optimizer.optimization.harness import (
        _f9_accuracy_delta_safe,
    )

    import pytest

    assert _f9_accuracy_delta_safe({"full_accuracy": 91.7}, 83.3) == pytest.approx(
        8.4, abs=1e-9
    )
    assert _f9_accuracy_delta_safe({"full_accuracy": 75.0}, 83.3) == pytest.approx(
        -8.3, abs=1e-9
    )
    assert _f9_accuracy_delta_safe({"full_accuracy": 83.3}, 83.3) == pytest.approx(
        0.0, abs=1e-9
    )


def test_f9_accuracy_delta_returns_float() -> None:
    """Defensive — gate_result occasionally carries integer
    accuracy values (e.g., 0 or 100 from edge-case rollups) and
    ``best_accuracy`` may be the literal ``0``. The helper must
    return ``float`` so downstream LearningInput validation does
    not reject the value."""
    from genie_space_optimizer.optimization.harness import (
        _f9_accuracy_delta_safe,
    )

    out = _f9_accuracy_delta_safe({"full_accuracy": 0}, 0)
    assert out == 0.0
    assert isinstance(out, float)


def test_f9_accuracy_delta_treats_non_dict_gate_result_as_zero() -> None:
    """Defensive — if the iteration body short-circuited and
    ``gate_result`` was never assigned (the
    ``locals().get('gate_result')`` call at the F9 site returns
    ``None``), the helper still returns ``0.0`` without raising."""
    from genie_space_optimizer.optimization.harness import (
        _f9_accuracy_delta_safe,
    )

    assert _f9_accuracy_delta_safe(None, 83.3) == 0.0
    assert _f9_accuracy_delta_safe("not a dict", 83.3) == 0.0  # type: ignore[arg-type]
