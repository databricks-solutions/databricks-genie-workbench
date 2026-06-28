"""Regression — closes the cross-scope NameError at
``harness.py:20531`` that Cycle 11's typed ``PRODUCER_EXCEPTION``
record surfaced in run ``90000000000003`` (space 7now).

The acceptance stage's candidate-pre-arbiter computation referenced
``full_pre_arbiter_accuracy``, a name only assigned in a sibling eval
helper function, not in ``_run_lever_loop``'s scope. When
``gate_result.get("full_pre_arbiter_accuracy")`` returned ``None`` the
fallback raised ``NameError`` and the producer try/except swallowed
it, making the bug invisible until Cycle 11's typed record landed
the class + repr + traceback in the iteration's decision-record set.

The fix extracts the computation into a small pure helper that
mirrors the post-arbiter pattern used four lines above the bug
(``float(gate_result.get("full_accuracy") or 0.0)``).
"""

from __future__ import annotations


def test_candidate_pre_arbiter_from_gate_falls_back_to_zero_when_missing() -> None:
    from genie_space_optimizer.optimization.harness import (
        _candidate_pre_arbiter_from_gate,
    )

    assert _candidate_pre_arbiter_from_gate({}) == 0.0
    assert _candidate_pre_arbiter_from_gate(
        {"full_pre_arbiter_accuracy": None}
    ) == 0.0
    assert _candidate_pre_arbiter_from_gate(
        {"full_pre_arbiter_accuracy": 0.0}
    ) == 0.0
    assert _candidate_pre_arbiter_from_gate(
        {"full_pre_arbiter_accuracy": 0.83}
    ) == 0.83


def test_candidate_pre_arbiter_from_gate_handles_none_input() -> None:
    """Defensive — when callers pass ``None`` instead of a dict
    (e.g., gate_result was never built because gate-checks raised),
    the helper still returns ``0.0`` without raising. This matches
    the post-arbiter sibling's ``gate_result.get(...) or 0.0``
    semantics on the same ``None`` input."""
    from genie_space_optimizer.optimization.harness import (
        _candidate_pre_arbiter_from_gate,
    )

    assert _candidate_pre_arbiter_from_gate(None) == 0.0


def test_candidate_pre_arbiter_from_gate_coerces_int_to_float() -> None:
    """Defensive — gate_result occasionally carries integer accuracy
    values (e.g., 0 or 100 from edge-case rollups). The helper must
    return ``float`` so downstream ``AcceptanceInput`` validation
    does not reject the value."""
    from genie_space_optimizer.optimization.harness import (
        _candidate_pre_arbiter_from_gate,
    )

    out = _candidate_pre_arbiter_from_gate({"full_pre_arbiter_accuracy": 0})
    assert out == 0.0
    assert isinstance(out, float)
