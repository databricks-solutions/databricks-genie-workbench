"""Regression — closes Bug C, the cross-scope ``NameError`` on
``_baseline_rows_for_control_plane`` at the rollback-side
``AcceptanceInput`` call site (``harness.py:_run_lever_loop`` line
``:20746``).

Cycle 11's typed ``PRODUCER_EXCEPTION`` decision record from run
``900000000000002`` (7now, parent run
``11110001-0000-4000-8000-000000000001``) named the bug:

    NameError("name '_baseline_rows_for_control_plane' is not defined")

Same family as Bug A (``full_pre_arbiter_accuracy``) and Bug B
(``full_accuracy``). ``_baseline_rows_for_control_plane`` is assigned
ONLY inside ``_run_gate_checks`` (a sibling module-level function,
``harness.py:11807-11820``); ``_run_lever_loop`` reads the same name
at ``:20746`` with no local assignment and no closure — Python
compiles it as a free-variable lookup that fails LEGB.

The fix sources the rollback-side ``AcceptanceInput.pre_rows`` from
``_run_lever_loop``'s own ``_accepted_baseline_rows_for_control_plane``
local (the rollback-aware committed baseline already passed into the
gate at ``:20535``) via a small pure helper.
"""

from __future__ import annotations


def test_baseline_rows_for_acceptance_input_returns_empty_tuple_when_none() -> None:
    """The rollback path's AcceptanceInput.pre_rows accepts an empty
    tuple when no baseline was carried (e.g., iteration 0 / cold
    start). The helper must return ``()`` rather than raising and
    rather than reading a name from a sibling function's scope."""
    from genie_space_optimizer.optimization.harness import (
        _baseline_rows_for_acceptance_input,
    )

    assert _baseline_rows_for_acceptance_input(accepted_baseline_rows=None) == ()
    assert _baseline_rows_for_acceptance_input(accepted_baseline_rows=[]) == ()


def test_baseline_rows_for_acceptance_input_returns_tuple_of_rows() -> None:
    """When the carried baseline is non-empty the helper returns a
    plain ``tuple`` of the same dicts. Tuple-shape matches the
    AcceptanceInput contract; the original buggy call site used
    ``tuple(... or [])`` for the same reason."""
    from genie_space_optimizer.optimization.harness import (
        _baseline_rows_for_acceptance_input,
    )

    rows = [{"qid": "gs_001"}, {"qid": "gs_002"}]
    out = _baseline_rows_for_acceptance_input(accepted_baseline_rows=rows)
    assert out == ({"qid": "gs_001"}, {"qid": "gs_002"})
    assert isinstance(out, tuple)


def test_baseline_rows_for_acceptance_input_does_not_mutate_input() -> None:
    """Defensive — the harness reuses
    ``_accepted_baseline_rows_for_control_plane`` across iterations.
    The helper must produce its tuple without side-effecting the
    caller's list."""
    from genie_space_optimizer.optimization.harness import (
        _baseline_rows_for_acceptance_input,
    )

    rows = [{"qid": "gs_001"}]
    _ = _baseline_rows_for_acceptance_input(accepted_baseline_rows=rows)
    assert rows == [{"qid": "gs_001"}]
