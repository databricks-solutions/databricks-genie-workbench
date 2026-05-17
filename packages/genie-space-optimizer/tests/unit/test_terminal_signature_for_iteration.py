"""Phase 2 (2026-05-16) — adapter that converts Phase 1 iter-locals
dict + a ``TerminalReason`` into a fully populated
``TerminalSignature``. Keeps the eight non-accepted reflection-write
call sites in ``harness.py`` DRY.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
)


def _iter_locals_full() -> dict:
    """Reference iter-locals snapshot — mirrors what Phase 1's
    ``capture_iter_ag_context`` returns for AG_DECOMPOSED_H001 in
    Run B's airline iter 1."""
    return {
        "ag_id": "AG_DECOMPOSED_H001",
        "cluster_ids": ("H001",),
        "target_qids": ("airline_gs_017",),
        "levers": (5,),
        "root_cause": "missing_or_misordered_join",
        "blame_set": ("catalog.airline.fact_bookings",),
    }


def test_returns_terminal_signature_named_tuple():
    from genie_space_optimizer.optimization.terminal_signature_iter import (
        terminal_signature_for_iteration,
    )
    sig = terminal_signature_for_iteration(
        iter_locals=_iter_locals_full(),
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    assert isinstance(sig, TerminalSignature)


def test_carries_all_five_signature_fields():
    from genie_space_optimizer.optimization.terminal_signature_iter import (
        terminal_signature_for_iteration,
    )
    sig = terminal_signature_for_iteration(
        iter_locals=_iter_locals_full(),
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    assert sig.root_cause == "missing_or_misordered_join"
    assert sig.blame_set_norm == ("catalog.airline.fact_bookings",)
    assert sig.lever_set == frozenset({5})
    assert sig.target_qids == frozenset({"airline_gs_017"})
    assert sig.terminal_reason == "no_applied_patches"


def test_accepts_terminal_reason_as_string():
    """The adapter must accept both ``TerminalReason`` and its
    ``.value`` string (defensive: not every call site has the enum
    in scope)."""
    from genie_space_optimizer.optimization.terminal_signature_iter import (
        terminal_signature_for_iteration,
    )
    sig = terminal_signature_for_iteration(
        iter_locals=_iter_locals_full(),
        terminal_reason="no_applied_patches",
    )
    assert sig.terminal_reason == "no_applied_patches"


def test_rejects_unknown_terminal_reason_string():
    """``build_terminal_signature`` validates the enum value — propagate."""
    from genie_space_optimizer.optimization.terminal_signature_iter import (
        terminal_signature_for_iteration,
    )
    with pytest.raises(ValueError):
        terminal_signature_for_iteration(
            iter_locals=_iter_locals_full(),
            terminal_reason="not_a_terminal_reason",
        )


def test_empty_locals_produce_empty_signature():
    from genie_space_optimizer.optimization.terminal_signature_iter import (
        terminal_signature_for_iteration,
    )
    sig = terminal_signature_for_iteration(
        iter_locals={},
        terminal_reason=TerminalReason.UNKNOWN,
    )
    assert sig.root_cause == ""
    assert sig.blame_set_norm == ()
    assert sig.lever_set == frozenset()
    assert sig.target_qids == frozenset()
    assert sig.terminal_reason == "unknown"


def test_missing_keys_treated_as_empty():
    """Sparse iter-locals (e.g., a pre-AG-selection terminal) still
    yield a valid signature with empty fields — not an exception."""
    from genie_space_optimizer.optimization.terminal_signature_iter import (
        terminal_signature_for_iteration,
    )
    sig = terminal_signature_for_iteration(
        iter_locals={"ag_id": "AG1"},
        terminal_reason=TerminalReason.NO_ACTION_GROUP_EMITTED,
    )
    assert sig.root_cause == ""
    assert sig.blame_set_norm == ()
    assert sig.lever_set == frozenset()
    assert sig.target_qids == frozenset()
    assert sig.terminal_reason == "no_action_group_emitted"


def test_root_cause_is_normalised_lowercase():
    """``build_terminal_signature`` normalises root_cause via
    ``normalize_signature_str`` — propagate."""
    from genie_space_optimizer.optimization.terminal_signature_iter import (
        terminal_signature_for_iteration,
    )
    sig = terminal_signature_for_iteration(
        iter_locals={"root_cause": "  Missing_Or_Misordered_Join  "},
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    assert sig.root_cause == "missing_or_misordered_join"


def test_target_qids_become_frozenset():
    """Tuple input ↔ frozenset output (per spec Section 4.3 ordering rules)."""
    from genie_space_optimizer.optimization.terminal_signature_iter import (
        terminal_signature_for_iteration,
    )
    sig = terminal_signature_for_iteration(
        iter_locals={
            "target_qids": ("gs_017", "gs_017", "gs_018"),
        },
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    assert sig.target_qids == frozenset({"gs_017", "gs_018"})
