"""Trial 23 W1 — kept_insufficient is the authoritative terminal reason.

The d139 postmortem (``NO_APPLIED_PATCHES_CONFLICTS_WITH_KEPT_INSUFFICIENT``)
showed an iteration that applied patches and recorded kept-insufficient
acceptance, yet the terminal taxonomy emitted ``no_applied_patches``.
The single producer ``compute_iteration_terminal_reason`` must now return
``KEPT_INSUFFICIENT`` whenever ``kept_insufficient_count > 0``, at the
highest precedence.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.iteration_terminal import (
    compute_iteration_terminal_reason,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason


def test_kept_insufficient_count_forces_kept_insufficient_terminal():
    verdict = compute_iteration_terminal_reason(
        stage3_proposal_count=2,
        compiler_surviving_count=2,
        applied_outcome_count=2,
        kept_insufficient_count=2,
    )
    assert verdict.terminal_reason == TerminalReason.KEPT_INSUFFICIENT


def test_kept_insufficient_wins_over_slate_compiler_empty():
    """Even if some AGs dropped at the compiler, an applied+kept-
    insufficient signal in the iteration dominates: the iteration is not
    a no-applied-patches iteration."""
    verdict = compute_iteration_terminal_reason(
        stage3_proposal_count=3,
        compiler_surviving_count=0,
        compiler_top_drop_reason="bundle_invariant_violated",
        applied_outcome_count=1,
        kept_insufficient_count=1,
    )
    assert verdict.terminal_reason == TerminalReason.KEPT_INSUFFICIENT


def test_sentinel_preserves_pre_trial23_behavior():
    """Default sentinel (-1) leaves the W4 taxonomy byte-stable: a
    compiler-empty iteration still reports SLATE_COMPILER_EMPTY."""
    verdict = compute_iteration_terminal_reason(
        stage3_proposal_count=3,
        compiler_surviving_count=0,
        compiler_top_drop_reason="bundle_invariant_violated",
        applied_outcome_count=0,
    )
    assert verdict.terminal_reason == TerminalReason.SLATE_COMPILER_EMPTY


def test_zero_kept_insufficient_does_not_trigger():
    """kept_insufficient_count == 0 (measured, none) must NOT force the
    terminal — falls through to the fallback."""
    verdict = compute_iteration_terminal_reason(
        applied_outcome_count=-1,
        kept_insufficient_count=0,
    )
    assert verdict.terminal_reason == TerminalReason.NO_APPLIED_PATCHES
