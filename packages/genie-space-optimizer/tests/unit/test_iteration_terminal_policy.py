"""Phase 1.3 / spec Section 4.5 — iteration_terminal_policy.

Covers two functions:
  * decide_iteration_terminal_action(...)        — spec post-iteration
    router (Plan A canon)
  * decide_pre_iteration_pivot_action(...)       — pre-iteration pivot
    helper (Plan E Tier C addendum)
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
    build_terminal_signature,
)
from genie_space_optimizer.optimization.iteration_terminal_policy import (
    TerminalAction,
    TerminalActionKind,
    decide_iteration_terminal_action,
    decide_pre_iteration_pivot_action,
)


def _sig(*, root_cause="r", target_qids=("gs_001",),
         lever_set=(5,),
         terminal_reason=TerminalReason.NO_APPLIED_PATCHES):
    return build_terminal_signature(
        root_cause=root_cause, blame_set=(),
        lever_set=lever_set, target_qids=target_qids,
        terminal_reason=terminal_reason,
    )


# ──────────────────────────────────────────────────────────────────
# Tests for spec Section 4.5 post-iteration router
# ──────────────────────────────────────────────────────────────────

def test_post_iter_no_rca_ground_returns_retry_strategy_switch():
    """Spec Section 4.5 routing table row 1: NO_RCA_GROUND →
    retry_strategy_switch with add_to_forbidden_set=True."""
    sig = _sig(terminal_reason=TerminalReason.NO_RCA_GROUND)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.NO_RCA_GROUND,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=0,
        iteration_budget=5,
    )
    assert result.next_step == "retry_strategy_switch"
    assert result.add_to_forbidden_set is True
    assert isinstance(result.reflection_payload, dict)


def test_post_iter_ag_collision_returns_skip_no_op():
    """Spec routing: AG_COLLISION_WITH_FORBIDDEN_SET →
    skip_no_op (budget NOT consumed)."""
    sig = _sig(terminal_reason=TerminalReason.AG_COLLISION_WITH_FORBIDDEN_SET)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.AG_COLLISION_WITH_FORBIDDEN_SET,
        signature=sig,
        prior_forbidden_set=frozenset({sig}),
        iteration_index=2,
        iteration_budget=5,
    )
    assert result.next_step == "skip_no_op"
    assert result.add_to_forbidden_set is False  # already forbidden


def test_post_iter_applyability_rejected_returns_skip_productive():
    """Spec routing: APPLYABILITY_REJECTED →
    skip_productive (budget IS consumed)."""
    sig = _sig(terminal_reason=TerminalReason.APPLYABILITY_REJECTED)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.APPLYABILITY_REJECTED,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=1,
        iteration_budget=5,
    )
    assert result.next_step == "skip_productive"
    assert result.add_to_forbidden_set is True


def test_post_iter_invariant_violation_returns_abort_run():
    """Spec routing: INVARIANT_VIOLATION → abort_run (terminates the
    whole loop)."""
    sig = _sig(terminal_reason=TerminalReason.INVARIANT_VIOLATION)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.INVARIANT_VIOLATION,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=3,
        iteration_budget=5,
    )
    assert result.next_step == "abort_run"


def test_post_iter_at_budget_returns_abort_run():
    """When iteration_index >= iteration_budget - 1, ANY terminal
    reason that would normally retry instead returns abort_run."""
    sig = _sig(terminal_reason=TerminalReason.NO_RCA_GROUND)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.NO_RCA_GROUND,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=4,
        iteration_budget=5,
    )
    assert result.next_step == "abort_run"


def test_post_iter_reflection_payload_carries_signature():
    """The reflection_payload returned by decide_iteration_terminal_action
    MUST carry the signature and terminal_reason for the candidate
    ledger writer to consume."""
    sig = _sig(terminal_reason=TerminalReason.NO_APPLIED_PATCHES)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=0,
        iteration_budget=5,
    )
    assert "terminal_reason" in result.reflection_payload
    assert "terminal_signature" in result.reflection_payload
    assert result.reflection_payload["terminal_reason"] == \
        TerminalReason.NO_APPLIED_PATCHES.value


# ──────────────────────────────────────────────────────────────────
# Tests for the pre-iteration pivot helper (Plan E Tier C addendum)
# ──────────────────────────────────────────────────────────────────

def test_pre_iter_no_retired_returns_proceed():
    sigs = [_sig()]
    action = decide_pre_iteration_pivot_action(
        candidate_signatures=sigs,
        retired_signatures=frozenset(),
        all_clusters_have_active_blocked_no_rca=False,
    )
    assert action == TerminalActionKind.PROCEED


def test_pre_iter_some_retired_returns_pivot_to_remaining():
    sigs = [
        _sig(target_qids=("gs_001",)),
        _sig(target_qids=("gs_002",), root_cause="r2"),
    ]
    action = decide_pre_iteration_pivot_action(
        candidate_signatures=sigs,
        retired_signatures=frozenset({sigs[0]}),
        all_clusters_have_active_blocked_no_rca=False,
    )
    assert action == TerminalActionKind.PIVOT_TO_REMAINING


def test_pre_iter_all_retired_no_blocked_returns_force_regen():
    sig = _sig()
    action = decide_pre_iteration_pivot_action(
        candidate_signatures=[sig],
        retired_signatures=frozenset({sig}),
        all_clusters_have_active_blocked_no_rca=False,
    )
    assert action == TerminalActionKind.FORCE_RCA_REGEN_NON_TARGET


def test_pre_iter_all_retired_all_blocked_returns_terminate():
    sig = _sig()
    action = decide_pre_iteration_pivot_action(
        candidate_signatures=[sig],
        retired_signatures=frozenset({sig}),
        all_clusters_have_active_blocked_no_rca=True,
    )
    assert action == TerminalActionKind.TERMINATE_AG_RETIRED


def test_empty_candidates_returns_terminate():
    """No candidates at all = terminate."""
    action = decide_pre_iteration_pivot_action(
        candidate_signatures=[],
        retired_signatures=frozenset(),
        all_clusters_have_active_blocked_no_rca=True,
    )
    assert action == TerminalActionKind.TERMINATE_AG_RETIRED
