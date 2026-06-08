"""Trial 30 W30.4(b) — typed terminal reasons for the three harness
no-candidate paths that previously emitted the raw ``"unknown"`` string.

The W29.4 airline postmortem saw ``terminal_reason="unknown"`` 4×, which
the gso-postmortem skill flags as a terminal-reason taxonomy gap
(SKILL.md line ~2084) and which forces a ❌ row in the Generalizable
Architecture Invariants table → ``architecture_invariants_held=false``.

The three paths are infrastructure / post-apply-rollback terminals that
no structural-funnel reason covered:

* pre-AG snapshot capture failed (infrastructure)
* applier / Genie API rejected the PATCH payload (infrastructure)
* slice / p0 gate regressed the candidate after apply (post-apply
  rollback)

This pins (1) the three new closed-vocabulary members exist and (2) the
post-iteration router treats them byte-identically to the prior
``"unknown"``-via-default routing (``skip_productive`` + forbid) so the
fix is observability-only — no behavioural drift in retry/forbid policy.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    build_terminal_signature,
)
from genie_space_optimizer.optimization.iteration_terminal_policy import (
    decide_iteration_terminal_action,
)


_NEW_REASONS = (
    (TerminalReason.INFRASTRUCTURE_PRE_AG_SNAPSHOT_FAILED,
     "infrastructure_pre_ag_snapshot_failed"),
    (TerminalReason.INFRASTRUCTURE_APPLIER_FAILED,
     "infrastructure_applier_failed"),
    (TerminalReason.SLICE_OR_P0_GATE_REGRESSION_ROLLBACK,
     "slice_or_p0_gate_regression_rollback"),
)


def test_new_typed_reasons_exist_with_canonical_values():
    for member, value in _NEW_REASONS:
        assert member.value == value
        # Round-trips through the StrEnum constructor → recognised by the
        # closed-vocabulary present-invariant validator.
        assert TerminalReason(value) is member


def test_new_reasons_route_like_prior_unknown_default():
    # Prior behaviour: raw "unknown" was absent from _ROUTING_TABLE, so it
    # fell through to the ("skip_productive", True) default. The typed
    # members must preserve that exact routing (observability-only fix).
    for member, _ in _NEW_REASONS:
        sig = build_terminal_signature(
            root_cause="r", blame_set=(), lever_set=(5,),
            target_qids=("gs_555",), terminal_reason=member,
        )
        result = decide_iteration_terminal_action(
            terminal_reason=member,
            signature=sig,
            prior_forbidden_set=frozenset(),
            iteration_index=0,
            iteration_budget=5,
        )
        assert result.next_step == "skip_productive"
        assert result.add_to_forbidden_set is True


def test_unknown_still_routes_skip_productive():
    # The defensive UNKNOWN member is unchanged — producers should no
    # longer emit it for these three paths, but it must still route.
    sig = build_terminal_signature(
        root_cause="r", blame_set=(), lever_set=(5,),
        target_qids=("gs_555",), terminal_reason=TerminalReason.UNKNOWN,
    )
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.UNKNOWN,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=0,
        iteration_budget=5,
    )
    assert result.next_step == "skip_productive"
    assert result.add_to_forbidden_set is True
