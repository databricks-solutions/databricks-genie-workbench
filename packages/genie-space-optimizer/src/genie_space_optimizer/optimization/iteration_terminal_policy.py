"""Spec Section 4.5 + Plan E Tier C addendum — iteration terminal
policy.

Two pure functions live here:

  * ``decide_iteration_terminal_action(...)`` — Plan A canon. Given a
    terminal reason + signature + state, return whether the iteration
    is retried, skipped (productive budget consumed), or skipped_no_op
    (productive budget NOT consumed), or whether the loop aborts.

  * ``decide_pre_iteration_pivot_action(...)`` — Plan E Tier C addendum.
    Given the list of candidate AG signatures and the retired set,
    decide whether to proceed, pivot to a non-retired candidate, force
    RCA regen, or terminate early.

Both functions are pure (no I/O, no DecisionRecord emission).
"""
from __future__ import annotations

from enum import StrEnum
from typing import NamedTuple, Sequence

from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
)


# Closed vocabulary for ``TerminalAction.next_step`` per spec
# Section 4.5. Stored as a frozenset for invariant assertions.
TERMINAL_NEXT_STEPS: frozenset[str] = frozenset({
    "retry_strategy_switch",
    "skip_productive",
    "skip_no_op",
    "abort_run",
})


class TerminalAction(NamedTuple):
    """Spec Section 4.5 ``TerminalAction`` — return shape of
    ``decide_iteration_terminal_action``. The harness consumes this
    to drive next-iteration / abort routing."""

    next_step: str
    """One of TERMINAL_NEXT_STEPS."""
    add_to_forbidden_set: bool
    """If True, the iteration's ``signature`` is admitted to the
    forbidden set (causes :class:`~optimization.forbidden_ag_set_v2`
    to emit GSO_TERMINAL_SIGNATURE_RETIRED_V1)."""
    reflection_payload: dict[str, object]
    """Payload merged into ``reflection_buffer[i]`` by the harness;
    carries the terminal_reason, terminal_signature, and any
    diagnostics needed by the candidate ledger / postmortem."""


class TerminalActionKind(StrEnum):
    """Pre-iteration pivot decisions — used only by
    ``decide_pre_iteration_pivot_action``."""

    PROCEED = "proceed"
    """No retired signatures intersect with candidates — continue
    the iteration normally."""
    PIVOT_TO_REMAINING = "pivot_to_remaining"
    """Some candidates are retired; pivot to the first non-retired
    candidate."""
    FORCE_RCA_REGEN_NON_TARGET = "force_rca_regen_non_target"
    """All candidates retired, but non-target clusters can still
    be RCA-regenerated."""
    TERMINATE_AG_RETIRED = "terminate_ag_retired"
    """All candidates retired AND every non-target cluster is
    blocked → terminate the loop honestly with
    TerminalReason.AG_COLLISION_WITH_FORBIDDEN_SET."""


# Spec Section 4.5 / contract spec Section 12.6 routing table. Each
# terminal reason maps to a (next_step, add_to_forbidden_set) tuple.
# This table MUST match the canonical 17-row table in spec Section
# 12.6 exactly — do not edit without a corresponding spec amendment.
_ROUTING_TABLE: dict[TerminalReason, tuple[str, bool]] = {
    TerminalReason.NO_RCA_GROUND: ("retry_strategy_switch", True),
    TerminalReason.NO_ACTION_GROUP_EMITTED: ("retry_strategy_switch", True),
    TerminalReason.AG_COLLISION_WITH_FORBIDDEN_SET: ("skip_no_op", False),
    TerminalReason.NO_STRUCTURAL_CANDIDATE: ("retry_strategy_switch", True),
    TerminalReason.PROPOSAL_GENERATION_EMPTY: ("retry_strategy_switch", True),
    # Plan 12 pivot trigger (2026-05-22): instruction-only structural
    # drops now retry within-iteration. Pre-fix this was skip_productive
    # — combined with the legacy archetype fallback (removed in the
    # PR-4 deletion commit), every hard-failure iteration burned
    # budget without giving the LLM-first synthesis path a second
    # chance. Aligned with PROPOSAL_GENERATION_EMPTY and
    # NO_STRUCTURAL_CANDIDATE.
    TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY: ("retry_strategy_switch", True),
    TerminalReason.APPLYABILITY_REJECTED: ("skip_productive", True),
    TerminalReason.BLAST_RADIUS_REJECTED: ("skip_productive", True),
    TerminalReason.COLLATERAL_RISK_REJECTED: ("skip_productive", True),
    TerminalReason.ALL_SELECTED_PATCHES_DROPPED_BY_APPLIER: ("skip_productive", True),
    TerminalReason.NO_APPLIED_PATCHES: ("skip_productive", True),
    TerminalReason.TARGET_QIDS_NOT_IMPROVED: ("retry_strategy_switch", True),
    TerminalReason.CONTENT_REGRESSION_ROLLBACK: ("retry_strategy_switch", True),
    TerminalReason.MULTI_PATCH_REGRESSION_NO_ISOLATION: ("retry_strategy_switch", True),
    TerminalReason.DIRECTIVE_OUTCOME_VIOLATION: ("retry_strategy_switch", True),
    TerminalReason.INVARIANT_VIOLATION: ("abort_run", True),
    TerminalReason.UNKNOWN: ("skip_productive", True),
}


def decide_iteration_terminal_action(
    *,
    terminal_reason: TerminalReason,
    signature: TerminalSignature,
    prior_forbidden_set: "frozenset[TerminalSignature]",
    iteration_index: int,
    iteration_budget: int,
) -> TerminalAction:
    """Spec Section 4.5 — pure router. Given a terminal reason +
    signature + state, return the next-step routing.

    Rules:
      * Look up (next_step, add_to_forbidden_set) in _ROUTING_TABLE
      * If signature is already in prior_forbidden_set,
        ``add_to_forbidden_set`` is forced to False (idempotency rule
        — spec Section 12.6)
      * If iteration_index >= iteration_budget - 1, any
        retry/skip_productive collapses to ``abort_run`` (budget-
        boundary rule — spec Section 12.6; no further iterations
        possible)
    """
    next_step, add_flag = _ROUTING_TABLE.get(
        terminal_reason, ("skip_productive", True),
    )
    # Forbidden-set idempotency rule (spec Section 12.6): already in
    # the set → do not re-admit.
    if signature in prior_forbidden_set:
        add_flag = False
    # Budget-boundary rule (spec Section 12.6): no more iterations
    # available → collapse retry/skip to abort_run.
    if iteration_index >= iteration_budget - 1 and next_step != "abort_run":
        next_step = "abort_run"
    return TerminalAction(
        next_step=next_step,
        add_to_forbidden_set=add_flag,
        reflection_payload={
            "terminal_reason": terminal_reason.value,
            "terminal_signature": signature,
            "iteration_index": iteration_index,
            "iteration_budget": iteration_budget,
        },
    )


def decide_pre_iteration_pivot_action(
    *,
    candidate_signatures: Sequence[TerminalSignature],
    retired_signatures: "frozenset[TerminalSignature]",
    all_clusters_have_active_blocked_no_rca: bool,
) -> TerminalActionKind:
    """Plan E Tier C addendum — pre-iteration pivot decision.

    Pure: no I/O.

    Decision rules (spec Section 12.1):
      * No candidate signatures at all → TERMINATE_AG_RETIRED.
      * No intersection between candidates and retired → PROCEED.
      * Some candidates remain non-retired → PIVOT_TO_REMAINING.
      * All candidates retired AND all non-target clusters blocked →
        TERMINATE_AG_RETIRED (honest early termination).
      * All candidates retired BUT non-target clusters available for
        RCA regen → FORCE_RCA_REGEN_NON_TARGET.
    """
    if not candidate_signatures:
        return TerminalActionKind.TERMINATE_AG_RETIRED

    candidates_set = set(candidate_signatures)
    if not candidates_set & retired_signatures:
        return TerminalActionKind.PROCEED

    if candidates_set - retired_signatures:
        return TerminalActionKind.PIVOT_TO_REMAINING

    if all_clusters_have_active_blocked_no_rca:
        return TerminalActionKind.TERMINATE_AG_RETIRED
    return TerminalActionKind.FORCE_RCA_REGEN_NON_TARGET
