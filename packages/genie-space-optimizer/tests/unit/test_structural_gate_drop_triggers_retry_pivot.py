"""Plan 12 pivot trigger contract:
``STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY`` must route to
``retry_strategy_switch``, not ``skip_productive``.

Motivation
----------
The 2026-05-22 postmortems (98ec8950, dc89d1a9) show that for the
hard-failure anchors gs_009 / gs_021 / gs_024 / gs_026, proposals
arrived at Stage 3 as instruction-only patches (legacy archetype /
``RcaKind.UNKNOWN`` → ``generic_judge_clarification`` fallback), the
structural-repair gate correctly rejected them, and the iteration
recorded ``terminal_reason = structural_gate_dropped_instruction_only``.

The routing table previously mapped this terminal reason to
``("skip_productive", True)`` — meaning the signature was retired and
budget was consumed, but no retry within the iteration was attempted.
Combined with the legacy archetype fallback (which Commit 3 removes),
this guaranteed flat accuracy: every hard-failure iteration burned
through budget without ever giving the LLM-first synthesis path a
chance to produce a structural patch.

After Commit 3 deletes the archetype fallback, residual
``STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY`` events should be rare,
but when they happen the harness must retry with a different
signature (different lever family, different RCA cluster) so the
LLM-first path gets multiple chances per iteration. The right next
step is ``retry_strategy_switch``, matching ``PROPOSAL_GENERATION_EMPTY``
and ``NO_STRUCTURAL_CANDIDATE`` which already route this way.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.iteration_terminal_policy import (
    decide_iteration_terminal_action,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    build_terminal_signature,
)


def _sig(terminal_reason: TerminalReason):
    return build_terminal_signature(
        root_cause="wrong_aggregation",
        blame_set=(),
        lever_set=(6,),
        target_qids=("gs_009",),
        terminal_reason=terminal_reason,
    )


def test_structural_gate_dropped_instruction_only_triggers_retry_switch():
    """Plan 12 pivot trigger: instruction-only structural drops must
    cause the harness to switch strategy within the iteration, NOT
    consume budget silently. This is the trigger that lets the
    LLM-first synthesis path get a second chance after a residual
    legacy-style proposal slips through.
    """
    sig = _sig(TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=0,
        iteration_budget=5,
    )
    assert result.next_step == "retry_strategy_switch", (
        "structural_gate_dropped_instruction_only must trigger a "
        "strategy retry (so the harness can try a different lever / "
        "RCA cluster) instead of skip_productive (which consumes "
        f"budget without retry). Got next_step={result.next_step!r}."
    )
    assert result.add_to_forbidden_set is True, (
        "The retired signature must still be admitted to the "
        "forbidden set so the same instruction-only shape is not "
        "regenerated in subsequent iterations."
    )


def test_proposal_generation_empty_already_triggers_retry_switch():
    """Regression pin: ``PROPOSAL_GENERATION_EMPTY`` already triggers
    retry. This test exists so Commit 2's intent is captured next to
    the new contract — every pre-applyability empty/dropped path
    should retry in-iteration.
    """
    sig = _sig(TerminalReason.PROPOSAL_GENERATION_EMPTY)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=0,
        iteration_budget=5,
    )
    assert result.next_step == "retry_strategy_switch"
    assert result.add_to_forbidden_set is True


def test_no_structural_candidate_already_triggers_retry_switch():
    """Regression pin: ``NO_STRUCTURAL_CANDIDATE`` already triggers
    retry. Aligned with the above — every pre-applyability empty path
    is now uniformly retry, not skip.
    """
    sig = _sig(TerminalReason.NO_STRUCTURAL_CANDIDATE)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.NO_STRUCTURAL_CANDIDATE,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=0,
        iteration_budget=5,
    )
    assert result.next_step == "retry_strategy_switch"
    assert result.add_to_forbidden_set is True


def test_structural_gate_drop_collapses_to_abort_at_budget_boundary():
    """The budget-boundary rule still applies: at the last iteration,
    retry collapses to abort_run. This pin protects against an
    accidental loop when budget is exhausted but the routing says
    retry.
    """
    sig = _sig(TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY)
    result = decide_iteration_terminal_action(
        terminal_reason=TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY,
        signature=sig,
        prior_forbidden_set=frozenset(),
        iteration_index=4,  # last iteration (budget=5)
        iteration_budget=5,
    )
    assert result.next_step == "abort_run", (
        "Budget-boundary rule (spec Section 12.6) must collapse "
        f"retry_strategy_switch to abort_run. Got {result.next_step!r}."
    )
