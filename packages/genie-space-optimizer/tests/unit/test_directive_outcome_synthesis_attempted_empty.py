"""Phase 6.5 — when synthesis WAS attempted but returned no
candidate, the directive outcome must be SYNTHESIS_ATTEMPTED_EMPTY,
distinct from the pre-Phase-6 catch-all NO_STRUCTURAL_CANDIDATE
which covered both 'never attempted' and 'attempted and empty'.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.directive_outcome import (
    DirectiveOutcomeCode,
    LeverProposalSnapshot,
    classify_lever_proposal_outcome,
)


def test_zero_proposals_with_attempted_synthesis_returns_attempted_empty():
    snap = LeverProposalSnapshot(
        lever_key=5,
        proposals_emitted_count=0,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=False,
        attempted_synthesis=True,
    )
    assert classify_lever_proposal_outcome(snap) == (
        DirectiveOutcomeCode.SYNTHESIS_ATTEMPTED_EMPTY
    )


def test_zero_proposals_without_attempted_synthesis_returns_no_candidate():
    snap = LeverProposalSnapshot(
        lever_key=5,
        proposals_emitted_count=0,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=False,
        attempted_synthesis=False,
    )
    assert classify_lever_proposal_outcome(snap) == (
        DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE
    )


def test_proposals_emitted_still_classifies_as_emitted():
    """Sanity — Phase 6.5 only affects the zero-proposal branch."""
    snap = LeverProposalSnapshot(
        lever_key=5,
        proposals_emitted_count=2,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=False,
        attempted_synthesis=True,
    )
    assert classify_lever_proposal_outcome(snap) == (
        DirectiveOutcomeCode.PROPOSAL_EMITTED
    )


def test_attempted_synthesis_field_defaults_to_false():
    """Backwards compat: existing call sites that don't pass the new
    field must default to False (pre-Phase-6 'NO_STRUCTURAL_CANDIDATE'
    semantics)."""
    snap = LeverProposalSnapshot(
        lever_key=5,
        proposals_emitted_count=0,
        structural_gate_drop_count=0,
        applyability_drop_count=0,
        collateral_drop_count=0,
        force_llm_declined=False,
        # attempted_synthesis omitted — defaults to False
    )
    assert classify_lever_proposal_outcome(snap) == (
        DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE
    )
