"""Structural repair gate as ValidationGate.

Wraps the existing structural repair check. Reads the latest
ProposalAttempt + the typed RepairProposal it points at. On success
advances to NORMALIZED. On failure cycles back to PROPOSED with a
typed ``structural_repair_rejected`` ProposalAttempt; the Phase 3
``escalation_ladder`` is registered on the same PROPOSED stage to
take over once the legacy gate has rejected.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ProposalAttempt,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformer import (
    ValidationGate,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    GateVerdict,
    TransformerContext,
)


def _proposal_passes_structural_check(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> tuple[bool, str]:
    """Wrap the legacy structural check.

    Returns ``(passed, rejection_reason)``. Patchable for tests.
    Production wire-in lands in PR 2.5 (routes to the legacy
    ``structural_repair_gate`` callsite from `optimizer.py`).
    """
    raise NotImplementedError(
        "Structural check not wired to production yet. "
        "Tests must monkeypatch _proposal_passes_structural_check."
    )


def _predicate(state: QuestionStateInIteration, ctx: TransformerContext) -> GateVerdict:
    passed, reason = _proposal_passes_structural_check(state, ctx)
    if passed:
        return GateVerdict.success()
    latest = state.proposals[-1]
    rejected_attempt = ProposalAttempt(
        attempt_index=latest.attempt_index,
        intent_id=latest.intent_id,
        patch_type=latest.patch_type,
        deepest_stage_in_attempt=FunnelStage.PROPOSED,
        outcome="structural_repair_rejected",
        outcome_reason=reason,
    )
    return GateVerdict.reject_proposal(rejected_attempt)


structural_repair_gate = ValidationGate(
    name="structural_repair_gate",
    from_stage=FunnelStage.PROPOSED,
    to_stage_on_success=FunnelStage.NORMALIZED,
    to_stage_on_reject=FunnelStage.PROPOSED,  # cycle back for escalation
    predicate=_predicate,
)
