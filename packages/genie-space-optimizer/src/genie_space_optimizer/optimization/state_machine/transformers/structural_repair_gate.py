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


_STRUCTURAL_PATCH_TYPE_TAGS: tuple[str, ...] = (
    "metric_view", "example_sql", "narrow_l6",
    "join", "routing", "grain", "sql_pattern",
)


def _patch_type_to_intended_shape(patch_type: str) -> str:
    """Map a ``RepairProposal.patch_type`` value to the ``intended_patch_shape``
    string the legacy ``enforce_structural_repair_shape`` consumes.

    Uses the same substring-tag heuristic as the legacy
    ``resolve_emitted_patch_shape`` so intended vs emitted comparisons
    are symmetric.
    """
    p = (patch_type or "").lower()
    if any(tag in p for tag in _STRUCTURAL_PATCH_TYPE_TAGS):
        return "structural"
    return "non_structural"


def _proposal_passes_structural_check(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> tuple[bool, str]:
    """Adapter over ``optimization.structural_repair_gate.enforce_structural_repair_shape``.

    1. Look up the typed ``RepairProposal`` in ``ctx.proposal_store``
       by the latest ``ProposalAttempt.intent_id``. Missing → gate
       rejects with ``proposal_store_miss``.
    2. Derive ``intended_patch_shape`` from the proposal's
       ``patch_type`` via the substring-tag heuristic.
    3. Classify ``emitted_patch_shape`` via the legacy
       ``resolve_emitted_patch_shape`` over the proposal's
       ``patch_body`` projected with a ``patch_type`` key.
    4. Call ``enforce_structural_repair_shape`` and map the verdict.
    """
    if not state.proposals:
        return (False, "no_proposal_attempt_on_state")

    latest = state.proposals[-1]
    proposal = ctx.proposal_store.lookup(latest.intent_id)
    if proposal is None:
        return (False, f"proposal_store_miss:{latest.intent_id}")

    # Lazy imports to avoid circular references at module load.
    from genie_space_optimizer.optimization.structural_repair_gate import (
        enforce_structural_repair_shape,
    )
    from genie_space_optimizer.optimization.terminal_signature import (
        resolve_emitted_patch_shape,
    )

    patch_type_str = (
        proposal.patch_type.value
        if hasattr(proposal.patch_type, "value")
        else str(proposal.patch_type)
    )
    intended = _patch_type_to_intended_shape(patch_type_str)
    emitted = resolve_emitted_patch_shape([
        {"patch_type": patch_type_str},
    ])

    verdict = enforce_structural_repair_shape(
        intended_patch_shape=intended,
        emitted_patch_shape=emitted,
        narrow_replacement_available=False,
    )
    if verdict.outcome == "admitted":
        return (True, "")
    return (False, verdict.terminal_reason or "structural_repair_rejected")


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
