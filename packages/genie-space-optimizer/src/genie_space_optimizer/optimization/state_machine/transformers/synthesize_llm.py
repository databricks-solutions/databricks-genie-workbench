"""Stage 3 synthesis as LlmStateTransformer.

Returns a typed RepairProposal; writes a ProposalAttempt onto state.proposals.
Validates the Phase 1 contract (validate_synthesis_output_for_state_machine)
at exit so contract failures are visible as typed terminals — never silent.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ProposalAttempt,
    StageTransition,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
)


def _invoke_stage3_llm(state: QuestionStateInIteration, ctx: TransformerContext):
    """Dispatch the actual Stage 3 LLM call (patchable for tests).

    Production wire-in lands in PR 2.5 via
    ``stages.synthesize.run_plan11_synthesis_for_single_cluster``.
    """
    raise NotImplementedError(
        "Stage 3 LLM dispatch not yet wired to production lever loop. "
        "Tests must monkeypatch _invoke_stage3_llm with a fake RepairProposal."
    )


def _repair_proposal_to_dict(rp) -> dict:
    return {
        "intent_id": getattr(rp, "intent_id", ""),
        "patch_type": getattr(rp, "patch_type", ""),
        "target_objects": tuple(getattr(rp, "target_objects", ())),
        "target_qids": tuple(getattr(rp, "target_qids", ())),
        "rca_card_id": getattr(rp, "rca_card_id", ""),
        "causal_target": getattr(rp, "causal_target", ""),
        "original_patch_body": getattr(rp, "original_patch_body", ""),
    }


def _terminate_no_candidates(state: QuestionStateInIteration, name: str, reason: str):
    return state.terminate(
        transition=StageTransition(
            from_stage=state.current_stage,
            to_stage=FunnelStage.TERMINATED,
            at_ms=int(time.time() * 1000),
            transformer_name=name,
            transition_kind="llm",
            reason=reason,
        ),
        terminal=TerminalRecord(
            kind="OPTIMIZER_NO_CANDIDATES",
            reason=reason,
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature="",
        ),
    )


def _terminate_invariant(
    state: QuestionStateInIteration,
    name: str,
    failed_attempt: ProposalAttempt,
):
    return state.terminate(
        transition=StageTransition(
            from_stage=state.current_stage,
            to_stage=FunnelStage.TERMINATED,
            at_ms=int(time.time() * 1000),
            transformer_name=name,
            transition_kind="llm",
            reason=failed_attempt.outcome_reason,
            proposal_attempt_index=failed_attempt.attempt_index,
        ),
        terminal=TerminalRecord(
            kind="OPTIMIZER_INVARIANT_VIOLATION",
            reason=failed_attempt.outcome_reason,
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature="",
        ),
    )


@dataclass(frozen=True, slots=True)
class _Plan11Stage3Transformer:
    name: str = "plan11_stage3_synthesis"
    from_stage: FunnelStage = FunnelStage.CLUSTERED
    to_stage_on_success: FunnelStage = FunnelStage.PROPOSED
    to_stage_on_reject: FunnelStage = FunnelStage.TERMINATED

    def transform(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration:
        proposal = _invoke_stage3_llm(state, ctx)
        if proposal is None:
            return _terminate_no_candidates(state, self.name, "stage3_returned_none")

        # Validate the Phase 1 state-machine contract — non-empty intent_id,
        # target_qids, rca_card_id, causal_target, original_patch_body, etc.
        # Failure surfaces as a typed terminal, never silent.
        from genie_space_optimizer.optimization.stages.synthesize import (
            StageThreeContractError,
            validate_synthesis_output_for_state_machine,
        )
        try:
            validate_synthesis_output_for_state_machine(
                _repair_proposal_to_dict(proposal),
            )
        except StageThreeContractError as e:
            attempt = ProposalAttempt(
                attempt_index=len(state.proposals),
                intent_id=getattr(proposal, "intent_id", "") or "unknown",
                patch_type=getattr(proposal, "patch_type", "") or "unknown",
                deepest_stage_in_attempt=FunnelStage.PROPOSED,
                outcome="contract_failed",
                outcome_reason=str(e),
            )
            return _terminate_invariant(state, self.name, attempt)

        # In-flight sentinel: outcome="applied" is the pre-terminal placeholder
        # downstream gates overwrite to the real outcome. SM7 (escalated_to
        # _attempt_index iff outcome=='escalated') tolerates this because
        # outcome is "applied" not "escalated" here.
        attempt = ProposalAttempt(
            attempt_index=len(state.proposals),
            intent_id=str(proposal.intent_id),
            patch_type=str(proposal.patch_type),
            deepest_stage_in_attempt=FunnelStage.PROPOSED,
            outcome="applied",
            outcome_reason="pending_gates",
        )

        return state.advance(
            to_stage=self.to_stage_on_success,
            transition=StageTransition(
                from_stage=self.from_stage,
                to_stage=self.to_stage_on_success,
                at_ms=int(time.time() * 1000),
                transformer_name=self.name,
                transition_kind="llm",
                proposal_attempt_index=attempt.attempt_index,
            ),
            proposals=state.proposals + (attempt,),
        )


plan11_stage3_synthesis = _Plan11Stage3Transformer()
