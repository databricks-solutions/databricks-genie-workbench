"""Transformer protocols: the ONLY way to mutate QuestionStateInIteration."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol

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
    GateVerdict,
    TransformerContext,
)


class StateTransformer(Protocol):
    name: str
    from_stage: FunnelStage
    to_stage_on_success: FunnelStage
    to_stage_on_reject: FunnelStage

    def transform(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration: ...


@dataclass(frozen=True, slots=True)
class LlmStateTransformer:
    """Reasoning step. Wraps exactly one LLM call."""
    name: str
    from_stage: FunnelStage
    to_stage_on_success: FunnelStage
    to_stage_on_reject: FunnelStage
    invoke: Callable[[QuestionStateInIteration, TransformerContext], Any]
    record_builder: Callable[[QuestionStateInIteration, Any], dict[str, Any]]

    def transform(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration:
        result = self.invoke(state, ctx)
        record_updates = self.record_builder(state, result)
        transition = StageTransition(
            from_stage=self.from_stage,
            to_stage=self.to_stage_on_success,
            at_ms=int(time.time() * 1000),
            transformer_name=self.name,
            transition_kind="llm",
        )
        return state.advance(
            to_stage=self.to_stage_on_success,
            transition=transition,
            **record_updates,
        )


@dataclass(frozen=True, slots=True)
class ValidationGate:
    """Deterministic check. Pure function modulo witness marker (emitted by orchestrator)."""
    name: str
    from_stage: FunnelStage
    to_stage_on_success: FunnelStage
    to_stage_on_reject: FunnelStage
    predicate: Callable[[QuestionStateInIteration, TransformerContext], GateVerdict]

    def transform(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration:
        verdict = self.predicate(state, ctx)
        now_ms = int(time.time() * 1000)
        if verdict.passed:
            transition = StageTransition(
                from_stage=self.from_stage,
                to_stage=self.to_stage_on_success,
                at_ms=now_ms,
                transformer_name=self.name,
                transition_kind="validation_gate",
            )
            record_updates: dict[str, Any] = {}
            if verdict.success_record is not None:
                # Caller-provided record gets attached as-is; orchestrator-level
                # validation ensures the type matches the target stage's slot.
                record_updates = _record_updates_for_stage(
                    self.to_stage_on_success, verdict.success_record,
                )
            return state.advance(
                to_stage=self.to_stage_on_success,
                transition=transition,
                **record_updates,
            )

        # Rejection: either escalate (rejection_outcome is ProposalAttempt → back to PROPOSED)
        # or terminate (rejection_outcome is TerminalRecord → TERMINATED).
        rejection = verdict.rejection_outcome
        if isinstance(rejection, TerminalRecord):
            transition = StageTransition(
                from_stage=self.from_stage,
                to_stage=FunnelStage.TERMINATED,
                at_ms=now_ms,
                transformer_name=self.name,
                transition_kind="validation_gate",
                reason=rejection.reason,
            )
            return state.terminate(transition=transition, terminal=rejection)
        if isinstance(rejection, ProposalAttempt):
            transition = StageTransition(
                from_stage=self.from_stage,
                to_stage=self.to_stage_on_reject,
                at_ms=now_ms,
                transformer_name=self.name,
                transition_kind="validation_gate",
                proposal_attempt_index=rejection.attempt_index,
                reason=rejection.outcome_reason,
            )
            return state.advance(
                to_stage=self.to_stage_on_reject,
                transition=transition,
                proposals=state.proposals + (rejection,),
            )
        raise TypeError(
            f"unsupported rejection_outcome type: {type(rejection).__name__}"
        )


class BatchTransformer(Protocol):
    """For transitions over QID sets (clustering, blast-radius across collateral)."""
    name: str
    from_stage: FunnelStage
    to_stage_on_success: FunnelStage
    to_stage_on_reject: FunnelStage

    def transform_batch(
        self,
        states: tuple[QuestionStateInIteration, ...],
        ctx: TransformerContext,
    ) -> tuple[QuestionStateInIteration, ...]: ...


def _record_updates_for_stage(stage: FunnelStage, record: Any) -> dict[str, Any]:
    """Map a stage-target to the QuestionStateInIteration field name."""
    mapping = {
        FunnelStage.DIAGNOSED: "diagnosed",
        FunnelStage.CLUSTERED: "clustered",
        FunnelStage.APPLIED: "applied",
        FunnelStage.EVALUATED: "evaluated",
        FunnelStage.ACCEPTED: "accepted",
    }
    field = mapping.get(stage)
    if field is None:
        return {}
    return {field: record}
