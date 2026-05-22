"""StateMachine — the orchestrator. Routes states through transformers. Emits witness markers."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.markers import (
    qstate_transition_marker,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformer import (
    StateTransformer,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
)


@dataclass(frozen=True)
class StateMachine:
    """The orchestrator. Owns the transformer registry and routes step-by-step."""
    transformers: Mapping[FunnelStage, tuple[StateTransformer, ...]]

    def step(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration:
        """Apply transformers registered for ``state.current_stage``; emit witness markers.

        Multiple transformers may be registered at a single stage (e.g.
        ``structural_repair_gate`` then ``escalation_ladder`` at PROPOSED).
        They run in registration order, but the loop **breaks** as soon
        as the state's stage diverges from the original — the
        downstream transformers are registered for a stage the state
        is no longer at, so calling them would route incorrectly. The
        next ``step()`` invocation picks up the new stage and applies
        its registered transformers.
        """
        original_stage = state.current_stage
        registered = self.transformers.get(original_stage, ())
        for transformer in registered:
            new_state = transformer.transform(state, ctx)
            # Find the newly appended transitions (always exactly one per
            # transform call by contract); emit a witness marker for each.
            new_transitions = new_state.transitions[len(state.transitions):]
            for t in new_transitions:
                print(
                    qstate_transition_marker(
                        run_id=ctx.run_id,
                        iteration=ctx.iteration,
                        qid=state.qid,
                        transition=t,
                    ),
                    flush=True,
                )
            state = new_state
            if state.current_stage == FunnelStage.TERMINATED:
                return state
            if state.current_stage != original_stage:
                # Stage advanced; the remaining transformers in this
                # iteration are registered for the *original* stage and
                # would route incorrectly. The next ``step()`` looks up
                # transformers for the new stage and applies them.
                return state
        return state

    def run_until_settled(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
        *,
        max_iterations: int = 32,
    ) -> QuestionStateInIteration:
        """Repeatedly call step() until state stops advancing or terminates."""
        for _ in range(max_iterations):
            prior_stage = state.current_stage
            state = self.step(state, ctx)
            if state.current_stage == FunnelStage.TERMINATED:
                return state
            if state.current_stage == prior_stage:
                return state
        return state

    def run_iteration(
        self,
        states: tuple[QuestionStateInIteration, ...],
        ctx: TransformerContext,
    ) -> tuple[QuestionStateInIteration, ...]:
        """Drive each state to settled (terminal or fixed-point); return the batch."""
        return tuple(self.run_until_settled(s, ctx) for s in states)
