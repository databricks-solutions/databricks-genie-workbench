"""QuestionStateInIteration — the canonical per-QID per-iteration state object.

This is THE mutable artifact of the optimizer state machine. By design,
only the state_machine package may construct instances (Phase 5 AST lint
SM4 will enforce this). All transformations are immutable: ``advance``
returns a NEW state; the input is never mutated.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
    is_legal_transition,
    stage_index,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
    AppliedRecord,
    ClusterMembershipRecord,
    DiagnosisRecord,
    EvaluatedRecord,
    HardQidSeenRecord,
    ProposalAttempt,
    StageTransition,
    TerminalRecord,
)


@dataclass(frozen=True, slots=True)
class QuestionStateInIteration(JsonRoundTrip):
    qid: str
    iteration: int
    current_stage: FunnelStage
    deepest_stage_reached: FunnelStage

    seen: HardQidSeenRecord
    diagnosed: DiagnosisRecord | None = None
    clustered: ClusterMembershipRecord | None = None
    proposals: tuple[ProposalAttempt, ...] = ()
    applied: AppliedRecord | None = None
    evaluated: EvaluatedRecord | None = None
    accepted: AcceptanceDecisionRecord | None = None
    terminal: TerminalRecord | None = None

    transitions: tuple[StageTransition, ...] = ()

    def advance(
        self,
        to_stage: FunnelStage,
        transition: StageTransition,
        **record_updates: Any,
    ) -> "QuestionStateInIteration":
        """Return a new state at ``to_stage``, attaching the transition and any record updates."""
        if not is_legal_transition(self.current_stage, to_stage):
            raise ValueError(
                f"illegal transition {self.current_stage} -> {to_stage} for qid={self.qid}"
            )
        if transition.from_stage != self.current_stage or transition.to_stage != to_stage:
            raise ValueError(
                f"transition stages mismatch state: state {self.current_stage}->{to_stage}, "
                f"transition {transition.from_stage}->{transition.to_stage}"
            )
        new_deepest = self._max_stage(self.deepest_stage_reached, to_stage)
        return replace(
            self,
            current_stage=to_stage,
            deepest_stage_reached=new_deepest,
            transitions=self.transitions + (transition,),
            **record_updates,
        )

    def terminate(
        self,
        transition: StageTransition,
        terminal: TerminalRecord,
    ) -> "QuestionStateInIteration":
        """Return a new state at TERMINATED with the given TerminalRecord attached."""
        if self.current_stage == FunnelStage.TERMINATED:
            raise ValueError(f"state for qid={self.qid} is already TERMINATED")
        if transition.to_stage != FunnelStage.TERMINATED:
            raise ValueError("terminate() requires transition.to_stage == TERMINATED")
        return replace(
            self,
            current_stage=FunnelStage.TERMINATED,
            terminal=terminal,
            transitions=self.transitions + (transition,),
            # deepest_stage_reached intentionally NOT bumped to TERMINATED;
            # deepest tracks meaningful funnel depth, not absorbing state.
        )

    @staticmethod
    def _max_stage(a: FunnelStage, b: FunnelStage) -> FunnelStage:
        # TERMINATED never counts as deepest (it's an absorbing state, not a depth).
        if a == FunnelStage.TERMINATED:
            return b
        if b == FunnelStage.TERMINATED:
            return a
        return a if stage_index(a) >= stage_index(b) else b


def build_initial_state(
    *,
    qid: str,
    iteration: int,
    seen: HardQidSeenRecord,
) -> QuestionStateInIteration:
    """Construct a fresh QuestionStateInIteration at HARD_QID_SEEN.

    This is the ONE allowed constructor entry point outside ``advance``/
    ``terminate``. Phase 5 AST lint SM4 ensures every other constructor
    call lives in the state_machine package.
    """
    return QuestionStateInIteration(
        qid=qid,
        iteration=iteration,
        current_stage=FunnelStage.HARD_QID_SEEN,
        deepest_stage_reached=FunnelStage.HARD_QID_SEEN,
        seen=seen,
    )
