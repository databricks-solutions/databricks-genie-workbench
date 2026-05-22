"""Blast-radius assessment as BatchTransformer.

NORMALIZED → APPLYABLE on safe; NORMALIZED → PROPOSED with a typed
``blast_radius_rejected`` ProposalAttempt on collateral risk. Phase 3
escalation_ladder picks up the cycled-back state to try a narrower
artifact.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ProposalAttempt,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
)


def _assess_blast_radius(
    state: QuestionStateInIteration,
    all_normalized: tuple[QuestionStateInIteration, ...],
) -> tuple[str, object | None]:
    """Wrap the legacy blast_radius assessor.

    Returns ``("safe", None)`` or ``("reject", BlastRadiusDropRecord)``.
    Patchable for tests. Production wire-in lands in PR 2.5.
    """
    raise NotImplementedError(
        "Blast-radius assessment not wired to production yet. "
        "Tests must monkeypatch _assess_blast_radius."
    )


@dataclass(frozen=True, slots=True)
class _BlastRadiusBatchTransformer:
    name: str = "blast_radius_batch"
    from_stage: FunnelStage = FunnelStage.NORMALIZED
    to_stage_on_success: FunnelStage = FunnelStage.APPLYABLE
    to_stage_on_reject: FunnelStage = FunnelStage.PROPOSED

    def transform_batch(
        self,
        states: tuple[QuestionStateInIteration, ...],
        ctx: TransformerContext,
    ) -> tuple[QuestionStateInIteration, ...]:
        out: list[QuestionStateInIteration] = []
        now_ms = int(time.time() * 1000)
        for s in states:
            verdict, drop = _assess_blast_radius(s, states)
            if verdict == "safe":
                out.append(s.advance(
                    to_stage=self.to_stage_on_success,
                    transition=StageTransition(
                        from_stage=self.from_stage,
                        to_stage=self.to_stage_on_success,
                        at_ms=now_ms,
                        transformer_name=self.name,
                        transition_kind="batch",
                    ),
                ))
                continue
            latest = s.proposals[-1]
            # Build the rejection-outcome reason; defensive on optional drop.
            collateral = tuple(getattr(drop, "collateral_qids", ()) or ())
            drop_intent = getattr(drop, "intent_id", "") if drop else ""
            rejected = ProposalAttempt(
                attempt_index=latest.attempt_index,
                intent_id=latest.intent_id,
                patch_type=latest.patch_type,
                deepest_stage_in_attempt=FunnelStage.APPLYABLE,  # blocked promotion
                outcome="blast_radius_rejected",
                outcome_reason=(
                    f"collateral={collateral} drop_record_id={drop_intent}"
                ),
            )
            out.append(s.advance(
                to_stage=self.to_stage_on_reject,
                transition=StageTransition(
                    from_stage=self.from_stage,
                    to_stage=self.to_stage_on_reject,
                    at_ms=now_ms,
                    transformer_name=self.name,
                    transition_kind="batch",
                    proposal_attempt_index=latest.attempt_index,
                    reason=rejected.outcome_reason,
                ),
                proposals=s.proposals + (rejected,),
            ))
        return tuple(out)


blast_radius_batch = _BlastRadiusBatchTransformer()
