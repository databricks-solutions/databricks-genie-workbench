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


@dataclass(frozen=True, slots=True)
class _BlastRadiusDrop:
    """Adapter shape consumed by ``transform_batch``'s rejection branch.

    ``collateral_qids`` carries the ``passing_dependents_outside_target``
    list the legacy gate emits; ``intent_id`` is the rejected proposal's id.
    """
    intent_id: str
    collateral_qids: tuple[str, ...]
    reason: str


def _assess_blast_radius(
    state: QuestionStateInIteration,
    all_normalized: tuple[QuestionStateInIteration, ...],
    ctx: TransformerContext | None = None,
) -> tuple[str, object | None]:
    """Adapter over ``proposal_grounding.patch_blast_radius_is_safe``.

    Looks up the typed ``RepairProposal`` in ``ctx.proposal_store`` to
    derive ``ag_target_qids`` and to read any ``passing_dependents`` /
    ``high_collateral_risk`` fields the harness counterfactual scanner
    stamped on the patch body. When the proposal carries no
    ``passing_dependents`` field, the legacy gate's
    ``no_passing_dependents_field`` safe-by-default path returns safe —
    Plan v3 iteration 1 takes that fallback because the counterfactual
    scanner is not yet plumbed through the state machine ctx.

    Returns ``("safe", None)`` or ``("reject", _BlastRadiusDrop)``.
    """
    if not state.proposals:
        return ("reject", _BlastRadiusDrop(
            intent_id="", collateral_qids=(),
            reason="no_proposal_attempt_on_state",
        ))

    latest = state.proposals[-1]
    proposal = None
    if ctx is not None:
        proposal = ctx.proposal_store.lookup(latest.intent_id)
    if proposal is None:
        return ("reject", _BlastRadiusDrop(
            intent_id=latest.intent_id, collateral_qids=(),
            reason=f"proposal_store_miss:{latest.intent_id}",
        ))

    # Lazy import to avoid heavy module-load chains.
    from genie_space_optimizer.optimization.proposal_grounding import (
        patch_blast_radius_is_safe,
    )

    patch_dict = dict(proposal.patch_body)
    patch_dict.setdefault(
        "patch_type",
        proposal.patch_type.value
        if hasattr(proposal.patch_type, "value")
        else str(proposal.patch_type),
    )

    live_hard = (
        tuple(ctx.live_hard_qids) if ctx is not None else ()
    ) or None

    verdict = patch_blast_radius_is_safe(
        patch_dict,
        ag_target_qids=tuple(proposal.target_qids),
        live_hard_qids=live_hard,
    )

    if verdict.get("safe", False):
        return ("safe", None)
    outside = tuple(
        str(q) for q in (
            verdict.get("passing_dependents_outside_target") or ()
        )
    )
    return ("reject", _BlastRadiusDrop(
        intent_id=latest.intent_id,
        collateral_qids=outside,
        reason=str(verdict.get("reason", "blast_radius_rejected")),
    ))


@dataclass(frozen=True, slots=True)
class _BlastRadiusBatchTransformer:
    name: str = "blast_radius_batch"
    from_stage: FunnelStage = FunnelStage.NORMALIZED
    to_stage_on_success: FunnelStage = FunnelStage.APPLYABLE
    to_stage_on_reject: FunnelStage = FunnelStage.PROPOSED

    def transform(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration:
        """Single-state adapter so the orchestrator's per-state
        ``step()`` can call this BatchTransformer."""
        out = self.transform_batch((state,), ctx)
        return out[0]

    def transform_batch(
        self,
        states: tuple[QuestionStateInIteration, ...],
        ctx: TransformerContext,
    ) -> tuple[QuestionStateInIteration, ...]:
        out: list[QuestionStateInIteration] = []
        now_ms = int(time.time() * 1000)
        for s in states:
            verdict, drop = _assess_blast_radius(s, states, ctx)
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
            drop_reason = getattr(drop, "reason", "") if drop else ""
            rejected = ProposalAttempt(
                attempt_index=latest.attempt_index,
                intent_id=latest.intent_id,
                patch_type=latest.patch_type,
                deepest_stage_in_attempt=FunnelStage.APPLYABLE,  # blocked promotion
                outcome="blast_radius_rejected",
                outcome_reason=(
                    f"reason={drop_reason} collateral={collateral} "
                    f"drop_record_id={drop_intent}"
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
