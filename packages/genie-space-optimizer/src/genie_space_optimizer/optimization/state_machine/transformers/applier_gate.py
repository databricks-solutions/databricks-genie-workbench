"""Applier gate as ValidationGate wrapping the Genie API side effect.

APPLYABLE → APPLIED on success; APPLYABLE → PROPOSED on
applyability_rejected (caller-side rejection so the escalation_ladder
can try a narrower artifact).

Witnesses the apply outcome via ``GSO_PATCH_OUTCOME_V1`` derived from
the ProposalAttempt that just landed. This is the Phase 2 wire-in of
the marker that Plan 12 PR 3 introduced; the state machine is now the
sole emitter for outcomes the orchestrator observes.
"""
from __future__ import annotations

import time

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.markers import (
    patch_outcome_marker_from_attempt,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AppliedRecord,
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


def _apply_via_genie_api(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> tuple[str, bool, str]:
    """Wrap the legacy applier; patchable for tests.

    Returns ``(apply_call_id, succeeded, error_reason)``. Production
    wire-in lands in PR 2.5 — routes to ``applier.apply_patch_set``.
    """
    raise NotImplementedError(
        "Genie API apply not wired to production yet. "
        "Tests must monkeypatch _apply_via_genie_api."
    )


def _predicate(state: QuestionStateInIteration, ctx: TransformerContext) -> GateVerdict:
    latest = state.proposals[-1]
    call_id, ok, reason = _apply_via_genie_api(state, ctx)
    if ok:
        applied = AppliedRecord(
            applied_at_ms=int(time.time() * 1000),
            apply_call_id=call_id,
            proposal_attempt_index=latest.attempt_index,
            applied_intent_ids=(latest.intent_id,),
        )
        # Witness the now-applied attempt via GSO_PATCH_OUTCOME_V1.
        applied_attempt = ProposalAttempt(
            attempt_index=latest.attempt_index,
            intent_id=latest.intent_id,
            patch_type=latest.patch_type,
            deepest_stage_in_attempt=FunnelStage.APPLIED,
            outcome="applied",
            outcome_reason="applied_ok",
            patch_outcome_id=call_id,
        )
        print(
            patch_outcome_marker_from_attempt(
                run_id=ctx.run_id, iteration=ctx.iteration,
                qid=state.qid, attempt=applied_attempt,
            ),
            flush=True,
        )
        return GateVerdict.success(record=applied)

    # Failure: surface a typed ProposalAttempt with applyability_rejected.
    rejected = ProposalAttempt(
        attempt_index=latest.attempt_index,
        intent_id=latest.intent_id,
        patch_type=latest.patch_type,
        deepest_stage_in_attempt=FunnelStage.APPLYABLE,
        outcome="applyability_rejected",
        outcome_reason=reason or "applier_failure",
    )
    print(
        patch_outcome_marker_from_attempt(
            run_id=ctx.run_id, iteration=ctx.iteration,
            qid=state.qid, attempt=rejected,
        ),
        flush=True,
    )
    return GateVerdict.reject_proposal(rejected)


applier_gate = ValidationGate(
    name="applier_gate",
    from_stage=FunnelStage.APPLYABLE,
    to_stage_on_success=FunnelStage.APPLIED,
    to_stage_on_reject=FunnelStage.PROPOSED,  # escalation cycle
    predicate=_predicate,
)
