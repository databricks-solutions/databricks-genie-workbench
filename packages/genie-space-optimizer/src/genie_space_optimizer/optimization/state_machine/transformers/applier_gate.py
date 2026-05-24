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
    gate_reasoning_marker,
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
    """Adapter over ``applier.apply_patch_set``.

    1. Look up the typed ``RepairProposal`` in ``ctx.proposal_store``.
       Store miss → ``(call_id="", False, "proposal_store_miss:...")``.
    2. Stamp the proposal's ``patch_type`` on a copy of its
       ``patch_body`` (the applier dispatches on the ``patch_type``
       key inside each patch dict, not a separate arg).
    3. Call ``apply_patch_set`` with ``force_apply=True`` — the v3
       chain's structural + blast gates upstream already enforced
       the risk policy, so the applier's own risk-tier review is
       redundant here.
    4. Map the apply_log to ``(apply_call_id, succeeded, error_reason)``.
       ``apply_call_id`` is synthesized as
       ``apply_{iteration}_{intent_id}`` for the v3 audit trail
       (the legacy applier does not emit a call_id of its own).

    Test-stub override:
      When ``ctx.extras["applier"]`` is callable, it is invoked with
      ``(state, ctx, proposal)`` and expected to return the same
      ``(apply_call_id, succeeded, error_reason)`` tuple. Lets the
      synthetic anchor replay bypass the real Genie API.
    """
    if not state.proposals:
        return ("", False, "no_proposal_attempt_on_state")

    latest = state.proposals[-1]
    proposal = ctx.proposal_store.lookup(latest.intent_id)
    if proposal is None:
        return ("", False, f"proposal_store_miss:{latest.intent_id}")

    extras = getattr(ctx, "extras", {}) or {}
    stub = extras.get("applier") if extras else None
    apply_call_id = f"apply_{ctx.iteration}_{latest.intent_id}"
    if callable(stub):
        try:
            return stub(state=state, ctx=ctx, proposal=proposal)
        except TypeError:
            return stub()
    # When the test wires synthesize_llm but no live Genie client
    # (``ctx.w is None`` and ``ctx.space_id == ""``), short-circuit the
    # apply step to a deterministic success. The v3 contract is "apply
    # is a side effect we can mock out for synthetic replay"; the eval
    # / acceptance gates downstream consume the records, not the live
    # space state.
    if "synthesize_llm" in extras and ctx.w is None and not ctx.space_id:
        return (apply_call_id, True, "")

    patch_type_str = (
        proposal.patch_type.value
        if hasattr(proposal.patch_type, "value")
        else str(proposal.patch_type)
    )
    patch_dict = dict(proposal.patch_body)
    patch_dict.setdefault("patch_type", patch_type_str)
    patch_dict.setdefault("type", patch_type_str)
    patches = [patch_dict]

    try:
        from genie_space_optimizer.optimization.applier import (
            apply_patch_set,
        )
        apply_log = apply_patch_set(
            ctx.w,
            ctx.space_id,
            patches,
            dict(ctx.metadata_snapshot),
            force_apply=True,
        )
    except Exception as exc:  # pragma: no cover — exception path under test
        return (apply_call_id, False, f"applier_raised:{exc}")

    if apply_log.get("patch_deployed"):
        return (apply_call_id, True, "")

    # Trial 15 — surface typed ``ApplierDecision`` reasons instead of
    # the opaque ``apply_failed_no_reason`` sentinel.
    # ``apply_patch_set`` already emits a per-patch audit trail via
    # ``build_applier_decision`` (see applier_audit.py) with closed
    # ``ApplierDecisionStatus`` literals (``applied`` /
    # ``dropped_validation`` / ``dropped_dedupe`` / ``dropped_no_op`` /
    # ``dropped_exception``) plus a free-form ``reason`` and
    # ``error_excerpt``. Up to Trial 14 the gate ignored that audit
    # trail and defaulted to the sentinel, which is why every applier
    # rejection in the dc89d1a9 and 98ec8950 postmortems landed with
    # ``apply_failed_no_reason`` despite the underlying applier knowing
    # exactly why it dropped the patch.
    decisions = apply_log.get("applier_decisions") or []
    matching = None
    for decision_row in decisions:
        if not isinstance(decision_row, dict):
            continue
        decision_patch_type = str(
            decision_row.get("patch_type") or ""
        )
        if decision_patch_type and decision_patch_type == patch_type_str:
            matching = decision_row
            break
    if matching is not None:
        decision_label = str(matching.get("decision") or "unknown")
        reason_label = str(matching.get("reason") or "")
        composed = f"{decision_label}:{reason_label}" if reason_label else decision_label
        error_excerpt = str(matching.get("error_excerpt") or "")
        if error_excerpt:
            # Keep the surface short — gate_reasoning markers are
            # grepped from stdout in postmortems, so a 500-char excerpt
            # blowing up a single log line is more harmful than helpful.
            composed = f"{composed}:{error_excerpt[:200]}"
        return (apply_call_id, False, composed)

    # ``apply_patch_set`` returned ``patch_deployed=False`` without
    # populating ``applier_decisions`` AND without surfacing
    # ``patch_error`` / ``validation_errors``. After Trial 15 Part B2
    # audits the applier dispatchers, this branch should be unreachable
    # — operators grep for ``apply_no_decision_emitted`` to catch the
    # genuine "applier-side bug" case.
    error = (
        apply_log.get("patch_error")
        or (apply_log.get("validation_errors") or [""])[0]
        or "apply_no_decision_emitted"
    )
    return (apply_call_id, False, str(error))


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
        gate_reasoning_marker(
            gate="applier_gate",
            qid=state.qid,
            verdict="rejected",
            predicate_inputs={
                "intent_id": latest.intent_id,
                "patch_type": latest.patch_type,
                "apply_call_id": call_id,
                "apply_ok": ok,
            },
            reason=reason or "applier_failure",
        ),
        flush=True,
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
