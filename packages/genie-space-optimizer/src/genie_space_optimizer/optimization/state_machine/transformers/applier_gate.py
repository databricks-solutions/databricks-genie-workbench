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
    TerminalRecord,
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


# Trial 17 step 4 — metadata patch_types that benefit from a
# pre-applier resolvability check. These are the patch families that
# can cause ``dropped_no_op:missing_table`` /
# ``dropped_no_op:missing_column`` when the LLM proposed a target that
# does not exist in the current metadata snapshot. Preflighting them
# short-circuits the dead-end before the live applier is called.
_PREFLIGHT_METADATA_PATCH_TYPES = frozenset(
    {
        "add_column_description",
        "update_column_description",
        "add_column_synonym",
        "remove_column_synonym",
        "hide_column",
        "unhide_column",
        "rename_column_alias",
        "add_description",
        "update_description",
        "add_join_spec",
        "update_join_spec",
        "remove_join_spec",
    }
)


def _preflight_metadata_patch(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> tuple[bool, str, str]:
    """Trial 17 step 4 — return ``(applyable, reason, target_table)``.

    For metadata patches, dry-run ``check_patch_applyability`` against
    ``ctx.metadata_snapshot`` BEFORE invoking the live applier. When
    the target table/column is unresolvable, return ``(False,
    "preflight_target_missing", target_table)`` so the gate can
    short-circuit with a typed forbidden_signature the next iteration's
    LLM can pivot on.

    Returns ``(True, "", "")`` when:
      * The patch_type is not metadata-shaped (preflight not applicable).
      * The proposal cannot be resolved from the store.
      * ``ctx.metadata_snapshot`` is empty (e.g. tests / synthetic
        replays where the snapshot is intentionally unwired — fall
        through to the live applier).
      * The preflight passed (no target gap).
    """
    if not state.proposals:
        return (True, "", "")
    latest = state.proposals[-1]
    patch_type_str = str(latest.patch_type or "").lower()
    if patch_type_str not in _PREFLIGHT_METADATA_PATCH_TYPES:
        return (True, "", "")

    snapshot = getattr(ctx, "metadata_snapshot", None) or {}
    if not snapshot:
        # No snapshot wired — fall through to live applier; the
        # post-apply ``dropped_no_op`` path will still catch genuine
        # target misses (legacy behavior).
        return (True, "", "")

    proposal = ctx.proposal_store.lookup(latest.intent_id)
    if proposal is None:
        return (True, "", "")

    # Build a patch dict shaped like the applier dispatches on.
    pb = dict(getattr(proposal, "patch_body", {}) or {})
    pb.setdefault("patch_type", patch_type_str)
    pb.setdefault("type", patch_type_str)

    # Trial 17 — only short-circuit when the LLM explicitly named a
    # table in the patch_body. The legacy ``object_id``-based encoding
    # (``{"object_id": "t:c", ...}``) is opaque to
    # ``check_patch_applyability``; for those shapes we fall through
    # to the live applier where the dispatcher knows how to parse
    # ``object_id``. This keeps the preflight strictly additive: it
    # catches the gs_024-style ``patch_body["table"] = <missing>``
    # pattern from the postmortems without regressing patches that
    # encode their target differently.
    target_table = ""
    raw_table = pb.get("table") or pb.get("target")
    if isinstance(raw_table, str):
        target_table = raw_table.strip()
    if not target_table:
        return (True, "", "")

    from genie_space_optimizer.optimization.patch_applyability import (
        check_patch_applyability,
    )

    decision = check_patch_applyability(
        patch=pb,
        metadata_snapshot=snapshot,
        space_id=ctx.space_id,
    )
    if decision.applyable:
        return (True, "", "")

    # Translate the typed applyability reason to the Trial 17 preflight
    # vocabulary. We only short-circuit on target-resolution gaps —
    # other failures (render_exception, apply_exception) keep flowing
    # to the live applier where existing error handling already
    # surfaces them with full context.
    if decision.reason in {
        "missing_table",
        "invalid_column_target",
        "missing_column",
    }:
        return (
            False,
            "preflight_target_missing",
            decision.table or target_table,
        )
    return (True, "", "")


def _predicate(state: QuestionStateInIteration, ctx: TransformerContext) -> GateVerdict:
    latest = state.proposals[-1]

    # Trial 17 step 4 — preflight metadata patches before the live
    # apply call. If the target table/column doesn't exist in the
    # current metadata snapshot, short-circuit to a terminal with the
    # typed ``preflight_target_missing`` forbidden_signature.
    applyable, preflight_reason, preflight_table = _preflight_metadata_patch(
        state, ctx,
    )
    if not applyable:
        from genie_space_optimizer.optimization.levers_contract import (
            infer_lever_from_patch_type,
        )

        selected_lever = ""
        proposal = ctx.proposal_store.lookup(latest.intent_id)
        if proposal is not None:
            selected_lever = str(
                getattr(proposal, "selected_lever", "") or ""
            )
        if not selected_lever:
            selected_lever = infer_lever_from_patch_type(
                str(latest.patch_type)
            )

        rejected = ProposalAttempt(
            attempt_index=latest.attempt_index,
            intent_id=latest.intent_id,
            patch_type=latest.patch_type,
            deepest_stage_in_attempt=FunnelStage.APPLYABLE,
            outcome="applyability_rejected",
            outcome_reason=preflight_reason,
        )
        print(
            gate_reasoning_marker(
                gate="applier_gate",
                qid=state.qid,
                verdict="rejected",
                predicate_inputs={
                    "intent_id": latest.intent_id,
                    "patch_type": latest.patch_type,
                    "preflight": True,
                    "preflight_table": preflight_table,
                },
                reason=preflight_reason,
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
        sig_parts = [
            selected_lever or "?",
            str(latest.patch_type or "?"),
            preflight_reason,
        ]
        if preflight_table:
            sig_parts.append(f"table={preflight_table}")
        forbidden_signature = ":".join(sig_parts)
        terminal = TerminalRecord(
            kind="OPTIMIZER_STALLED_SAFE_NOOP",
            reason=f"applyability_rejected:{preflight_reason}",
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature=forbidden_signature,
        )
        return GateVerdict.reject_terminal(terminal)

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

    # Failure: surface a typed ProposalAttempt with applyability_rejected,
    # *then* terminate with a typed forbidden_signature the strategist
    # can learn from.
    #
    # Trial 16 RC3 — up to Trial 15 the gate returned
    # ``GateVerdict.reject_proposal(rejected)`` and ``to_stage_on_reject``
    # was ``FunnelStage.PROPOSED``, so applier no-op rejections cycled
    # back to PROPOSED and the synthesize LLM regenerated the same
    # dead-end proposal up to 32× per qid (postmortem evidence on
    # gs_024 + others). Now we terminate the qid with a typed
    # ``forbidden_signature`` so the next iteration's
    # ``cluster_batch._predicate`` (via ``ctx.forbidden_signatures``)
    # sees the dead end and the strategist's lever choice avoids it.
    #
    # The forbidden_signature shape matches the legacy
    # ``escalation_ladder`` rung-5 terminal:
    #   ``<patch_type>:<applier_reason>`` where applier_reason is the
    # composed ``decision:reason[:error_excerpt]`` already produced by
    # the Trial 15 typed-decision surface above.
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
    composed_reason = reason or "applier_failure"
    # Trial 17 step 3b — enrich the forbidden_signature with the
    # ``selected_lever`` and (where applicable) the ``table=<X>``
    # token so the next iteration's LLM has the full pivot context.
    # ``selected_lever`` comes from the proposal_store; for legacy
    # proposals lacking the field we infer it from patch_type via
    # ``levers_contract.infer_lever_from_patch_type``.
    from genie_space_optimizer.optimization.levers_contract import (
        infer_lever_from_patch_type,
    )

    selected_lever = ""
    target_table = ""
    proposal = ctx.proposal_store.lookup(latest.intent_id)
    if proposal is not None:
        selected_lever = str(
            getattr(proposal, "selected_lever", "") or ""
        )
        # Surface the patch_body target table when present — only
        # used in the signature when the reason mentions missing_table.
        pb = getattr(proposal, "patch_body", None) or {}
        if isinstance(pb, dict):
            target_table = str(pb.get("table") or pb.get("target") or "")
    if not selected_lever:
        selected_lever = infer_lever_from_patch_type(
            str(latest.patch_type)
        )

    sig_parts: list[str] = [
        selected_lever or "?",
        str(latest.patch_type or "?"),
        composed_reason,
    ]
    if "missing_table" in composed_reason and target_table:
        sig_parts.append(f"table={target_table}")
    forbidden_signature = ":".join(sig_parts)
    terminal = TerminalRecord(
        kind="OPTIMIZER_STALLED_SAFE_NOOP",
        reason=f"applyability_rejected:{composed_reason}",
        deepest_stage_reached=state.deepest_stage_reached,
        forbidden_signature=forbidden_signature,
    )
    return GateVerdict.reject_terminal(terminal)


applier_gate = ValidationGate(
    name="applier_gate",
    from_stage=FunnelStage.APPLYABLE,
    to_stage_on_success=FunnelStage.APPLIED,
    # Trial 16 RC3 — was FunnelStage.PROPOSED (escalation cycle).
    # The cycle caused applier no-op rejections to recycle the same
    # dead-end proposal up to 32× per qid before max_iterations
    # consumed the budget. Now no-ops terminate immediately and the
    # typed ``forbidden_signature`` on the TerminalRecord routes
    # through ``ctx.forbidden_signatures`` to the next-iteration
    # strategist so the dead end is avoided rather than re-attempted.
    # Note: ``ValidationGate.transform`` honors ``TerminalRecord``
    # rejections by routing to FunnelStage.TERMINATED regardless of
    # this field; setting it to TERMINATED keeps the documented gate
    # contract (``to_stage_on_reject`` matches actual reject target)
    # in sync.
    to_stage_on_reject=FunnelStage.TERMINATED,
    predicate=_predicate,
)
