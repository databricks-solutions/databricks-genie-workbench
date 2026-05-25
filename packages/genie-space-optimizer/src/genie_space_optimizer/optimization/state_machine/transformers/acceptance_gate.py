"""Acceptance gate: EVALUATED → ACCEPTED, or roll back to TERMINATED.

Reads ``state.evaluated.{pre,post}_apply_score`` plus the collateral
assessment to decide. Acceptance requires both:
  * target_fixed (post > pre), AND
  * no collateral regressions.

Failure of either → ``OPTIMIZER_TRIED_NO_GAIN`` terminal, which the
postmortem renderers + run-outcome classifier treat as
"tried, applied, eval did not improve — clean rollback".
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.markers import (
    gate_reasoning_marker,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord, TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformer import ValidationGate
from genie_space_optimizer.optimization.state_machine.verdict import (
    GateVerdict, TransformerContext,
)


def _assess_collateral(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> tuple[str, ...]:
    """Compute per-QID collateral regression by comparing
    ``ctx.baseline_eval_rows`` (pre-apply) with
    ``ctx.post_apply_eval_rows`` (set by §H's eval step).

    A QID is collateral-regressed when:
      * It is *not* in the rejected proposal's ``target_qids`` (those
        are intentional fixes), AND
      * Its baseline ``feedback/result_correctness/value`` was > 0
        (was passing), AND
      * Its post-apply value is <= 0 (now failing).

    When either side has no rows (v3 iteration 1 without the harness
    plumbing), returns ``()`` — the acceptance gate falls back to
    target_fixed-only acceptance, which is the safer-side default.
    """
    baseline = ctx.baseline_eval_rows or ()
    post = ctx.post_apply_eval_rows or ()
    if not baseline or not post:
        return ()

    # Target QIDs are the intentional fixes — never count them as collateral.
    target_qids: set[str] = set()
    if state.proposals:
        latest = state.proposals[-1]
        proposal = ctx.proposal_store.lookup(latest.intent_id)
        if proposal is not None:
            target_qids = {str(q) for q in proposal.target_qids}
    target_qids.add(state.qid)  # always exclude the state's own target

    # Trial 16 RC2b — use the canonical extractor instead of the
    # strict ``r.get("question_id")`` lookup. Symmetric with RC2a in
    # ``evaluated_gate``: MLflow-flattened rows carry the qid under
    # ``inputs/question_id`` / ``inputs.question_id`` / nested inputs,
    # and the strict lookup mapped every such row to key ``""`` —
    # silently masking real collateral regressions.
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    def _score(row: dict) -> float:
        return float(row.get("feedback/result_correctness/value", 0.0) or 0.0)

    pre_by_qid = {
        extract_question_id(dict(r))[0]: _score(r) for r in baseline
    }
    post_by_qid = {
        extract_question_id(dict(r))[0]: _score(r) for r in post
    }
    regressed: list[str] = []
    for qid, pre_score in pre_by_qid.items():
        if qid in target_qids or not qid:
            continue
        if pre_score <= 0:
            continue  # was already failing; no regression
        post_score = post_by_qid.get(qid)
        if post_score is None:
            continue  # not re-evaluated post-apply — silent on regression
        if post_score <= 0:
            regressed.append(qid)
    return tuple(regressed)


def _predicate(state: QuestionStateInIteration, ctx: TransformerContext) -> GateVerdict:
    assert state.evaluated is not None, "acceptance_gate requires evaluated record"
    target_fixed = state.evaluated.post_apply_score > state.evaluated.pre_apply_score
    collateral = _assess_collateral(state, ctx)

    if target_fixed and not collateral:
        return GateVerdict.success(record=AcceptanceDecisionRecord(
            decision="accepted",
            arbiter_reason="target_fixed_no_regression",
            target_fixed=True,
            collateral_regressions=(),
        ))

    if not target_fixed:
        reason = "target_unchanged: post_score <= pre_score"
    else:
        reason = f"collateral_regressions={list(collateral)}"
    print(
        gate_reasoning_marker(
            gate="acceptance_gate",
            qid=state.qid,
            verdict="rejected",
            predicate_inputs={
                "pre_apply_score": state.evaluated.pre_apply_score,
                "post_apply_score": state.evaluated.post_apply_score,
                "target_fixed": target_fixed,
                "collateral_regressions": list(collateral),
            },
            reason=reason,
        ),
        flush=True,
    )
    # Trial 16 Chunk 3 + Trial 17 — surface the typed reason as the
    # ``forbidden_signature`` so cluster_batch's
    # ``ctx.forbidden_signatures`` channel can teach the next
    # iteration's strategist that the applied patch shape didn't
    # close the gap. Trial 17 enriches the format to carry the lever +
    # patch_type + rca_kind tokens so the LLM can pivot to a different
    # lever instead of re-proposing the same one.
    #
    # Format:
    #   "<selected_lever>:<patch_type>:target_unchanged:rca=<rca_kind>"
    # or for the collateral case:
    #   "<selected_lever>:<patch_type>:collateral_regressions=[...]"
    #
    # ``selected_lever`` comes from the proposal_store; when the
    # proposal landed before Trial 17 wired selected_lever (legacy),
    # we infer the lever from patch_type via
    # ``levers_contract.infer_lever_from_patch_type``. ``rca_kind``
    # comes from ``state.diagnosed.rca_kind_label``.
    from genie_space_optimizer.optimization.levers_contract import (
        infer_lever_from_patch_type,
    )

    selected_lever = ""
    patch_type_str = ""
    if state.proposals:
        latest = state.proposals[-1]
        patch_type_str = str(latest.patch_type or "")
        proposal = ctx.proposal_store.lookup(latest.intent_id)
        if proposal is not None:
            selected_lever = str(
                getattr(proposal, "selected_lever", "") or ""
            )
        if not selected_lever and patch_type_str:
            selected_lever = infer_lever_from_patch_type(patch_type_str)

    rca_kind = ""
    if state.diagnosed is not None:
        rca_kind = str(
            getattr(state.diagnosed, "rca_kind_label", "") or ""
        )

    if not target_fixed:
        forbidden_signature = (
            f"{selected_lever or '?'}:{patch_type_str or '?'}"
            f":target_unchanged:rca={rca_kind or '?'}"
        )
    else:
        forbidden_signature = (
            f"{selected_lever or '?'}:{patch_type_str or '?'}"
            f":collateral_regressions={list(collateral)}"
            f":rca={rca_kind or '?'}"
        )

    return GateVerdict.reject_terminal(TerminalRecord(
        kind="OPTIMIZER_TRIED_NO_GAIN",
        reason=reason,
        deepest_stage_reached=state.deepest_stage_reached,
        forbidden_signature=forbidden_signature,
    ))


acceptance_gate = ValidationGate(
    name="acceptance_gate",
    from_stage=FunnelStage.EVALUATED,
    to_stage_on_success=FunnelStage.ACCEPTED,
    to_stage_on_reject=FunnelStage.TERMINATED,
    predicate=_predicate,
)
