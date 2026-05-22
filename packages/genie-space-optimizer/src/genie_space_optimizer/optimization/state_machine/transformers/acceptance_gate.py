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

    def _score(row: dict) -> float:
        return float(row.get("feedback/result_correctness/value", 0.0) or 0.0)

    pre_by_qid = {
        str(r.get("question_id") or ""): _score(r) for r in baseline
    }
    post_by_qid = {
        str(r.get("question_id") or ""): _score(r) for r in post
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
    return GateVerdict.reject_terminal(TerminalRecord(
        kind="OPTIMIZER_TRIED_NO_GAIN",
        reason=reason,
        deepest_stage_reached=state.deepest_stage_reached,
        forbidden_signature="",
    ))


acceptance_gate = ValidationGate(
    name="acceptance_gate",
    from_stage=FunnelStage.EVALUATED,
    to_stage_on_success=FunnelStage.ACCEPTED,
    to_stage_on_reject=FunnelStage.TERMINATED,
    predicate=_predicate,
)
