"""Evaluated gate: APPLIED → EVALUATED.

Wraps the post-apply eval side effect: re-run the QID's question through
Genie after the patch lands, score the result, and attach an
``EvaluatedRecord`` carrying ``pre_apply_score`` / ``post_apply_score``
plus the pre/post SQL strings for trajectory comparison.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import EvaluatedRecord
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformer import ValidationGate
from genie_space_optimizer.optimization.state_machine.verdict import (
    GateVerdict, TransformerContext,
)


class _PostApplyEvalError(Exception):
    """Internal: surfaced when the post-apply eval cannot produce a row
    for this state's QID. The gate maps it to a terminal."""


def _run_post_apply_eval(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> tuple[float, str, str]:
    """Adapter over ``stages.evaluation.evaluate_post_patch``.

    Constructs a minimal ``EvaluationInput`` from ``ctx`` fields, runs
    the legacy post-apply eval, and extracts the row matching
    ``state.qid`` to return ``(score, generated_sql, eval_row_id)``.

    Raises ``_PostApplyEvalError`` when the eval returned no row for
    this QID — the caller maps that to a typed terminal.
    """
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput, evaluate_post_patch,
    )

    eval_input = EvaluationInput(
        space_state=dict(ctx.metadata_snapshot),
        eval_qids=tuple(ctx.eval_qids) or (state.qid,),
        run_role="iteration_eval",
        iteration_label=f"iter_{ctx.iteration:03d}",
        scope="full",
    )

    result = evaluate_post_patch(
        ctx.stage_ctx, eval_input, eval_kwargs=ctx.eval_kwargs,
    )

    rows = tuple(getattr(result, "eval_rows", ()) or ())
    matching = next(
        (r for r in rows if str(r.get("question_id") or "") == state.qid),
        None,
    )
    if matching is None:
        raise _PostApplyEvalError(
            f"no_post_apply_row_for_qid:{state.qid}",
        )

    score = float(matching.get("feedback/result_correctness/value", 0.0) or 0.0)
    generated_sql = str(matching.get("generated_sql") or "")
    eval_row_id = str(
        matching.get("eval_row_id") or matching.get("row_id") or "",
    )
    return (score, generated_sql, eval_row_id)


def _predicate(state: QuestionStateInIteration, ctx: TransformerContext) -> GateVerdict:
    from genie_space_optimizer.optimization.state_machine.records import (
        TerminalRecord,
    )
    try:
        post_score, post_sql, eval_row_id_post = _run_post_apply_eval(state, ctx)
    except Exception as exc:
        # Any failure to obtain a post-apply score is terminal — we
        # cannot make an accept/reject decision without it.
        return GateVerdict.reject_terminal(TerminalRecord(
            kind="OPTIMIZER_INVARIANT_VIOLATION",
            reason=f"post_apply_eval_failed:{exc}",
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature="",
        ))
    return GateVerdict.success(record=EvaluatedRecord(
        pre_apply_score=state.seen.score,
        post_apply_score=post_score,
        pre_apply_sql=state.seen.baseline_sql,
        post_apply_sql=post_sql,
        eval_row_id_post=eval_row_id_post,
    ))


evaluated_gate = ValidationGate(
    name="evaluated_gate",
    from_stage=FunnelStage.APPLIED,
    to_stage_on_success=FunnelStage.EVALUATED,
    to_stage_on_reject=FunnelStage.TERMINATED,
    predicate=_predicate,
)
