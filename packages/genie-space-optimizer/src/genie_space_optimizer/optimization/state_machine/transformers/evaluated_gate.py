"""Evaluated gate: APPLIED → EVALUATED.

Wraps the post-apply eval side effect: re-run the QID's question through
Genie after the patch lands, score the result, and attach an
``EvaluatedRecord`` carrying ``pre_apply_score`` / ``post_apply_score``
plus the pre/post SQL strings for trajectory comparison.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.markers import (
    gate_reasoning_marker,
)
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

    Trial 15 — workbench escape hatch. When ``ctx.extras["post_apply_eval"]``
    is callable, it is invoked with ``(state=, ctx=)`` and expected to
    return the same ``(score, generated_sql, eval_row_id)`` tuple. This
    is symmetric to ``ctx.extras["applier"]`` at
    ``applier_gate.py:_apply_via_genie_api`` and lets the workbench /
    structural tests drive the gate to ``EVALUATED`` without spinning
    up live MLflow + Genie. Production callers never set this key.
    """
    extras = getattr(ctx, "extras", {}) or {}
    stub = extras.get("post_apply_eval") if extras else None
    if callable(stub):
        try:
            return stub(state=state, ctx=ctx)
        except TypeError:
            # Allow zero-arg stubs in the simplest tests.
            return stub()

    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )
    from genie_space_optimizer.optimization.stages.evaluation import (
        EvaluationInput, evaluate_post_patch,
    )

    # Trial 16.2 — per-applied-patch eval is scoped to ``state.qid``
    # ONLY, never the wider ``ctx.eval_qids``.
    #
    # ``ctx.eval_qids`` carries the iteration's *full* benchmark-qid
    # context (Trial 16.1 helper fix populates it correctly; pre-fix it
    # was always empty by accident). The gate runs once per applied
    # patch, and each patch targets exactly one qid — the qid we just
    # patched. Forwarding the full ``ctx.eval_qids`` makes
    # ``_run_full_evaluation`` slice to all 23 benchmarks and run the
    # full 12-minute eval per applied patch — the exact Trial 16 RC1
    # timeout pattern (postmortems 575892594490176 +
    # 319530250904653 timed out at ~120 min from 7 applied patches ×
    # ~12 min each = ~84 min of repeated full evals).
    #
    # The wider ``ctx.eval_qids`` is still used by:
    # * ``harness.py:_eval_inp_full`` (once-per-iteration baseline
    #   eval — actually wants all benchmark qids).
    # * ``gate_reasoning_marker`` below (classification metadata,
    #   not scope).
    eval_input = EvaluationInput(
        space_state=dict(ctx.metadata_snapshot),
        eval_qids=(state.qid,),
        run_role="iteration_eval",
        iteration_label=f"iter_{ctx.iteration:03d}",
        scope="full",
    )

    result = evaluate_post_patch(
        ctx.stage_ctx, eval_input, eval_kwargs=ctx.eval_kwargs,
    )

    # Trial 16 RC2a — use the canonical extractor instead of the
    # strict ``r.get("question_id")`` lookup. MLflow-flattened rows
    # carry the canonical qid under ``inputs/question_id`` (slash),
    # ``inputs.question_id`` (dot), nested ``inputs: {...}``, or
    # ``metadata.question_id``; the helper handles all of them. Before
    # this change the gate ignored every shape but top-level
    # ``question_id`` and produced ``no_post_apply_row_for_qid`` for
    # every applied patch — the dominant failure mode in production
    # postmortems 575892594490176 + 319530250904653.
    rows = tuple(getattr(result, "eval_rows", ()) or ())
    matching = next(
        (r for r in rows if extract_question_id(dict(r))[0] == state.qid),
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
        print(
            gate_reasoning_marker(
                gate="evaluated_gate",
                qid=state.qid,
                verdict="rejected",
                predicate_inputs={
                    "eval_qids": list(ctx.eval_qids),
                    "exception_type": type(exc).__name__,
                },
                reason=f"post_apply_eval_failed:{exc}",
            ),
            flush=True,
        )
        # Trial 16 Chunk 3 — surface the typed eval failure shape as
        # the ``forbidden_signature`` so cluster_batch's
        # ``ctx.forbidden_signatures`` channel can teach the next
        # iteration's strategist (the exception type + message often
        # encode the root cause: ``no_post_apply_row_for_qid``,
        # ``benchmarks_empty``, ``mlflow_unavailable`` etc.).
        return GateVerdict.reject_terminal(TerminalRecord(
            kind="OPTIMIZER_INVARIANT_VIOLATION",
            reason=f"post_apply_eval_failed:{exc}",
            deepest_stage_reached=state.deepest_stage_reached,
            forbidden_signature=f"post_apply_eval_failed:{exc}",
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
