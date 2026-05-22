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


def _run_post_apply_eval(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> tuple[float, str, str]:
    """Wrap the legacy harness post-apply eval; patchable for tests.

    Returns ``(post_apply_score, post_apply_sql, eval_row_id_post)``.
    Production wire-in lands alongside the deployed-smoke gate (PR 4.5)
    that the user deferred to a future session.
    """
    raise NotImplementedError(
        "Post-apply eval not wired to production yet. "
        "Tests must monkeypatch _run_post_apply_eval."
    )


def _predicate(state: QuestionStateInIteration, ctx: TransformerContext) -> GateVerdict:
    post_score, post_sql, eval_row_id_post = _run_post_apply_eval(state, ctx)
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
