"""Plan 11 Stage 1 diagnosis as a typed LlmStateTransformer.

Wraps the existing ``stages/cluster_plan11.py`` Stage 1 logic so the
state machine sees a typed input → typed output transformation.
Legacy entry point keeps running in parallel through Phase 4; Phase 5
deletes it.
"""
from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)


@dataclass(frozen=True, slots=True)
class Stage1LlmInput:
    qid: str
    eval_row_id: str
    baseline_sql: str
    expected_shape: str
    iteration_first_seen: int


def build_stage1_llm_input(state: QuestionStateInIteration) -> Stage1LlmInput:
    """Project QuestionStateInIteration.seen into the Stage 1 LLM input shape."""
    return Stage1LlmInput(
        qid=state.qid,
        eval_row_id=state.seen.eval_row_id,
        baseline_sql=state.seen.baseline_sql,
        expected_shape=state.seen.expected_shape,
        iteration_first_seen=state.seen.iteration_first_seen,
    )
