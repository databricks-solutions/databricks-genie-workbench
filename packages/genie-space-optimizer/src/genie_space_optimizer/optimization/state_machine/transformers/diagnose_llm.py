"""Plan 11 Stage 1 diagnosis as a typed LlmStateTransformer.

Wraps the existing ``stages/cluster_plan11.py`` Stage 1 logic so the
state machine sees a typed input → typed output transformation.
Legacy entry point keeps running in parallel through Phase 4; Phase 5
deletes it.
"""
from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.state_machine.records import DiagnosisRecord
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


def build_diagnosis_record_from_llm_result(
    state: QuestionStateInIteration,
    result,
) -> DiagnosisRecord:
    """Translate a Stage 1 LLM result into a typed DiagnosisRecord.

    Reads the same fields the legacy ``stages/cluster_plan11.py`` Stage 1
    output produces. The legacy code populated a dict; this returns a
    typed record the state machine writes to ``state.diagnosed`` via
    ``state.advance(...)``.
    """
    return DiagnosisRecord(
        source="plan11_stage1",
        rca_kind_label=str(result.rca_kind_label),
        evidence_summary=str(result.evidence_summary),
        observed_failure=str(result.observed_failure),
        expected_sql_shape=str(result.expected_sql_shape),
        confidence=str(result.confidence),  # type: ignore[arg-type]
        rca_card_id=str(result.rca_card_id),
    )
