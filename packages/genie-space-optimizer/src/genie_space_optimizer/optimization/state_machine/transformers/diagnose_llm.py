"""Plan 11 Stage 1 diagnosis as a typed LlmStateTransformer.

Wraps the existing ``stages/cluster_plan11.py`` Stage 1 logic so the
state machine sees a typed input → typed output transformation.
Legacy entry point keeps running in parallel through Phase 4; Phase 5
deletes it.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    DiagnosisRecord,
    StageTransition,
    TerminalRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
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


# ─── Transformer assembly ──────────────────────────────────────────────


def _invoke_stage1_llm(state: QuestionStateInIteration, ctx: TransformerContext):
    """Dispatch the actual Stage 1 LLM call.

    Kept as a module-level function so unit tests can monkeypatch it
    cleanly (no need to import the real LlmReasoningCall or pull in
    the workspace client). Production callers exercise this via the
    transformer's ``transform()`` method.

    The real implementation will route through
    ``stages.cluster_plan11`` Stage 1's existing LLM dispatch (kept
    running alongside through Phase 4). Phase 5 deletes the legacy
    callsite and this helper becomes the only Stage 1 entry point.
    """
    raise NotImplementedError(
        "Stage 1 LLM dispatch not yet wired to production lever loop. "
        "Tests must monkeypatch _invoke_stage1_llm with a fake "
        "LlmReasoningResponse. Phase 2 wire-in lives in PR 2.5."
    )


class _Stage1Abstain(Exception):
    def __init__(self, *, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class _Plan11Stage1Transformer:
    """Concrete LlmStateTransformer with abstain handling.

    The generic ``LlmStateTransformer`` dataclass cannot terminate
    cleanly on abstain (its ``transform`` always advances forward).
    Abstain on Stage 1 must produce ``OPTIMIZER_NO_CANDIDATES`` — a
    typed terminal — so we implement the ``StateTransformer``
    protocol directly with abstain-aware advance.
    """
    name: str = "plan11_stage1_diagnosis"
    from_stage: FunnelStage = FunnelStage.HARD_QID_SEEN
    to_stage_on_success: FunnelStage = FunnelStage.DIAGNOSED
    to_stage_on_reject: FunnelStage = FunnelStage.TERMINATED

    def transform(
        self,
        state: QuestionStateInIteration,
        ctx: TransformerContext,
    ) -> QuestionStateInIteration:
        try:
            response = _invoke_stage1_llm(state, ctx)
            if not getattr(response, "succeeded", False):
                raise _Stage1Abstain(
                    reason=f"abstain: {getattr(response, 'declined', 'unknown')}",
                )
            parsed = response.parsed_output
            diagnosed = build_diagnosis_record_from_llm_result(state, parsed)
        except _Stage1Abstain as ab:
            transition = StageTransition(
                from_stage=self.from_stage,
                to_stage=FunnelStage.TERMINATED,
                at_ms=int(time.time() * 1000),
                transformer_name=self.name,
                transition_kind="llm",
                reason=ab.reason,
            )
            return state.terminate(
                transition=transition,
                terminal=TerminalRecord(
                    kind="OPTIMIZER_NO_CANDIDATES",
                    reason=ab.reason,
                    deepest_stage_reached=state.deepest_stage_reached,
                    forbidden_signature="",
                ),
            )

        transition = StageTransition(
            from_stage=self.from_stage,
            to_stage=self.to_stage_on_success,
            at_ms=int(time.time() * 1000),
            transformer_name=self.name,
            transition_kind="llm",
        )
        return state.advance(
            to_stage=self.to_stage_on_success,
            transition=transition,
            diagnosed=diagnosed,
        )


plan11_stage1_diagnosis = _Plan11Stage1Transformer()
