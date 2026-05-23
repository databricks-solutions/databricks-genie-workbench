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


@dataclass(frozen=True, slots=True)
class _Stage1Parsed:
    """Parsed-output projection consumed by ``build_diagnosis_record_from_llm_result``.

    Mirrors the field set the legacy ``LlmReasoningResponse.parsed_output``
    exposes for Stage 1, but synthesized from ``PerQidDiagnosis`` (which
    omits ``rca_card_id`` — derived deterministically here).
    """
    rca_kind_label: str
    evidence_summary: str
    observed_failure: str
    expected_sql_shape: str
    confidence: str
    rca_card_id: str


@dataclass(frozen=True, slots=True)
class _Stage1Response:
    """``LlmReasoningResponse``-shaped adapter wrapping diagnose output."""
    succeeded: bool
    parsed_output: _Stage1Parsed | None = None
    declined: str | None = None


def _find_eval_row(ctx: TransformerContext, qid: str) -> dict | None:
    """Return the eval row whose canonical question id matches ``qid``.

    2026-05-23 admission fix: production MLflow rows carry the qid under
    ``inputs/question_id``, nested ``inputs.question_id``, or
    ``request.kwargs.question_id``. Single-key ``row.get("question_id")``
    returned ``""`` for every such row and prevented downstream diagnose
    transformers from ever pairing a hard state with its source row.
    Delegate to the canonical extractor (Cycle 8).
    """
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    for row in ctx.baseline_eval_rows:
        candidate_qid, _source = extract_question_id(dict(row))
        if candidate_qid == qid:
            return dict(row)
    return None


def _build_failing_qid_payload(state: QuestionStateInIteration, row: dict) -> dict:
    """Project (state, eval_row) into the dict shape ``diagnose_failing_qids``
    consumes — same schema as the legacy
    ``_build_plan11_failing_qids_from_raw`` builder in optimizer.py."""
    return {
        "qid": state.qid,
        "question_text": str(row.get("question") or ""),
        "ground_truth_sql": str(row.get("ground_truth_sql") or ""),
        "generated_sql": str(row.get("generated_sql") or ""),
        "judge_rationale": str(row.get("judge_rationale") or ""),
        "blame_set_seed": [],
        "rca_evidence": {
            "observed_failure": str(row.get("judge_rationale") or ""),
            "generated_sql_issue": "",
            "expected_sql_shape": "",
            "suggested_repair_family": "",
            "confidence": "",
        },
    }


def _stub_response_from_rca_card(state: QuestionStateInIteration, card: dict) -> _Stage1Response:
    """Build a ``_Stage1Response`` from a stubbed RCA card dict.

    Test stubs return an ``rca_card`` dict directly (see anchor
    fixtures and ``test_anchor_reaches_applied_via_state_machine``).
    Project it into the parsed-output adapter shape the transformer
    consumes.
    """
    rca_card_id = str(
        card.get("rca_card_id")
        or f"rca_card_{state.qid}_{card.get('rca_kind_label', 'unknown')}"
    )
    confidence = str(card.get("confidence") or "high")
    parsed = _Stage1Parsed(
        rca_kind_label=str(card.get("rca_kind_label") or ""),
        evidence_summary=str(card.get("evidence_summary") or ""),
        observed_failure=str(card.get("observed_failure") or ""),
        expected_sql_shape=str(card.get("expected_sql_shape") or ""),
        confidence=confidence,
        rca_card_id=rca_card_id,
    )
    return _Stage1Response(succeeded=True, parsed_output=parsed)


def _invoke_stage1_llm(
    state: QuestionStateInIteration, ctx: TransformerContext,
) -> _Stage1Response:
    """Dispatch the actual Stage 1 LLM call.

    Adapter over ``stages.diagnose.diagnose_failing_qids``: builds the
    single-element failing_qids payload from ``ctx.baseline_eval_rows``,
    invokes the existing Stage 1 entry point, and projects the matching
    ``PerQidDiagnosis`` back into the ``LlmReasoningResponse``-shaped
    object the transformer consumes.

    Test-stub override:
      When ``ctx.extras["diagnose_llm"]`` is callable, it is invoked
      with ``(state, ctx)`` and expected to return a dict carrying the
      ``rca_card`` fields. This lets the synthetic anchor replay drive
      the diagnose lane without plumbing baseline_eval_rows.

    Defensive abstain paths:
      * No matching eval row → short-circuit (don't burn an LLM call).
      * Empty diagnosis list → declined.
      * No PerQidDiagnosis matches this state's QID → declined.
    """
    stub = ctx.extras.get("diagnose_llm") if ctx.extras else None
    if callable(stub):
        try:
            card = stub(state=state, ctx=ctx)
        except TypeError:
            card = stub()
        if isinstance(card, dict) and card:
            return _stub_response_from_rca_card(state, card)
        return _Stage1Response(
            succeeded=False, declined="diagnose_stub_returned_empty",
        )

    row = _find_eval_row(ctx, state.qid)
    if row is None:
        return _Stage1Response(
            succeeded=False,
            declined=f"no_eval_row_for_qid:{state.qid}",
        )

    # Lazy import — diagnose imports through harness in some paths.
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    payload = _build_failing_qid_payload(state, row)
    diagnoses = diagnose_failing_qids(
        failing_qids=[payload],
        schema_columns=list(ctx.schema_columns),
        optimization_run_id=ctx.run_id,
        iteration=ctx.iteration,
        w=ctx.w,
        recent_diagnoses=(
            [dict(d) for d in ctx.recent_diagnoses] or None
        ),
    )
    if not diagnoses:
        return _Stage1Response(
            succeeded=False, declined="diagnose_returned_empty",
        )

    matching = next((d for d in diagnoses if d.qid == state.qid), None)
    if matching is None:
        return _Stage1Response(
            succeeded=False,
            declined=f"diagnose_returned_no_matching_qid:{state.qid}",
        )

    parsed = _Stage1Parsed(
        rca_kind_label=matching.rca_kind_label,
        evidence_summary=matching.evidence_summary,
        observed_failure=matching.observed_failure,
        expected_sql_shape=matching.expected_sql_shape,
        confidence=matching.confidence,
        rca_card_id=f"rca_card_{state.qid}_{matching.rca_kind_label}",
    )
    return _Stage1Response(succeeded=True, parsed_output=parsed)


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
