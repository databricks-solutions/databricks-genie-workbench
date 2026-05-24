"""Plan 11 Stage 1 diagnosis as a typed LlmStateTransformer.

Wraps the existing ``stages/cluster_plan11.py`` Stage 1 logic so the
state machine sees a typed input → typed output transformation.
Legacy entry point keeps running in parallel through Phase 4; Phase 5
deletes it.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

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
    """``LlmReasoningResponse``-shaped adapter wrapping diagnose output.

    ``blame_set`` is carried alongside ``parsed_output`` because the
    Stage 1 actionability gate (Trial 13 Track 3) keys off blame-set
    cardinality but :class:`_Stage1Parsed` omits the field (it mirrors
    the legacy ``LlmReasoningResponse.parsed_output`` schema, which
    pre-dated the per-QID blame_set in ``PerQidDiagnosis``).
    """
    succeeded: bool
    parsed_output: _Stage1Parsed | None = None
    declined: str | None = None
    blame_set: tuple[str, ...] = ()


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


def _build_failing_qid_payload(
    state: QuestionStateInIteration,
    row: dict,
    typed_evidence: Any | None = None,
    *,
    schema_columns: tuple[str, ...] = (),
) -> dict:
    """Project (state, eval_row) into the dict shape ``diagnose_failing_qids``
    consumes — same schema as the legacy
    ``_build_plan11_failing_qids_from_raw`` builder in optimizer.py.

    Trial 12 fix: delegates to :func:`eval_row_access.build_stage1_evidence_card`
    so all three Stage 1 input builders hydrate from the canonical
    row-shape adapter (Trial 11 root cause: flat ``row.get(...)`` +
    hardcoded empty ``rca_evidence.*`` produced empty cards on
    production-shape rows; the LLM correctly declined with
    ``missing_schema_context``).

    Trial 13 fix: ``typed_evidence`` is now threaded through so the SM
    canonical lane can hand Plan 12's per-QID ``PerQidRcaEvidence`` to
    the builder, symmetric to the Plan 11 batch path at
    ``optimizer._build_plan11_failing_qids_from_typed_evidence``
    (``optimizer.py:8543``). Before this argument was added, hard QIDs
    whose rows did not carry embedded blame/rca silently aborted Stage
    1 with ``evidence_card_empty:blame_set_empty,rca_evidence_empty``
    (Trial 12 / 13 postmortems).
    """
    from genie_space_optimizer.optimization.eval_row_access import (
        build_stage1_evidence_card,
    )

    return build_stage1_evidence_card(
        state.qid,
        row,
        typed_evidence=typed_evidence,
        schema_columns=schema_columns,
    )


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
    rca_kind_label = str(card.get("rca_kind_label") or "")
    evidence_summary = str(card.get("evidence_summary") or "")
    blame_set = tuple(
        str(b) for b in (card.get("blame_set") or ()) if str(b).strip()
    )
    parsed = _Stage1Parsed(
        rca_kind_label=rca_kind_label,
        evidence_summary=evidence_summary,
        observed_failure=str(card.get("observed_failure") or ""),
        expected_sql_shape=str(card.get("expected_sql_shape") or ""),
        confidence=confidence,
        rca_card_id=rca_card_id,
    )
    return _Stage1Response(
        succeeded=True,
        parsed_output=parsed,
        # Trial 13 Track 3 — keep blame_set on the response so the
        # actionability gate downstream of this builder can classify
        # without re-reading the stub card.
        blame_set=blame_set,
    )


def _gate_non_actionable(
    state: QuestionStateInIteration,
    ctx: TransformerContext,
    response: _Stage1Response,
) -> _Stage1Response:
    """Trial 13 Track 3 — actionability hard gate.

    A mechanically-successful Stage 1 response (``succeeded=True``)
    still has to pass the actionability gate before downstream stages
    see it. The gate fires when:

    * ``rca_kind_label`` is the ``"insufficient evidence to determine
      root cause"`` sentinel — Stage 1 admitted it cannot classify, OR
    * ``blame_set`` is empty — no objects to focus on, OR
    * ``evidence_summary`` is empty — no narrative to reason from.

    Trial 12 shadow batch path advanced 21/24 ``diagnosed`` outcomes
    that failed every one of these checks; Stage 3 then emitted
    ``empty_synthesis`` and the optimizer applied zero patches. Trial
    13 ships the gate so the QID terminates here with a typed reason
    instead of silently flowing into Stage 2.
    """
    if not response.succeeded or response.parsed_output is None:
        return response
    from genie_space_optimizer.optimization.run_analysis_contract import (
        classify_non_actionable_reason,
        plan11_stage1_non_actionable_reject_marker,
    )

    parsed = response.parsed_output
    reason = classify_non_actionable_reason(
        rca_kind_label=parsed.rca_kind_label,
        evidence_summary=parsed.evidence_summary,
        blame_set=response.blame_set,
    )
    if not reason:
        return response
    print(
        plan11_stage1_non_actionable_reject_marker(
            optimization_run_id=ctx.run_id,
            iteration=ctx.iteration,
            qid=state.qid,
            reason=reason,
            rca_kind_label=parsed.rca_kind_label,
            blame_set_size=len(response.blame_set or ()),
            evidence_summary_chars=len(parsed.evidence_summary or ""),
        ),
        flush=True,
    )
    return _Stage1Response(
        succeeded=False,
        declined=f"non_actionable_diagnosis:{reason}",
    )


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
            stub_response = _stub_response_from_rca_card(state, card)
            return _gate_non_actionable(state, ctx, stub_response)
        return _Stage1Response(
            succeeded=False, declined="diagnose_stub_returned_empty",
        )

    row = _find_eval_row(ctx, state.qid)
    if row is None:
        return _Stage1Response(
            succeeded=False,
            declined=f"no_eval_row_for_qid:{state.qid}",
        )

    # Trial 13 typed-evidence cutover — look up Plan 12's per-QID typed
    # RCA evidence on the TransformerContext and thread it into the
    # Stage 1 builder. Symmetric to the Plan 11 batch path at
    # ``optimizer._build_plan11_failing_qids_from_typed_evidence``.
    # ``ctx.rca_evidence_typed`` defaults to an empty mapping so callers
    # that did not provide typed evidence (legacy harness paths, unit
    # tests) keep the prior row-only behaviour.
    typed_ev = (ctx.rca_evidence_typed or {}).get(state.qid)
    # Trial 13i — thread ``ctx.schema_columns`` into the builder so the
    # seed FQN normalizer can resolve free-text ASI tokens
    # (``DEST_AIRPORT_CD``) into 4-part FQNs that survive Stage 1's
    # schema filter.
    payload = _build_failing_qid_payload(
        state,
        row,
        typed_evidence=typed_ev,
        schema_columns=tuple(ctx.schema_columns or ()),
    )

    # Trial 12 pre-flight: reject empty evidence cards BEFORE invoking
    # the LLM. Trial 11 burned tokens on 55/55 calls only to receive
    # the same correct ``missing_schema_context`` decline. Symmetric
    # to ``DatabricksEndpointRequestContract`` at the wire boundary.
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage1_input_card_empty_marker,
        plan11_stage1_input_quality_marker,
    )
    from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
        DEFAULT_STAGE1_CONTRACT,
        Stage1InputCardEmptyError,
    )

    # Trial 13i — emit the input-quality marker for every QID once the
    # payload is hydrated. The marker carries the run-level
    # ``schema_columns`` provenance label (set by the SM/workbench seam
    # via ``_derive_schema_columns``) and the per-QID seed normalization
    # stats stamped on the card under ``_seed_normalization`` by
    # :func:`build_stage1_evidence_card`. Postmortems use it to
    # distinguish ``"empty"`` (deploy-block canary) from
    # ``"typed_evidence_union"`` (healthy) without re-deriving.
    _seed_norm_stats = payload.get("_seed_normalization") or {}
    # Trial 14 — derive the per-kind histogram from the typed
    # ``_blame_structured`` stamp on the card so the marker carries
    # an at-a-glance "what blame did the judges actually emit?"
    # signal alongside the seed normalization verdict.
    _blame_structured_entries = payload.get("_blame_structured") or ()
    _blame_kind_distribution: dict[str, int] = {}
    for _entry in _blame_structured_entries:
        if not isinstance(_entry, dict):
            continue
        _kind = str(_entry.get("kind") or "").strip().lower()
        if not _kind:
            continue
        _blame_kind_distribution[_kind] = (
            _blame_kind_distribution.get(_kind, 0) + 1
        )

    schema_cols_violations = DEFAULT_STAGE1_CONTRACT.validate_schema_columns(
        ctx.schema_columns
    )
    _contract_violation_field = (
        schema_cols_violations[0].field
        if schema_cols_violations
        else ""
    )
    print(
        plan11_stage1_input_quality_marker(
            optimization_run_id=ctx.run_id,
            iteration=ctx.iteration,
            qid=state.qid,
            schema_columns_source=str(ctx.schema_columns_source or "empty"),
            schema_columns_size=len(ctx.schema_columns or ()),
            seeds_pre_normalize=int(
                _seed_norm_stats.get("seeds_pre_normalize") or 0
            ),
            seeds_post_normalize=int(
                _seed_norm_stats.get("seeds_post_normalize") or 0
            ),
            seeds_normalized=int(
                _seed_norm_stats.get("seeds_normalized") or 0
            ),
            seeds_dropped=int(_seed_norm_stats.get("seeds_dropped") or 0),
            contract_violation=_contract_violation_field,
            blame_kind_distribution=_blame_kind_distribution,
        ),
        flush=True,
    )

    # Trial 13i — pre-flight on the run-level ``schema_columns``
    # channel. When the channel is empty the Stage 1 LLM cannot ground
    # a blame_set under the Trial 13h prompt contract; short-circuiting
    # here with ``missing_schema_columns`` surfaces the upstream defect
    # cleanly instead of forcing the LLM into a guaranteed
    # ``insufficient_blame_set`` decline.
    if schema_cols_violations:
        err = Stage1InputCardEmptyError(schema_cols_violations)
        print(
            plan11_stage1_input_card_empty_marker(
                optimization_run_id=ctx.run_id,
                iteration=ctx.iteration,
                qid=state.qid,
                violations=[v.field for v in schema_cols_violations],
                field_sources={
                    "schema_columns": str(
                        ctx.schema_columns_source or "empty"
                    ),
                },
            ),
            flush=True,
        )
        return _Stage1Response(
            succeeded=False,
            declined=err.as_declined_reason(),
        )

    violations = DEFAULT_STAGE1_CONTRACT.validate(payload)
    if violations:
        err = Stage1InputCardEmptyError(violations)
        print(
            plan11_stage1_input_card_empty_marker(
                optimization_run_id=ctx.run_id,
                iteration=ctx.iteration,
                qid=state.qid,
                violations=[v.field for v in violations],
                field_sources=DEFAULT_STAGE1_CONTRACT.field_sources(payload),
            ),
            flush=True,
        )
        return _Stage1Response(
            succeeded=False,
            declined=err.as_declined_reason(),
        )

    # Lazy import — diagnose imports through harness in some paths.
    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )
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
    successful = _Stage1Response(
        succeeded=True,
        parsed_output=parsed,
        blame_set=tuple(matching.blame_set or ()),
    )
    return _gate_non_actionable(state, ctx, successful)


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
