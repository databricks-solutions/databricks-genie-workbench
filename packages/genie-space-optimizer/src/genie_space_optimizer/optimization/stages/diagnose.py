"""Plan 11 — Stage 1: LLM-driven per-QID diagnosis.

Replaces the deterministic RCA classifier. Each failing QID produces one
:class:`PerQidDiagnosis` with a free-text ``rca_kind_label``.

Entry point: :func:`diagnose_failing_qids`.

The handler is dormant during PR 1 (the feature flag is OFF). PR 2 wires
it into ``optimizer.py``.
"""
from __future__ import annotations

import json
import time
from typing import Any

from genie_space_optimizer.optimization.llm_reasoning_call import LlmReasoningCall
from genie_space_optimizer.optimization.llm_reasoning_io import LlmReasoningRequest
from genie_space_optimizer.optimization.run_analysis_contract import (
    llm_contract_failure_marker,
    plan11_stage1_diagnosis_marker,
)
from genie_space_optimizer.optimization.stages.plan11_types import PerQidDiagnosis
from genie_space_optimizer.skills._loader import _SKILL_LOADER
from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
    Plan11DiagnoseOutput,
)


_SKILL_ID = "plan11_diagnose"
_PROMPT_CONST = "PLAN11_DIAGNOSE_PROMPT"


def _classify_llm_error(
    exception_class: str,
    tokens_input: int,
    request: LlmReasoningRequest,
) -> str:
    """Map an LLM-call exception class to a structured ``error_kind``.

    Phase 1.C of the SM cutover. The 2026-05-23 trial emitted
    ``outcome="llm_error"`` markers with ``tokens_input=0`` and no
    further detail, leaving postmortems unable to distinguish a
    pre-flight client failure (no endpoint reachable) from a mid-flight
    parse failure. This classifier maps the exception class name from
    ``LlmReasoningResponse.error`` (formatted as ``"ClassName: msg"``)
    into one of: client_construction, empty_prompt, endpoint_decline,
    timeout, parse, unknown.
    """
    if not exception_class:
        # Empty prompt is the only way to get an "llm_error" without an
        # underlying exception class (the call short-circuits).
        if tokens_input == 0 and not (
            request.user_prompt and request.system_msg
        ):
            return "empty_prompt"
        return "unknown"

    cls = exception_class.lower()
    if "timeout" in cls:
        return "timeout"
    if "envelopecontract" in cls or "parse" in cls or "json" in cls:
        return "parse"
    if "connection" in cls or "endpoint" in cls or "httpx" in cls or "permission" in cls:
        return "endpoint_decline"
    if "client" in cls or "config" in cls or "auth" in cls:
        return "client_construction"
    if tokens_input == 0 and not (request.user_prompt and request.system_msg):
        return "empty_prompt"
    return "unknown"


def _build_request(
    *,
    failing_qids: list[dict[str, Any]],
    schema_columns: list[str],
    recent_diagnoses: list[dict[str, Any]],
    iteration: int,
) -> LlmReasoningRequest:
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    if rsm is None:
        raise RuntimeError(
            f"{_SKILL_ID!r} is not a reasoning skill — check SKILL.md "
            "frontmatter"
        )
    output_cls = _SKILL_LOADER.load_output_schema_class(_SKILL_ID)
    system_body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name=_PROMPT_CONST,
    )
    user_prompt = json.dumps(
        {
            "iteration": iteration,
            "failing_qids": failing_qids,
            "schema_columns": schema_columns,
            "recent_diagnoses_for_same_qids": recent_diagnoses,
        },
        default=str,
    )
    return LlmReasoningRequest(
        call_id=f"plan11_stage1_diagnose.iter_{int(iteration)}",
        skill_id=_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
    )


def diagnose_failing_qids(
    *,
    failing_qids: list[dict[str, Any]],
    schema_columns: list[str],
    optimization_run_id: str,
    iteration: int,
    w: Any,
    recent_diagnoses: list[dict[str, Any]] | None = None,
) -> list[PerQidDiagnosis]:
    """Plan 11 Stage 1 — batch-diagnose all failing QIDs in one LLM call.

    Returns a list of :class:`PerQidDiagnosis` (one per diagnosed QID).
    QIDs the LLM declines on, or for which the call fails, are omitted
    from the return; their markers carry ``outcome="declined"`` or
    ``"llm_error"``. The empty-input path returns ``[]`` with no marker.
    """
    if not failing_qids:
        return []

    request = _build_request(
        failing_qids=failing_qids,
        schema_columns=schema_columns,
        recent_diagnoses=recent_diagnoses or [],
        iteration=iteration,
    )

    t0 = time.monotonic()
    resp = LlmReasoningCall().invoke(w=w, request=request)
    duration_ms = int((time.monotonic() - t0) * 1000)
    tokens_in = int(getattr(resp, "tokens_input", 0) or 0)
    tokens_out = int(getattr(resp, "tokens_output", 0) or 0)

    if not resp.succeeded or resp.parsed_output is None:
        abstain_reason = ""
        abstain_explanation = ""
        if resp.declined is not None:
            abstain_reason = str(getattr(resp.declined, "reason", ""))
            abstain_explanation = str(getattr(resp.declined, "explanation", ""))
        outcome = "declined" if resp.declined is not None else "llm_error"
        # SM Cutover Phase 1.C — classify llm_error so the zero-token
        # marker postmortems saw in 2026-05-23 ("opaque llm_error with
        # tokens_input=0") becomes actionable.
        error_kind = ""
        exception_class = ""
        if outcome == "llm_error":
            raw_err = str(getattr(resp, "error", "") or "")
            # Error format from llm_reasoning_call.invoke: "ClassName: message".
            exception_class = raw_err.split(":", 1)[0].strip() if raw_err else ""
            error_kind = _classify_llm_error(exception_class, tokens_in, request)
        for qid_input in failing_qids:
            qid = str(qid_input.get("qid", ""))
            print(
                plan11_stage1_diagnosis_marker(
                    optimization_run_id=optimization_run_id,
                    iteration=iteration,
                    qid=qid,
                    outcome=outcome,
                    abstain_reason=abstain_reason,
                    abstain_explanation=abstain_explanation,
                    duration_ms=duration_ms,
                    tokens_input=tokens_in,
                    tokens_output=tokens_out,
                    error_kind=error_kind,
                    exception_class=exception_class,
                )
            )
        return []

    # parsed_output is a dict (LlmReasoningResponse stores parsed.model_dump()).
    raw_diagnoses = resp.parsed_output.get("diagnoses", []) or []
    if not raw_diagnoses:
        # Pydantic validation succeeded but the model emitted no items —
        # treat as a contract failure so postmortem can grep for it.
        print(
            llm_contract_failure_marker(
                schema_name="Plan11DiagnoseOutput",
                failing_fields=["diagnoses"],
                raw_payload=resp.parsed_output,
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                skill_name=_SKILL_ID,
                error_repr="empty diagnoses list",
            )
        )
        for qid_input in failing_qids:
            qid = str(qid_input.get("qid", ""))
            print(
                plan11_stage1_diagnosis_marker(
                    optimization_run_id=optimization_run_id,
                    iteration=iteration,
                    qid=qid,
                    outcome="contract_failure",
                    duration_ms=duration_ms,
                    tokens_input=tokens_in,
                    tokens_output=tokens_out,
                )
            )
        return []

    diagnoses: list[PerQidDiagnosis] = []
    schema_col_set = set(schema_columns)
    for item in raw_diagnoses:
        # Drop blame_set entries that don't appear in schema_columns
        # (the prompt instructs the LLM to draw from this list).
        raw_blame = item.get("blame_set") or []
        valid_blame = (
            tuple(str(b) for b in raw_blame if str(b) in schema_col_set)
            if schema_col_set
            else tuple(str(b) for b in raw_blame)
        )
        diag = PerQidDiagnosis(
            qid=str(item.get("qid", "")),
            rca_kind_label=str(item.get("rca_kind_label", "")),
            observed_failure=str(item.get("observed_failure", "")),
            generated_sql_issue=str(item.get("generated_sql_issue", "")),
            expected_sql_shape=str(item.get("expected_sql_shape", "")),
            blame_set=valid_blame,
            evidence_summary=str(item.get("evidence_summary", "")),
            confidence=item.get("confidence", "low"),  # type: ignore[arg-type]
        )
        diagnoses.append(diag)
        print(
            plan11_stage1_diagnosis_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                qid=diag.qid,
                outcome="diagnosed",
                rca_kind_label=diag.rca_kind_label,
                confidence=str(diag.confidence),
                blame_set_size=len(diag.blame_set),
                evidence_summary_chars=len(diag.evidence_summary),
                duration_ms=duration_ms,
                tokens_input=tokens_in,
                tokens_output=tokens_out,
            )
        )

    return diagnoses
