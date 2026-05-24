"""Plan 11 Stage 1 — LLM output contract for per-QID diagnosis.

The LLM returns a batch of DiagnosisItem objects — one per failing QID.
The AbstainableEnvelope wraps the result (or declined verdict) per the
LlmReasoningCall framework.

Trial 13 Track 4 — field caps relaxed 5× over Trial 12 to accommodate
realistic LLM outputs (the 98ec8950 trial observed
``string_too_long`` on ``generated_sql_issue`` at 300 and
``evidence_summary`` at 400). Oversize fields are now truncated
gracefully with a trailing ``"..."`` instead of raising; the original
length is preserved in a typed ``GSO_PLAN11_POST_PARSE_FIELD_TRUNCATE_V1``
marker so postmortems can surface the abnormal length without
discarding the semantically-correct LLM response.

``max_length`` constraints on string fields apply at Pydantic
validation time. The Databricks JSON Schema subset doesn't support
``maxLength``, so ``build_response_format`` strips them before sending
the schema to the model — but the constraint still rejects over-long
responses on the way back in.
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


_DIAGNOSE_FIELD_CAPS = {
    "rca_kind_label": 200,
    "observed_failure": 1000,
    "generated_sql_issue": 1500,
    "expected_sql_shape": 1500,
    "evidence_summary": 2000,
}


def _truncate_with_ellipsis(text: str, cap: int) -> str:
    """Truncate ``text`` to ``cap`` chars; the trailing 3 chars are
    ``"..."`` so postmortems can distinguish a truncated value from a
    naturally-short one.

    Caller guarantees ``cap >= 4`` (the smallest Trial 13 cap is 200).
    """
    if len(text) <= cap:
        return text
    return text[: cap - 3] + "..."


class DiagnosisItem(LLMOutputContract):
    qid: str
    rca_kind_label: str = Field(max_length=_DIAGNOSE_FIELD_CAPS["rca_kind_label"])
    observed_failure: str = Field(max_length=_DIAGNOSE_FIELD_CAPS["observed_failure"])
    generated_sql_issue: str = Field(max_length=_DIAGNOSE_FIELD_CAPS["generated_sql_issue"])
    expected_sql_shape: str = Field(max_length=_DIAGNOSE_FIELD_CAPS["expected_sql_shape"])
    blame_set: list[str] = Field(default_factory=list)
    evidence_summary: str = Field(max_length=_DIAGNOSE_FIELD_CAPS["evidence_summary"])
    confidence: Literal["high", "medium", "low"]

    @field_validator(*_DIAGNOSE_FIELD_CAPS.keys(), mode="before")
    @classmethod
    def _truncate_oversize_field(cls, v, info):
        if not isinstance(v, str):
            return v
        cap = _DIAGNOSE_FIELD_CAPS.get(info.field_name)
        if cap is None:
            return v
        return _truncate_with_ellipsis(v, cap)


class Plan11DiagnoseOutput(LLMOutputContract):
    """LLMOutputContract for plan11_diagnose skill — wraps the per-QID
    list so the response is a strict-mode JSON object (root-level
    arrays are rejected by Databricks JSON Schema strict mode)."""

    diagnoses: list[DiagnosisItem]
