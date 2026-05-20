"""Plan 11 Stage 1 — LLM output contract for per-QID diagnosis.

The LLM returns a batch of DiagnosisItem objects — one per failing QID.
The AbstainableEnvelope wraps the result (or declined verdict) per the
LlmReasoningCall framework.

``max_length`` constraints on string fields apply at Pydantic validation
time. The Databricks JSON Schema subset doesn't support ``maxLength``,
so ``build_response_format`` strips them before sending the schema to
the model — but the constraint still rejects over-long responses on
the way back in.
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class DiagnosisItem(LLMOutputContract):
    qid: str
    rca_kind_label: str = Field(max_length=80)
    observed_failure: str = Field(max_length=200)
    generated_sql_issue: str = Field(max_length=300)
    expected_sql_shape: str = Field(max_length=300)
    blame_set: list[str] = Field(default_factory=list)
    evidence_summary: str = Field(max_length=400)
    confidence: Literal["high", "medium", "low"]


class Plan11DiagnoseOutput(LLMOutputContract):
    """LLMOutputContract for plan11_diagnose skill — wraps the per-QID
    list so the response is a strict-mode JSON object (root-level
    arrays are rejected by Databricks JSON Schema strict mode)."""

    diagnoses: list[DiagnosisItem]
