"""Phase 5 / Track 4 — ``diagnosis_actionable`` boolean on
``GSO_PLAN11_STAGE1_DIAGNOSIS_V1``.

Trial 10/11 produced ``outcome=diagnosed`` markers that postmortems
read as success — yet Stage 1 still emitted "insufficient evidence to
determine root cause" with ``blame_set_size=0`` and
``evidence_summary_chars=0``. The mechanical success of the LLM call
masked the semantic failure of the diagnosis. ``diagnosis_actionable``
is the typed signal postmortems use to distinguish "mechanically
succeeded" from "actually produced something Plan 11 can act on."

Computed from:
- ``evidence_summary_chars > 0``
- ``rca_kind_label`` is not the "insufficient evidence" sentinel
- ``blame_set_size > 0``

When ``outcome`` is anything other than ``"diagnosed"``, the field is
``False`` (Stage 1 didn't produce a diagnosis, so nothing is actionable).
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.run_analysis_contract import (
    plan11_stage1_diagnosis_marker,
)


def _payload(line: str) -> dict:
    return json.loads(line.split(" ", 1)[1])


def test_diagnosed_with_full_evidence_is_actionable() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        outcome="diagnosed",
        rca_kind_label="wrong_aggregation",
        blame_set_size=2,
        evidence_summary_chars=150,
    )
    assert _payload(line)["diagnosis_actionable"] is True


def test_diagnosed_but_insufficient_evidence_is_not_actionable() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        outcome="diagnosed",
        rca_kind_label="insufficient evidence to determine root cause",
        blame_set_size=2,
        evidence_summary_chars=150,
    )
    assert _payload(line)["diagnosis_actionable"] is False


def test_diagnosed_but_zero_blame_set_is_not_actionable() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        outcome="diagnosed",
        rca_kind_label="wrong_aggregation",
        blame_set_size=0,
        evidence_summary_chars=150,
    )
    assert _payload(line)["diagnosis_actionable"] is False


def test_diagnosed_but_zero_evidence_summary_chars_is_not_actionable() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        outcome="diagnosed",
        rca_kind_label="wrong_aggregation",
        blame_set_size=2,
        evidence_summary_chars=0,
    )
    assert _payload(line)["diagnosis_actionable"] is False


def test_declined_is_never_actionable() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        outcome="declined",
        abstain_reason="missing_schema_context",
        abstain_explanation="evidence card empty",
    )
    assert _payload(line)["diagnosis_actionable"] is False


def test_llm_error_is_never_actionable() -> None:
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        outcome="llm_error",
        error_kind="endpoint_decline",
        exception_class="BadRequestError",
        error_message="400",
    )
    assert _payload(line)["diagnosis_actionable"] is False


def test_diagnosis_actionable_case_insensitive_insufficient_evidence() -> None:
    # The "insufficient evidence" sentinel can appear in mixed case.
    line = plan11_stage1_diagnosis_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        outcome="diagnosed",
        rca_kind_label="Insufficient Evidence To Determine Root Cause",
        blame_set_size=2,
        evidence_summary_chars=150,
    )
    assert _payload(line)["diagnosis_actionable"] is False
