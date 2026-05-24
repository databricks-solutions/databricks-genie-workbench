"""Phase 5 (Trial 13) — Non-actionable diagnosis hard gate.

The dc89d1a9 shadow batch path emitted 24 ``diagnosed`` markers; 21
carried ``diagnosis_actionable=false`` AND ``blame_set_size=0``. All 24
flowed into Stage 2. Five Stage 2 calls errored, 3 clustered, but none
of the resulting clusters had usable evidence — Stage 3 emitted 6
``empty_synthesis``. Trial 12 pinned the marker; Trial 13 ships the
hard gate.

The gate has three units of behaviour:

* ``classify_non_actionable_reason`` — pure classifier that maps a
  Stage 1 diagnosis triple (``rca_kind_label``, ``evidence_summary``,
  ``blame_set``) to a typed reason or the empty string when the
  diagnosis IS actionable.
* ``plan11_stage1_non_actionable_reject_marker`` — typed marker
  emitted at the rejection site (parallel to the
  ``plan11_stage1_input_card_empty_marker`` introduced in Trial 12).
* ``_invoke_stage1_llm`` (the SM Stage 1 transformer) decline path:
  when the classifier returns a non-empty reason, the response is
  ``succeeded=False, declined=f"non_actionable_diagnosis:{reason}"``
  and the marker is emitted before returning.

This test pins all three. The integration test
``test_sm_diagnosis_actionable_gate`` provides the end-to-end SM
contract; this unit suite drives the building blocks.
"""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout

import pytest


def test_classify_non_actionable_reason_zero_blame_set() -> None:
    """Empty blame_set with a non-sentinel rca_kind is the most common
    Trial 12 shadow case (21/24)."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        classify_non_actionable_reason,
    )

    reason = classify_non_actionable_reason(
        rca_kind_label="missing_filter",
        evidence_summary="The query lacks a WHERE clause filtering by date",
        blame_set=(),
    )
    assert reason == "zero_blame_set", reason


def test_classify_non_actionable_reason_zero_evidence() -> None:
    """Non-empty blame_set with empty evidence_summary is still
    non-actionable: downstream stages cannot reason without narrative.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        classify_non_actionable_reason,
    )

    reason = classify_non_actionable_reason(
        rca_kind_label="missing_filter",
        evidence_summary="",
        blame_set=("orders.date",),
    )
    assert reason == "zero_evidence", reason


def test_classify_non_actionable_reason_insufficient_evidence_sentinel() -> None:
    """The Stage 1 LLM explicitly admits it cannot classify.

    Both the legacy ``"insufficient evidence to determine root cause"``
    and minor casing variants are classified the same way.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        classify_non_actionable_reason,
    )

    reason = classify_non_actionable_reason(
        rca_kind_label="insufficient evidence to determine root cause",
        evidence_summary="some narrative text",
        blame_set=("orders.date",),
    )
    assert reason == "insufficient_evidence_sentinel", reason

    reason_upper = classify_non_actionable_reason(
        rca_kind_label="Insufficient Evidence to Determine Root Cause",
        evidence_summary="some narrative text",
        blame_set=("orders.date",),
    )
    assert reason_upper == "insufficient_evidence_sentinel", reason_upper


def test_classify_non_actionable_reason_actionable_returns_empty() -> None:
    """A genuinely actionable diagnosis returns ``""``."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        classify_non_actionable_reason,
    )

    reason = classify_non_actionable_reason(
        rca_kind_label="missing_filter",
        evidence_summary="WHERE clause omitted; expected order_date filter",
        blame_set=("orders.order_date",),
    )
    assert reason == "", reason


def test_non_actionable_reject_marker_roundtrips() -> None:
    """Marker emits a stable line we can parse out of stdout."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage1_non_actionable_reject_marker,
    )

    line = plan11_stage1_non_actionable_reject_marker(
        optimization_run_id="run_t13",
        iteration=1,
        qid="gs_009",
        reason="zero_blame_set",
        rca_kind_label="missing_filter",
        blame_set_size=0,
        evidence_summary_chars=42,
    )
    assert line.startswith("GSO_PLAN11_STAGE1_NON_ACTIONABLE_REJECT_V1 "), line
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["optimization_run_id"] == "run_t13"
    assert payload["iteration"] == 1
    assert payload["qid"] == "gs_009"
    assert payload["reason"] == "zero_blame_set"
    assert payload["rca_kind_label"] == "missing_filter"
    assert payload["blame_set_size"] == 0
    assert payload["evidence_summary_chars"] == 42


def test_non_actionable_reject_marker_rejects_unknown_reason() -> None:
    """Closed vocabulary — caller must use the typed reason set."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage1_non_actionable_reject_marker,
    )

    with pytest.raises(ValueError, match="unknown non-actionable reason"):
        plan11_stage1_non_actionable_reject_marker(
            optimization_run_id="r",
            iteration=0,
            qid="q",
            reason="totally_made_up",
            rca_kind_label="x",
            blame_set_size=0,
            evidence_summary_chars=0,
        )


def _stub_ctx(extras_diagnose=None):
    """Minimal ``TransformerContext`` for the Stage 1 transformer."""
    from genie_space_optimizer.optimization.state_machine.verdict import (
        TransformerContext,
        ValidationContext,
    )

    extras: dict = {}
    if extras_diagnose is not None:
        extras["diagnose_llm"] = extras_diagnose
    return TransformerContext(
        iteration=1,
        run_id="run_actionable_gate",
        validation_context=ValidationContext(
            iteration=1, run_id="run_actionable_gate", extras={},
        ),
        extras=extras,
    )


def _stub_state(qid: str = "gs_009"):
    from genie_space_optimizer.optimization.state_machine.state import (
        QuestionStateInIteration,
    )
    from genie_space_optimizer.optimization.state_machine.records import (
        HardQidSeenRecord,
    )
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
    )

    seen = HardQidSeenRecord(
        eval_row_id=f"row_{qid}",
        predicate="row_is_hard_failure",
        score=1.0,
        baseline_sql="SELECT 1",
        expected_shape="SELECT *",
        iteration_first_seen=1,
    )
    return QuestionStateInIteration(
        qid=qid,
        iteration=1,
        current_stage=FunnelStage.HARD_QID_SEEN,
        deepest_stage_reached=FunnelStage.HARD_QID_SEEN,
        seen=seen,
    )


def test_invoke_stage1_llm_rejects_non_actionable_via_stub_card() -> None:
    """When the diagnose stub returns a non-actionable card the
    Stage 1 transformer MUST:

    * emit the ``plan11_stage1_non_actionable_reject_marker`` on stdout,
    * return ``_Stage1Response(succeeded=False, declined="non_actionable_diagnosis:zero_blame_set")``,
    * NOT advance the state into DIAGNOSED.
    """
    from genie_space_optimizer.optimization.state_machine.transformers import (
        diagnose_llm as dlm,
    )

    def stub(*, state, ctx):
        return {
            "rca_kind_label": "missing_filter",
            "evidence_summary": "lots of narrative here, plenty of chars",
            "observed_failure": "wrong",
            "expected_sql_shape": "SELECT * WHERE date >= ...",
            "blame_set": [],  # the load-bearing zero
            "confidence": "low",
        }

    ctx = _stub_ctx(extras_diagnose=stub)
    state = _stub_state()

    buf = io.StringIO()
    with redirect_stdout(buf):
        response = dlm._invoke_stage1_llm(state, ctx)
    out = buf.getvalue()

    assert response.succeeded is False, response
    assert response.declined == "non_actionable_diagnosis:zero_blame_set", (
        response.declined
    )
    assert "GSO_PLAN11_STAGE1_NON_ACTIONABLE_REJECT_V1" in out, out
    assert '"reason":"zero_blame_set"' in out, out
    assert '"qid":"gs_009"' in out, out


def test_invoke_stage1_llm_passes_actionable_card_through() -> None:
    """Actionable diagnoses MUST NOT trip the gate.

    Sanity check: the new gate is targeted strictly to the
    non-actionable cases and does not regress the happy path.
    """
    from genie_space_optimizer.optimization.state_machine.transformers import (
        diagnose_llm as dlm,
    )

    def stub(*, state, ctx):
        return {
            "rca_kind_label": "missing_filter",
            "evidence_summary": "WHERE clause omitted; order_date filter expected",
            "observed_failure": "wrong",
            "expected_sql_shape": "SELECT * WHERE date >= ...",
            "blame_set": ["orders.order_date"],
            "confidence": "high",
        }

    ctx = _stub_ctx(extras_diagnose=stub)
    state = _stub_state()

    response = dlm._invoke_stage1_llm(state, ctx)

    assert response.succeeded is True, response
    assert response.declined is None
    assert response.parsed_output is not None
    assert response.parsed_output.rca_kind_label == "missing_filter"
