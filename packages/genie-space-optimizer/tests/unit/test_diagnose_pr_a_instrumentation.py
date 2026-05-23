"""PR-A — Stage 1 BadRequest diagnostic instrumentation.

The 2026-05-23 trial postmortems for runs 98ec8950 and dc89d1a9 showed
55 consecutive Stage 1 LLM calls returning BadRequestError with no
captured error body. ``_classify_llm_error`` mapped them to
``error_kind="unknown"`` because the substring match did not recognise
``BadRequestError`` and never inspected the exception message.

This test file pins the four PR-A invariants:

  1. On ``outcome="llm_error"`` the marker carries ``error_message``,
     ``endpoint``, and a non-``unknown`` ``error_kind`` when the body
     contains the standard 400 signal.
  2. A ``GSO_PLAN11_STAGE1_REQUEST_V1`` marker fires alongside every
     ``llm_error`` outcome, carrying the request fingerprint
     (skill_id, max_tokens, response_format keywords, endpoint).
  3. The full untruncated error body is persisted to
     ``{run_root}/llm_errors/stage1_{iteration}_{qid}.json``.
  4. The instrumentation is silent on the success path (no extra
     stdout, no disk writes).
"""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)


def _failing_qid_input(qid: str = "gs_009") -> dict:
    return {
        "qid": qid,
        "question_text": "Top 10 orders by revenue?",
        "ground_truth_sql": "SELECT * FROM orders ORDER BY revenue DESC LIMIT 10",
        "generated_sql": "SELECT * FROM orders WHERE revenue = MAX(revenue)",
        "judge_rationale": "Generated SQL finds max revenue, not top 10",
        "blame_set_seed": ["catalog.schema.orders.revenue"],
    }


def _bad_request_response(message: str) -> LlmReasoningResponse:
    """Build the response shape ``LlmReasoningCall.invoke`` returns
    when ``client.chat.completions.create`` raises ``BadRequestError``.

    ``error`` is the ``f"{type(exc).__name__}: {exc}"`` string that
    ``invoke`` materializes (see llm_reasoning_call.py line 120)."""
    return LlmReasoningResponse(
        call_id="plan11_stage1_diagnose.iter_1",
        skill_id="plan11_diagnose",
        succeeded=False,
        parsed_output=None,
        declined=None,
        raw_text="",
        tokens_input=0,
        tokens_output=0,
        duration_ms=4904,
        error=f"BadRequestError: {message}",
    )


def _capture(qid: str, response: LlmReasoningResponse, *, run_root: Path):
    """Drive ``diagnose_failing_qids`` once and return captured stdout."""
    import os

    from genie_space_optimizer.optimization.stages.diagnose import (
        diagnose_failing_qids,
    )

    buf = io.StringIO()
    old_env = os.environ.get("GSO_PHASE_H_BUNDLE_ROOT")
    os.environ["GSO_PHASE_H_BUNDLE_ROOT"] = str(run_root)
    try:
        with redirect_stdout(buf), patch(
            "genie_space_optimizer.optimization.stages.diagnose."
            "LlmReasoningCall"
        ) as MockCall:
            MockCall.return_value.invoke = MagicMock(return_value=response)
            diagnose_failing_qids(
                failing_qids=[_failing_qid_input(qid)],
                schema_columns=["catalog.schema.orders.revenue"],
                optimization_run_id="run_pra",
                iteration=1,
                w=MagicMock(),
            )
    finally:
        if old_env is None:
            os.environ.pop("GSO_PHASE_H_BUNDLE_ROOT", None)
        else:
            os.environ["GSO_PHASE_H_BUNDLE_ROOT"] = old_env
    return buf.getvalue()


# ── Invariant 1: structured llm_error marker ─────────────────────────


def test_badrequest_response_format_signal_marker_has_response_format_invalid(tmp_path):
    """A 400 body referencing ``response_format`` must surface as
    ``error_kind=response_format_invalid`` on the marker, with the
    body captured in ``error_message``."""
    msg = (
        "Error code: 400 - {'error': {'message': \"Invalid "
        "'response_format': json_schema with $ref is not supported\", "
        "'type': 'invalid_request_error'}}"
    )
    stdout = _capture("gs_009", _bad_request_response(msg), run_root=tmp_path)

    diag_lines = [
        ln for ln in stdout.splitlines()
        if ln.startswith("GSO_PLAN11_STAGE1_DIAGNOSIS_V1 ")
    ]
    assert len(diag_lines) == 1, stdout
    payload = json.loads(diag_lines[0].split(" ", 1)[1])
    assert payload["outcome"] == "llm_error"
    assert payload["exception_class"] == "BadRequestError"
    assert payload["error_kind"] == "response_format_invalid"
    assert "response_format" in payload["error_message"].lower()
    assert payload["endpoint"]  # non-empty


def test_badrequest_token_limit_signal_marker_has_token_limit_exceeded(tmp_path):
    msg = (
        "Error code: 400 - {'error': {'message': 'This model maximum "
        "context length is 200000 tokens, however you requested 220000'}}"
    )
    stdout = _capture("gs_021", _bad_request_response(msg), run_root=tmp_path)
    diag_lines = [
        ln for ln in stdout.splitlines()
        if ln.startswith("GSO_PLAN11_STAGE1_DIAGNOSIS_V1 ")
    ]
    payload = json.loads(diag_lines[0].split(" ", 1)[1])
    assert payload["error_kind"] == "token_limit_exceeded"


def test_marker_truncates_long_error_message_to_500_chars(tmp_path):
    """A 2 KB error body must be truncated on the stdout marker but
    persisted in full to the on-disk dump."""
    body = (
        "Error code: 400 - "
        + "x" * 2000
        + " response_format invalid trailing-marker"
    )
    stdout = _capture("gs_016", _bad_request_response(body), run_root=tmp_path)
    diag_lines = [
        ln for ln in stdout.splitlines()
        if ln.startswith("GSO_PLAN11_STAGE1_DIAGNOSIS_V1 ")
    ]
    payload = json.loads(diag_lines[0].split(" ", 1)[1])
    # ≤500 chars on the marker.
    assert len(payload["error_message"]) <= 500
    # Disk dump retains the full body.
    dump_path = tmp_path / "llm_errors" / "stage1_1_gs_016.json"
    assert dump_path.exists()
    dump = json.loads(dump_path.read_text())
    assert "trailing-marker" in dump["error_message"]


# ── Invariant 2: request fingerprint marker ──────────────────────────


def test_request_marker_fires_alongside_llm_error(tmp_path):
    """Every ``llm_error`` outcome must be accompanied by one
    ``GSO_PLAN11_STAGE1_REQUEST_V1`` marker per failing QID. This is
    what tells postmortems *what request shape* the endpoint rejected."""
    stdout = _capture(
        "gs_009",
        _bad_request_response("Error code: 400 - generic"),
        run_root=tmp_path,
    )
    req_lines = [
        ln for ln in stdout.splitlines()
        if ln.startswith("GSO_PLAN11_STAGE1_REQUEST_V1 ")
    ]
    assert len(req_lines) == 1, stdout
    payload = json.loads(req_lines[0].split(" ", 1)[1])
    assert payload["skill_id"] == "plan11_diagnose"
    assert payload["max_tokens"] > 0
    assert payload["system_msg_chars"] > 0
    assert payload["user_prompt_chars"] > 0
    assert payload["endpoint"]
    # The Plan 11 envelope is a Pydantic AbstainableEnvelope[T]; we
    # expect at least the top-level json_schema keywords to appear.
    keywords = payload["response_format_keywords"]
    assert any("json_schema" in k for k in keywords), keywords
    assert payload["qid"] == "gs_009"
    assert payload["iteration"] == 1


# ── Invariant 3: on-disk dump persists full body ─────────────────────


def test_disk_dump_persists_full_body(tmp_path):
    body = (
        "Error code: 400 - {'error': {'message': "
        "'detailed databricks rejection text', 'type': 'x'}}"
    )
    _capture("gs_024", _bad_request_response(body), run_root=tmp_path)
    dump_path = tmp_path / "llm_errors" / "stage1_1_gs_024.json"
    assert dump_path.exists()
    dump = json.loads(dump_path.read_text())
    assert dump["optimization_run_id"] == "run_pra"
    assert dump["iteration"] == 1
    assert dump["qid"] == "gs_024"
    assert dump["skill_id"] == "plan11_diagnose"
    assert dump["endpoint"]
    assert "detailed databricks rejection text" in dump["error_message"]
    assert dump["max_tokens"] > 0


def test_disk_dump_failure_does_not_break_marker_emission(tmp_path, monkeypatch):
    """The disk persist path is best-effort — even when the filesystem
    rejects the write, the diagnosis marker must still emit."""

    def _broken_mkdir(self, *args, **kwargs):
        raise OSError("simulated filesystem rejection")

    monkeypatch.setattr(Path, "mkdir", _broken_mkdir)
    stdout = _capture(
        "gs_004",
        _bad_request_response("Error code: 400 - x"),
        run_root=tmp_path,
    )
    # The diagnosis marker is the load-bearing observability artifact;
    # it MUST still appear even if the disk dump fails.
    assert any(
        ln.startswith("GSO_PLAN11_STAGE1_DIAGNOSIS_V1 ")
        for ln in stdout.splitlines()
    )


# ── Invariant 4: success path emits no instrumentation noise ─────────


def test_success_path_emits_no_request_marker_no_disk_dump(tmp_path):
    success = LlmReasoningResponse(
        call_id="plan11_stage1_diagnose.iter_1",
        skill_id="plan11_diagnose",
        succeeded=True,
        parsed_output={
            "diagnoses": [
                {
                    "qid": "gs_009",
                    "rca_kind_label": "top-N collapsed",
                    "observed_failure": "x",
                    "generated_sql_issue": "y",
                    "expected_sql_shape": "z",
                    "blame_set": [],
                    "evidence_summary": "ok",
                    "confidence": "high",
                },
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=100,
        tokens_output=50,
        duration_ms=1234,
        error=None,
    )
    stdout = _capture("gs_009", success, run_root=tmp_path)
    assert not any(
        ln.startswith("GSO_PLAN11_STAGE1_REQUEST_V1 ")
        for ln in stdout.splitlines()
    )
    # No llm_errors directory should be created on the happy path.
    assert not (tmp_path / "llm_errors").exists()
