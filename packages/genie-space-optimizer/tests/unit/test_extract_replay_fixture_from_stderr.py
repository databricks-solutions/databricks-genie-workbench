"""Phase 0.1 — extract PHASE_A replay fixture from Jobs API stderr.

For ``python_wheel_task`` / ``spark_python_task`` runs, Databricks
populates ``notebook_output.error_trace`` (NOT ``logs``) with the
stderr stream. Since the harness writes the replay fixture to
stderr (harness.py:28912-28914), the evidence bundle must read
``error_trace`` as a fallback source when ``logs`` and
``notebook_output.result`` are empty.
"""
from __future__ import annotations

from genie_space_optimizer.tools.evidence_bundle import (
    _extract_stdout_with_fallback,
)
from genie_space_optimizer.tools.evidence_layout import MissingPieceKind


_FIXTURE_BLOB = (
    "\n===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n"
    '{"iterations":[{"iter":1,"eval_rows":5}],"summary":{"iterations":1}}\n'
    "===PHASE_A_REPLAY_FIXTURE_JSON_END===\n"
)


def test_logs_field_used_when_populated():
    out = {"logs": _FIXTURE_BLOB}
    text, source, missing = _extract_stdout_with_fallback(out)
    assert "PHASE_A_REPLAY_FIXTURE_JSON_BEGIN" in text
    assert source == "logs"
    assert missing is None


def test_notebook_output_result_used_when_logs_empty():
    out = {"logs": "", "notebook_output": {"result": _FIXTURE_BLOB}}
    text, source, missing = _extract_stdout_with_fallback(out)
    assert "PHASE_A_REPLAY_FIXTURE_JSON_BEGIN" in text
    assert source == "notebook_output.result"
    assert missing is not None
    assert missing.kind == MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT


def test_error_trace_used_when_logs_and_result_empty():
    """NEW Phase 0.1 path: when both ``logs`` and
    ``notebook_output.result`` are empty, fall back to
    ``notebook_output.error_trace`` (Jobs API stderr surface for
    python_wheel/spark_python tasks)."""
    out = {
        "logs": "",
        "notebook_output": {"result": "", "error_trace": _FIXTURE_BLOB},
    }
    text, source, missing = _extract_stdout_with_fallback(out)
    assert "PHASE_A_REPLAY_FIXTURE_JSON_BEGIN" in text
    assert source == "notebook_output.error_trace"
    assert missing is not None
    assert missing.kind == MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT


def test_absent_when_all_three_empty():
    out = {"logs": "", "notebook_output": {"result": "", "error_trace": ""}}
    text, source, missing = _extract_stdout_with_fallback(out)
    assert text == ""
    assert source == "absent"
    assert missing is None


def test_concatenation_when_partial_fixture_split_across_sources():
    """If the fixture begin marker is in ``notebook_output.result``
    but the end marker is in ``error_trace`` (Databricks truncation
    edge case), the function returns the concatenated text so the
    downstream extractor can resplit on the markers."""
    out = {
        "logs": "",
        "notebook_output": {
            "result": "\n===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===\n{\"iter\":1",
            "error_trace": ",\"end\":true}\n===PHASE_A_REPLAY_FIXTURE_JSON_END===\n",
        },
    }
    text, source, missing = _extract_stdout_with_fallback(out)
    assert "PHASE_A_REPLAY_FIXTURE_JSON_BEGIN" in text
    assert "PHASE_A_REPLAY_FIXTURE_JSON_END" in text
    assert source == "notebook_output.result+error_trace"
    assert missing is not None
