"""Unit tests for the stdout-source fallback helper."""
from __future__ import annotations

from genie_space_optimizer.tools.evidence_bundle import (
    _extract_stdout_with_fallback,
)
from genie_space_optimizer.tools.evidence_layout import MissingPieceKind


def test_logs_field_is_preferred_when_non_empty():
    out = {"logs": "stdout-from-logs", "notebook_output": {"result": "ignored"}}

    text, source, missing = _extract_stdout_with_fallback(out)

    assert text == "stdout-from-logs"
    assert source == "logs"
    assert missing is None


def test_falls_back_to_notebook_output_result_when_logs_empty():
    out = {
        "logs": "",
        "notebook_output": {
            "result": "GSO_RUN_MANIFEST_V1 ...",
            "truncated": False,
        },
    }

    text, source, missing = _extract_stdout_with_fallback(out)

    assert text == "GSO_RUN_MANIFEST_V1 ..."
    assert source == "notebook_output.result"
    assert missing is not None
    assert missing.kind == MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT
    assert "logs field empty" in missing.diagnosis
    assert "notebook task" in missing.diagnosis


def test_returns_empty_string_and_no_fallback_when_both_empty():
    out = {"logs": "", "notebook_output": {"result": ""}}

    text, source, missing = _extract_stdout_with_fallback(out)

    assert text == ""
    assert source == "absent"
    assert missing is None


def test_handles_missing_notebook_output_field():
    out = {"logs": ""}

    text, source, missing = _extract_stdout_with_fallback(out)

    assert text == ""
    assert source == "absent"


def test_handles_truncated_notebook_output():
    out = {
        "logs": "",
        "notebook_output": {"result": "partial...", "truncated": True},
    }

    text, source, missing = _extract_stdout_with_fallback(out)

    assert text == "partial..."
    assert source == "notebook_output.result"
    assert missing.kind == MissingPieceKind.STDOUT_FALLBACK_NOTEBOOK_OUTPUT
    assert "truncated" in missing.diagnosis.lower()
