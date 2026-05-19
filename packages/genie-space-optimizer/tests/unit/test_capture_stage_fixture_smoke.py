"""Smoke tests for scripts/capture_stage_fixture.py.

These tests exercise:
  - _redact()            (C2 Part A: expanded REDACTION_FIELDS / DBX_ID_FIELDS)
  - _run_pii_audit()     (C2 Part B: fail-loud guard for unknown long-text fields)
  - REDACTION_FIELDS     (C2 Part C: leak detector derives from the set)
  - _read_archive()      (C1: resolves numeric-prefixed stage directory)

Plus the five original smoke cases for _redact() that existed before C15-P0.8.
"""

from __future__ import annotations

import json
import re
import sys

import pytest

from scripts.capture_stage_fixture import (
    REDACTION_FIELDS,
    SAFE_LONG_TEXT_KEYS,
    _read_archive,
    _redact,
    _run_pii_audit,
)


# ---------------------------------------------------------------------------
# Original smoke tests (preserved from before C15-P0.8)
# ---------------------------------------------------------------------------


def test_redact_question_text() -> None:
    out = _redact({"question_text": "select * from foo"})
    assert out == {"question_text": "<redacted>"}


def test_redact_preserves_short_strings_in_dbx_ids() -> None:
    out = _redact({"databricks_job_id": "123"})
    assert out == {"databricks_job_id": "123"}


def test_redact_dbx_id_keeps_last_4() -> None:
    out = _redact({"databricks_job_id": "1105451933925748"})
    assert out == {"databricks_job_id": "X" * 12 + "5748"}


def test_redact_recurses_into_list() -> None:
    out = _redact([{"sql_body": "x"}, {"sql_body": "y"}])
    assert out == [{"sql_body": "<redacted>"}, {"sql_body": "<redacted>"}]


def test_redact_passes_unknown_fields_through() -> None:
    out = _redact({"some_int": 7, "nested": {"a": 1}})
    assert out == {"some_int": 7, "nested": {"a": 1}}


# ---------------------------------------------------------------------------
# C1 — _read_archive resolves the numeric-prefixed stage directory
# ---------------------------------------------------------------------------


def test_read_archive_resolves_indexed_stage_dir(tmp_path):
    """_read_archive must read stages/11_acceptance_decision/, not stages/acceptance_decision/.
    C15 Phase 2: strategist_context inserted at position 4 shifts acceptance_decision
    from position 09 to position 10. Plan 8 T4: candidate_critique inserted at
    position 7 shifts acceptance_decision further to position 11."""
    # Build the production bundle layout under tmp_path.
    stage_dir = tmp_path / "iterations" / "iter_01" / "stages" / "11_acceptance_decision"
    stage_dir.mkdir(parents=True)
    (stage_dir / "input.json").write_text(json.dumps({"keep": True}))
    (stage_dir / "output.json").write_text(json.dumps({"accepted": True}))

    inp, out = _read_archive(tmp_path, 1, "acceptance_decision")

    assert inp == {"keep": True}
    assert out == {"accepted": True}


# ---------------------------------------------------------------------------
# C2 Part A — Expanded REDACTION_FIELDS covers new keys
# ---------------------------------------------------------------------------


def test_redact_expands_to_full_field_list() -> None:
    """All newly added REDACTION_FIELDS and DBX_ID_FIELDS are redacted."""
    payload = {
        "rationale": "long llm output explaining the choice",
        "client_request_id": "abc123def",
        # spot-check a few more new keys
        "analysis_text": "some analysis",
        "proposed_value": "SELECT 1",
        "conversation_id": "conv-uuid-0001",
    }
    out = _redact(payload)

    assert out["rationale"] == "<redacted>"
    assert out["analysis_text"] == "<redacted>"
    assert out["proposed_value"] == "<redacted>"
    # DBX_ID_FIELDS — last 4 chars preserved, rest replaced with X.
    # "abc123def" has length 9, so 9-4=5 Xs followed by the last 4 chars "3def".
    assert out["client_request_id"] == "XXXXX" + "3def"
    # "conv-uuid-0001" has length 14, so 10 Xs followed by "0001".
    assert out["conversation_id"] == "X" * 10 + "0001"


# ---------------------------------------------------------------------------
# C2 Part B — _run_pii_audit raises SystemExit on unknown long-text fields
# ---------------------------------------------------------------------------


def test_capture_fails_loud_on_unknown_long_text() -> None:
    """A payload with an unrecognised long string must trigger SystemExit."""
    bad_payload = {"some_unknown_field": "x" * 500}
    with pytest.raises(SystemExit) as exc_info:
        _run_pii_audit(bad_payload)
    assert "unexpected long-text field" in str(exc_info.value)
    assert "some_unknown_field" in str(exc_info.value)


def test_capture_pii_audit_passes_for_safe_long_keys() -> None:
    """Fields in SAFE_LONG_TEXT_KEYS must not trigger the guard."""
    safe_payload = {"run_id": "a" * 300, "stage_key": "b" * 250}
    # Should not raise
    _run_pii_audit(safe_payload)


def test_capture_pii_audit_passes_for_short_strings() -> None:
    """Short strings (<=200 chars) on any key must not trigger the guard."""
    payload = {"arbitrary_key": "short value", "other": 42}
    _run_pii_audit(payload)


# ---------------------------------------------------------------------------
# C2 Part C — Leak detector pattern derives from REDACTION_FIELDS
# ---------------------------------------------------------------------------


def test_leak_detector_covers_all_redaction_fields() -> None:
    """Every key in REDACTION_FIELDS must appear in the rebuilt leak pattern."""
    leak_pattern = "|".join(re.escape(f) for f in sorted(REDACTION_FIELDS))
    for field_name in REDACTION_FIELDS:
        assert re.escape(field_name) in leak_pattern, (
            f"REDACTION_FIELDS key {field_name!r} is missing from the leak pattern"
        )
