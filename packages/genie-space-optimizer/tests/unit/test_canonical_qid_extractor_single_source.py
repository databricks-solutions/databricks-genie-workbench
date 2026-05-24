"""Phase 6 / Track 5 — single canonical QID extractor.

Two extractors exist today:

* :func:`genie_space_optimizer.optimization._qid_extraction.extract_question_id`
  — typed, returns ``(qid, source)``, used by admission and dispatch.
* :func:`genie_space_optimizer.optimization.eval_row_access.row_qid`
  — string-only, used by surface helpers and the typed evidence card
  builder.

Both ladders ran independent lookups. This test pins
``row_qid`` as a thin wrapper over ``extract_question_id`` so a future
divergence (e.g. someone widens canonical sources in one path but
not the other) cannot occur.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization._qid_extraction import (
    extract_question_id,
)
from genie_space_optimizer.optimization.eval_row_access import row_qid

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "production_eval_rows.json"
)


@pytest.fixture(scope="module")
def all_rows() -> list[dict]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [dict(r) for r in data["hydration_rows"]] + [
        dict(r) for r in data["eval_rows"]
    ]


def test_row_qid_returns_same_string_as_extract_question_id(
    all_rows: list[dict],
) -> None:
    for row in all_rows:
        access_qid = row_qid(row)
        canonical_qid, _source = extract_question_id(row)
        assert access_qid == canonical_qid, (
            f"row_qid disagrees with extract_question_id on shape "
            f"{row.get('_shape', '<unknown>')}: row_qid={access_qid!r}, "
            f"extract_question_id={canonical_qid!r}"
        )


def test_row_qid_returns_empty_when_no_qid_present() -> None:
    assert row_qid({}) == ""
    assert row_qid({"unrelated_key": "value"}) == ""


def test_row_qid_handles_trace_fallback_consistently() -> None:
    """trace_fallback rows: ``row_qid`` MUST mirror
    ``extract_question_id``'s "return the trace id" behavior. Callers
    that want canonical-only semantics check the ``source`` from
    ``extract_question_id`` directly; ``row_qid`` itself never lies
    about non-emptiness.
    """
    trace_row = {"client_request_id": "tr-abc123"}
    qid_from_access = row_qid(trace_row)
    qid_from_canonical, source = extract_question_id(trace_row)
    assert qid_from_access == qid_from_canonical
    assert source == "trace_fallback"
