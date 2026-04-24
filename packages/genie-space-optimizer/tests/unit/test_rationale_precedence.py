"""Tests for :func:`_merge_row_sources` (A2 — rationale precedence fix).

Pins the corrected precedence:

1. Trace assessments (authoritative — what MLflow stored).
2. Run-scoped scorer feedback cache (captured at scorer return time).
3. ``<judge>/rationale`` / ``<judge>/metadata`` already in ``row_dict``
   (weakest — the flat ``results_df`` columns are known to misalign rows
   in some MLflow versions).

The legacy pre-fix behavior (``results_df`` flat column wins) is pinned
by :func:`test_legacy_precedence_pre_fix` as an ``xfail`` so it documents
what the bug used to look like.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.evaluation import (
    _build_summary_row,
    _JUDGE_ORDER,
    _merge_row_sources,
)


def test_trace_beats_flat_column():
    row = {"arbiter/rationale": "flat-col-wrong"}
    assessment = {"arbiter": {"rationale": "trace-correct", "metadata": {"ok": True}}}

    out = _merge_row_sources(row, assessment, None)

    assert out["arbiter/rationale"] == "trace-correct"
    assert out["arbiter/metadata"] == {"ok": True}


def test_trace_beats_cache_and_flat_column():
    row = {"arbiter/rationale": "flat-col-wrong"}
    assessment = {"arbiter": {"rationale": "trace-correct", "metadata": {}}}
    cache = {"arbiter": {"rationale": "cache-also-wrong", "metadata": {}}}

    out = _merge_row_sources(row, assessment, cache)

    assert out["arbiter/rationale"] == "trace-correct"


def test_cache_fills_when_trace_silent():
    row = {"arbiter/rationale": "flat-col-wrong"}
    assessment = {}
    cache = {"arbiter": {"rationale": "cache-correct", "metadata": {"sev": "minor"}}}

    out = _merge_row_sources(row, assessment, cache)

    assert out["arbiter/rationale"] == "cache-correct"
    assert out["arbiter/metadata"] == {"sev": "minor"}


def test_flat_column_preserved_when_trace_and_cache_silent():
    row = {"arbiter/rationale": "only-source", "arbiter/metadata": {"legacy": True}}

    out = _merge_row_sources(row, {}, {})

    assert out["arbiter/rationale"] == "only-source"
    assert out["arbiter/metadata"] == {"legacy": True}


def test_unknown_judge_leaves_row_untouched():
    row = {"arbiter/rationale": "A"}
    assessment = {"some_other_judge": {"rationale": "X", "metadata": {}}}

    out = _merge_row_sources(row, assessment, None)

    assert out["arbiter/rationale"] == "A"
    assert out["some_other_judge/rationale"] == "X"


def test_metadata_and_rationale_resolved_independently():
    row: dict = {}
    assessment = {"arbiter": {"rationale": "trace-rat", "metadata": {}}}
    cache = {"arbiter": {"rationale": "cache-rat", "metadata": {"from": "cache"}}}

    out = _merge_row_sources(row, assessment, cache)

    assert out["arbiter/rationale"] == "trace-rat"
    assert out["arbiter/metadata"] == {"from": "cache"}


def test_empty_trace_rationale_does_not_overwrite_cache():
    row: dict = {}
    assessment = {"arbiter": {"rationale": "", "metadata": {}}}
    cache = {"arbiter": {"rationale": "cache-filled", "metadata": {}}}

    out = _merge_row_sources(row, assessment, cache)

    assert out["arbiter/rationale"] == "cache-filled"


def test_merge_returns_same_dict_object():
    row: dict = {"keep": 1}
    out = _merge_row_sources(row, {}, {})
    assert out is row


@pytest.mark.xfail(
    reason=(
        "Pins the pre-A2 bug: flat column would win over trace rationale. "
        "Fixed by the A2 precedence inversion; kept as xfail to document "
        "the regression we intentionally closed."
    ),
    strict=True,
)
def test_legacy_precedence_pre_fix():
    row = {"arbiter/rationale": "flat-col-wrong"}
    assessment = {"arbiter": {"rationale": "trace-correct", "metadata": {}}}

    out = _merge_row_sources(row, assessment, None)

    assert out["arbiter/rationale"] == "flat-col-wrong"


# ── A4: _build_summary_row canonical view ──────────────────────────────


def test_build_summary_row_returns_all_judges_in_order():
    rows = _build_summary_row({})
    assert [r["judge"] for r in rows] == _JUDGE_ORDER
    assert all(r["value"] == "" and r["rationale"] == "" for r in rows)


def test_build_summary_row_extracts_verdict_and_rationale():
    row = {
        "arbiter/value": "genie_correct",
        "arbiter/rationale": "Arbiter rationale text",
        "syntax_validity/value": "yes",
        "syntax_validity/rationale": "SQL parses",
    }
    rows = {r["judge"]: r for r in _build_summary_row(row)}
    assert rows["arbiter"]["value"] == "genie_correct"
    assert rows["arbiter"]["rationale"] == "Arbiter rationale text"
    assert rows["syntax_validity"]["value"] == "yes"
    assert rows["syntax_validity"]["rationale"] == "SQL parses"


def test_assert_canonical_off_allows_missing_rationale(monkeypatch):
    monkeypatch.delenv("GSO_ASSERT_ROW_CANONICAL", raising=False)
    row = {"arbiter/value": "no", "arbiter/rationale": ""}
    rows = _build_summary_row(row)
    rec = next(r for r in rows if r["judge"] == "arbiter")
    assert rec["value"] == "no"
    assert rec["rationale"] == ""


def test_assert_canonical_on_raises_for_missing_rationale(monkeypatch):
    monkeypatch.setenv("GSO_ASSERT_ROW_CANONICAL", "1")
    row = {"arbiter/value": "no", "arbiter/rationale": ""}
    with pytest.raises(AssertionError, match="Non-canonical summary row"):
        _build_summary_row(row)


def test_assert_canonical_on_accepts_empty_verdict(monkeypatch):
    monkeypatch.setenv("GSO_ASSERT_ROW_CANONICAL", "1")
    rows = _build_summary_row({})
    assert all(r["value"] == "" and r["rationale"] == "" for r in rows)


def test_assert_canonical_on_accepts_verdict_with_rationale(monkeypatch):
    monkeypatch.setenv("GSO_ASSERT_ROW_CANONICAL", "1")
    row = {"arbiter/value": "no", "arbiter/rationale": "explanation"}
    rows = _build_summary_row(row)
    rec = next(r for r in rows if r["judge"] == "arbiter")
    assert rec["value"] == "no"
    assert rec["rationale"] == "explanation"
