"""Tests for the shared canonical-QID extractor.

Cycle 7 taught us that ``client_request_id`` is normally an MLflow
trace ID (e.g. ``tr-...``), not a benchmark canonical QID. Cycle 8
revealed a second, narrower extractor in
``ground_truth_corrections._extract_question_id`` that did not learn
that lesson — it could not find the canonical QID on rows shaped like
``request.kwargs.question_id`` and silently dropped GT-correction
candidates.

This module pins the contract for a single shared helper that both
``harness._baseline_row_qid`` and
``ground_truth_corrections._extract_question_id`` delegate to. The
helper returns ``(qid, source)`` so callers that care about
canonical-vs-fallback (the GT-correction path needs to log a warning
when it falls back to a trace-id shape) can branch on the source.
"""

from __future__ import annotations

import json

from genie_space_optimizer.optimization._qid_extraction import (
    extract_question_id,
)


# ── canonical sources (priority chain) ───────────────────────────────


def test_top_level_question_id_is_canonical():
    qid, source = extract_question_id({"question_id": "airline_q_001"})

    assert qid == "airline_q_001"
    assert source == "canonical"


def test_top_level_id_alias_is_canonical():
    qid, source = extract_question_id({"id": "from_id"})

    assert qid == "from_id"
    assert source == "canonical"


def test_inputs_slash_form_is_canonical():
    qid, source = extract_question_id({"inputs/question_id": "from_inputs"})

    assert qid == "from_inputs"
    assert source == "canonical"


def test_inputs_dot_form_is_canonical():
    # Flattened MLflow eval shape (dot form). Already covered by the
    # ground_truth extractor; include in the shared contract.
    qid, source = extract_question_id({"inputs.question_id": "from_dot"})

    assert qid == "from_dot"
    assert source == "canonical"


def test_inputs_nested_dict_is_canonical():
    qid, source = extract_question_id({"inputs": {"question_id": "from_nested"}})

    assert qid == "from_nested"
    assert source == "canonical"


def test_request_kwargs_dict_is_canonical():
    row = {"request": {"kwargs": {"question_id": "airline_q_canonical"}}}

    qid, source = extract_question_id(row)

    assert qid == "airline_q_canonical"
    assert source == "canonical"


def test_request_kwargs_json_string_is_canonical():
    # ``request`` can be persisted as a JSON-encoded string in some
    # MLflow eval-table shapes. Track D's _baseline_row_qid handles
    # this; the shared helper must too.
    row = {
        "request": json.dumps({"kwargs": {"question_id": "from_json_kwargs"}}),
    }

    qid, source = extract_question_id(row)

    assert qid == "from_json_kwargs"
    assert source == "canonical"


def test_request_top_level_question_id_is_canonical():
    # Some shapes put question_id at the top level of request itself,
    # not nested under kwargs.
    row = {"request": {"question_id": "from_request_top"}}

    qid, source = extract_question_id(row)

    assert qid == "from_request_top"
    assert source == "canonical"


def test_metadata_nested_dict_is_canonical():
    # Synthetic harness rows put question_id under ``metadata``.
    # The ground_truth extractor used to handle this; preserve it
    # in the shared helper for back-compat.
    qid, source = extract_question_id({"metadata": {"question_id": "from_meta"}})

    assert qid == "from_meta"
    assert source == "canonical"


# ── canonical-source priority over trace-id aliases ──────────────────


def test_canonical_inputs_wins_over_client_request_id():
    """Cycle 7 row shape: trace ID present, canonical also present.
    The canonical must win, otherwise the carrier ends up with trace
    IDs in eval_rows (the original Track D bug)."""
    row = {
        "client_request_id": "tr-f74a86401aa0b8e292f602e0069d867d",
        "inputs": {"question_id": "airline_ticketing_and_fare_analysis_gs_024"},
    }

    qid, source = extract_question_id(row)

    assert qid == "airline_ticketing_and_fare_analysis_gs_024"
    assert source == "canonical"


def test_canonical_request_kwargs_wins_over_client_request_id():
    row = {
        "client_request_id": "tr-aaa",
        "request": {"kwargs": {"question_id": "airline_q_canonical"}},
    }

    qid, source = extract_question_id(row)

    assert qid == "airline_q_canonical"
    assert source == "canonical"


def test_top_level_question_id_wins_over_inputs_and_trace_id():
    row = {
        "question_id": "top_level_canonical",
        "client_request_id": "tr-aaa",
        "inputs": {"question_id": "inputs_canonical"},
    }

    qid, source = extract_question_id(row)

    assert qid == "top_level_canonical"
    assert source == "canonical"


# ── trace-id last-resort fallback ────────────────────────────────────


def test_client_request_id_only_is_trace_fallback():
    row = {"client_request_id": "tr-abc"}

    qid, source = extract_question_id(row)

    assert qid == "tr-abc"
    assert source == "trace_fallback"


def test_request_id_only_is_trace_fallback():
    row = {"request_id": "tr-xyz"}

    qid, source = extract_question_id(row)

    assert qid == "tr-xyz"
    assert source == "trace_fallback"


def test_client_request_id_carrying_canonical_value_is_still_tagged_fallback():
    # Source-of-truth is the LOOKUP PATH, not the value's shape. If a
    # row's only id-bearing key is client_request_id, callers should
    # see ``trace_fallback`` and decide what to do with it. Treating
    # values shaped like ``airline_...`` as canonical here would mask
    # producer-side bugs that put canonical QIDs in the wrong key.
    row = {"client_request_id": "airline_q_001"}

    qid, source = extract_question_id(row)

    assert qid == "airline_q_001"
    assert source == "trace_fallback"


# ── empty / unidentifiable rows ──────────────────────────────────────


def test_empty_row_returns_empty_qid_and_empty_source():
    qid, source = extract_question_id({})

    assert qid == ""
    assert source == ""


def test_unrelated_keys_return_empty():
    qid, source = extract_question_id({"unrelated": "value", "other": 42})

    assert qid == ""
    assert source == ""


def test_blank_canonical_value_falls_through_to_next_source():
    # A row carrying ``"question_id": ""`` (blank) plus a real
    # ``inputs.question_id`` must return the inputs value, not the
    # blank top-level. ``_baseline_row_qid``'s short-circuit ``or``
    # already handles this; pin it here so the refactor preserves it.
    row = {
        "question_id": "",
        "inputs": {"question_id": "non_blank_canonical"},
    }

    qid, source = extract_question_id(row)

    assert qid == "non_blank_canonical"
    assert source == "canonical"
