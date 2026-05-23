"""Lock the canonical qid-extraction contract for every production row shape.

The dispatch / SM admission adapters have starved across five architectural
redesigns because each redesign hand-rolled ``row.get("question_id")`` while
production MLflow rows carry the qid under ``inputs/question_id``, nested
``inputs.question_id``, ``request.kwargs.question_id``, etc. The canonical
extractor lives at ``_qid_extraction.extract_question_id`` and was built in
Cycle 8 specifically so this divergence cannot recur. This test pins the
contract: every production-shape row resolves through that extractor.
"""
from __future__ import annotations

from genie_space_optimizer.optimization._qid_extraction import (
    extract_question_id,
)


def test_top_level_question_id_is_canonical() -> None:
    qid, source = extract_question_id({"question_id": "gs_009"})
    assert qid == "gs_009"
    assert source == "canonical"


def test_mlflow_flattened_inputs_slash_question_id_is_canonical() -> None:
    qid, source = extract_question_id({"inputs/question_id": "gs_009"})
    assert qid == "gs_009"
    assert source == "canonical"


def test_dotted_inputs_dot_question_id_is_canonical() -> None:
    qid, source = extract_question_id({"inputs.question_id": "gs_009"})
    assert qid == "gs_009"
    assert source == "canonical"


def test_nested_inputs_dict_question_id_is_canonical() -> None:
    qid, source = extract_question_id({"inputs": {"question_id": "gs_009"}})
    assert qid == "gs_009"
    assert source == "canonical"


def test_request_kwargs_json_string_question_id_is_canonical() -> None:
    qid, source = extract_question_id(
        {"request": '{"kwargs": {"question_id": "gs_009"}}'}
    )
    assert qid == "gs_009"
    assert source == "canonical"


def test_request_kwargs_dict_question_id_is_canonical() -> None:
    qid, source = extract_question_id(
        {"request": {"kwargs": {"question_id": "gs_009"}}}
    )
    assert qid == "gs_009"
    assert source == "canonical"


def test_missing_question_id_returns_empty() -> None:
    qid, source = extract_question_id({"unrelated_field": "x"})
    assert qid == ""
    assert source == ""


def test_client_request_id_is_trace_fallback_not_canonical() -> None:
    """``client_request_id`` is normally an MLflow trace id, not a benchmark
    qid. Returning it lets callers admit the row with a structured warning
    so producer-side qid-misrouting becomes visible (Cycle 7/8 lesson)."""
    qid, source = extract_question_id({"client_request_id": "tr-abc"})
    assert qid == "tr-abc"
    assert source == "trace_fallback"
