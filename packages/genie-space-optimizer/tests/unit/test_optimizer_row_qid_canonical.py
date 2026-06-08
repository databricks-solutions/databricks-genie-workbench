"""Characterisation + delegation tests for ``optimizer._row_qid``.

``_row_qid`` is refactored to delegate canonical id resolution to the
single-source ``_qid_extraction.extract_question_id`` (closing the
HAND_ROLLED_QID_EXTRACTION invariant) while preserving its two non-canonical
behaviours: the question-TEXT fallback (so id-less rows still cluster by
content) and the caller's sentinel default. These tests pin that contract so
the delegation cannot silently drop a supported row shape.
"""

from __future__ import annotations

import json

from genie_space_optimizer.optimization.optimizer import _row_qid


def test_top_level_question_id():
    assert _row_qid({"question_id": "gs_005"}) == "gs_005"


def test_inputs_flat_question_id():
    assert _row_qid({"inputs/question_id": "gs_009"}) == "gs_009"


def test_request_dict_kwargs_question_id():
    assert _row_qid({"request": {"kwargs": {"question_id": "gs_007"}}}) == "gs_007"


def test_request_json_string_kwargs_question_id():
    row = {"request": json.dumps({"kwargs": {"question_id": "gs_008"}})}
    assert _row_qid(row) == "gs_008"


def test_canonical_question_id_wins_over_text():
    # A row carrying both a canonical id and question text resolves to the id.
    row = {"question_id": "gs_011", "inputs/question": "How many flights?"}
    assert _row_qid(row) == "gs_011"


def test_text_fallback_when_no_id():
    # No canonical/trace id: fall back to the question TEXT (content clustering).
    assert _row_qid({"inputs/question": "How many late flights?"}) == "How many late flights?"


def test_top_level_question_text_fallback():
    assert _row_qid({"question": "Top routes?"}) == "Top routes?"


def test_sentinel_default_when_empty():
    assert _row_qid({}) == "unknown"


def test_custom_sentinel_when_empty():
    assert _row_qid({}, fallback="q3") == "q3"
