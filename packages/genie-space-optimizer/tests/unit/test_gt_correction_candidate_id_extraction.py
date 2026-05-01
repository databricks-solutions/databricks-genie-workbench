"""Pin that question_id is extracted from every common row shape."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.ground_truth_corrections import (
    build_gt_correction_candidate,
)


def test_extracts_from_inputs_dot_question_id() -> None:
    candidate = build_gt_correction_candidate(
        {"inputs.question_id": "gs_009", "feedback/arbiter/value": "genie_correct"},
        run_id="r", iteration=0,
    )
    assert candidate["question_id"] == "gs_009"


def test_extracts_from_inputs_slash_question_id() -> None:
    candidate = build_gt_correction_candidate(
        {"inputs/question_id": "gs_017", "feedback/arbiter/value": "genie_correct"},
        run_id="r", iteration=0,
    )
    assert candidate["question_id"] == "gs_017"


def test_extracts_from_nested_inputs_dict() -> None:
    candidate = build_gt_correction_candidate(
        {"inputs": {"question_id": "gs_026"}, "feedback/arbiter/value": "genie_correct"},
        run_id="r", iteration=0,
    )
    assert candidate["question_id"] == "gs_026"


def test_extracts_from_metadata_question_id() -> None:
    candidate = build_gt_correction_candidate(
        {"metadata": {"question_id": "gs_021"}, "feedback/arbiter/value": "genie_correct"},
        run_id="r", iteration=0,
    )
    assert candidate["question_id"] == "gs_021"


def test_raises_when_question_id_cannot_be_extracted() -> None:
    with pytest.raises(ValueError, match="question_id"):
        build_gt_correction_candidate(
            {"feedback/arbiter/value": "genie_correct"},
            run_id="r", iteration=0,
        )
