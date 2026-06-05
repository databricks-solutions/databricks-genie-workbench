"""Trial 19 C1 — hard-QID admission filter unit test.

Verifies that ``is_trial19_arbiter_correct_gt_disagrees`` detects rows
where the arbiter says ``both_correct`` but the raw byte-match
``feedback/result_correctness/value`` is false — the case where the GT
corpus is likely missing an equivalently-valid alternative form. These
rows must be removed from the hard QID list (the harness's
``_analyze_and_distribute`` filter does this; this test covers the pure
predicate).
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.ground_truth_corrections import (
    is_trial19_arbiter_correct_gt_disagrees,
)


def test_arbiter_both_correct_byte_no_fires() -> None:
    row = {
        "question_id": "qid1",
        "feedback/arbiter/value": "both_correct",
        "feedback/result_correctness/value": "no",
    }
    assert is_trial19_arbiter_correct_gt_disagrees(row) is True


def test_arbiter_both_correct_byte_yes_does_not_fire() -> None:
    row = {
        "question_id": "qid1",
        "feedback/arbiter/value": "both_correct",
        "feedback/result_correctness/value": "yes",
    }
    assert is_trial19_arbiter_correct_gt_disagrees(row) is False


def test_arbiter_genie_correct_does_not_fire() -> None:
    # Distinct from is_gt_correction_candidate; Trial 19 only triggers
    # on ``both_correct``.
    row = {
        "question_id": "qid1",
        "feedback/arbiter/value": "genie_correct",
        "feedback/result_correctness/value": "no",
    }
    assert is_trial19_arbiter_correct_gt_disagrees(row) is False


def test_arbiter_ground_truth_correct_does_not_fire() -> None:
    row = {
        "question_id": "qid1",
        "feedback/arbiter/value": "ground_truth_correct",
        "feedback/result_correctness/value": "no",
    }
    assert is_trial19_arbiter_correct_gt_disagrees(row) is False


@pytest.mark.parametrize("byte_value", ["no", "false", "0", "0.0"])
def test_falsy_byte_values_all_trigger(byte_value: str) -> None:
    row = {
        "feedback/arbiter/value": "both_correct",
        "feedback/result_correctness/value": byte_value,
    }
    assert is_trial19_arbiter_correct_gt_disagrees(row) is True


def test_case_insensitive_arbiter() -> None:
    row = {
        "feedback/arbiter/value": "BOTH_CORRECT",
        "feedback/result_correctness/value": "NO",
    }
    assert is_trial19_arbiter_correct_gt_disagrees(row) is True


def test_empty_row_does_not_fire() -> None:
    assert is_trial19_arbiter_correct_gt_disagrees({}) is False
