"""Trial 19 C3 — ``pending_review`` candidate capture for
``both_correct + byte-mismatch`` rows.

Pins:

* ``build_gt_correction_candidate`` returns a row with
  ``status == "pending_review"`` (no auto-swap, no corpus mutation).
* The candidate carries the QID and the arbiter verdict so the human
  reviewer can audit it.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.ground_truth_corrections import (
    build_gt_correction_candidate,
    is_trial19_arbiter_correct_gt_disagrees,
)


def _make_arbiter_correct_gt_disagree_row(qid: str = "gs_009") -> dict:
    return {
        "inputs.question_id": qid,
        "inputs.question": "How many sales by week?",
        "inputs.expected_sql": "SELECT COUNT(*) FROM sales",
        "outputs.predictions.sql": "SELECT count(*) FROM sales",
        "feedback/arbiter/value": "both_correct",
        "feedback/arbiter/rationale": (
            "both SQL queries return semantically equivalent results"
        ),
        "feedback/result_correctness/value": "no",
    }


def test_predicate_fires_on_trial19_row():
    row = _make_arbiter_correct_gt_disagree_row()
    assert is_trial19_arbiter_correct_gt_disagrees(row) is True


def test_build_candidate_writes_pending_review_status():
    row = _make_arbiter_correct_gt_disagree_row()
    candidate = build_gt_correction_candidate(
        row, run_id="run-1", iteration=2,
    )
    assert candidate["status"] == "pending_review", (
        "Trial 19 C3 must write status=pending_review (no auto-swap)."
    )
    assert candidate["question_id"] == "gs_009"
    assert candidate["run_id"] == "run-1"
    assert candidate["iteration"] == 2
    assert candidate["arbiter_verdict"] == "both_correct"


def test_build_candidate_surfaces_arbiter_rationale():
    row = _make_arbiter_correct_gt_disagree_row()
    candidate = build_gt_correction_candidate(
        row, run_id="run-1", iteration=1,
    )
    # The rationale flows into the queue so reviewers can decide
    # whether to promote the corpus fix without re-running the eval.
    assert (
        "semantically equivalent" in (candidate["arbiter_rationale"] or "")
    )


def test_build_candidate_raises_when_qid_missing():
    """Trial 19 C3 must not silently emit unreviewable rows — the
    legacy ``arbiter=genie_correct`` branch and the new Trial 19
    branch share this contract."""
    row = {
        "feedback/arbiter/value": "both_correct",
        "feedback/result_correctness/value": "no",
    }
    with pytest.raises(ValueError):
        build_gt_correction_candidate(
            row, run_id="run-1", iteration=1,
        )
