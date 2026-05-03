"""TDD: pre-arbiter secondary acceptance signal in the control-plane gate."""
from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    decide_control_plane_acceptance,
)


def _row(qid: str, *, hard: bool = False, passing: bool = False) -> dict:
    if passing:
        return {
            "question_id": qid,
            "result_correctness": "yes",
            "arbiter": "both_correct",
        }
    if hard:
        return {
            "question_id": qid,
            "result_correctness": "no",
            "arbiter": "ground_truth_correct",
        }
    return {
        "question_id": qid,
        "result_correctness": "yes",
        "arbiter": "genie_correct",
    }


def test_accepts_pre_arbiter_improvement_when_post_arbiter_saturated():
    """Iter-3 case: post 91.7 → 91.7 (delta 0); pre 83.3 → 87.5 (delta +4.2)."""
    pre_rows = [_row(f"q{i}", passing=True) for i in range(22)] + [
        _row("q23", hard=True),
        _row("q24", hard=True),
    ]
    post_rows = pre_rows  # same hard set, but pre-arbiter raw genie better

    decision = decide_control_plane_acceptance(
        baseline_accuracy=91.7,
        candidate_accuracy=91.7,
        target_qids=["q23"],
        pre_rows=pre_rows,
        post_rows=post_rows,
        min_gain_pp=0.0,
        baseline_pre_arbiter_accuracy=83.3,
        candidate_pre_arbiter_accuracy=87.5,
        min_pre_arbiter_gain_pp=2.0,
    )

    assert decision.accepted is True
    assert decision.reason_code == "accepted_pre_arbiter_improvement"


def test_rejects_when_post_arbiter_flat_and_pre_arbiter_flat():
    pre_rows = [_row("q23", hard=True)]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=50.0,
        candidate_accuracy=50.0,
        target_qids=["q23"],
        pre_rows=pre_rows,
        post_rows=pre_rows,
        baseline_pre_arbiter_accuracy=50.0,
        candidate_pre_arbiter_accuracy=50.0,
        min_pre_arbiter_gain_pp=2.0,
    )
    assert decision.accepted is False
    assert decision.reason_code == "post_arbiter_not_improved"


def test_rejects_when_post_arbiter_flat_pre_improved_but_collateral_regression():
    """A pre-arbiter improvement that introduces an out-of-target hard
    regression must NOT slip past the gate via the new branch."""
    pre_rows = [
        _row("q1", passing=True),
        _row("q23", hard=True),  # the target
    ]
    post_rows = [
        _row("q1", hard=True),  # collateral regression
        _row("q23", hard=True),  # target still hard
    ]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=50.0,
        candidate_accuracy=50.0,
        target_qids=["q23"],
        pre_rows=pre_rows,
        post_rows=post_rows,
        baseline_pre_arbiter_accuracy=80.0,
        candidate_pre_arbiter_accuracy=85.0,
        min_pre_arbiter_gain_pp=2.0,
    )
    assert decision.accepted is False
    # The new branch must yield to the existing collateral-regression
    # protection. The exact reason code may be the existing
    # `out_of_target_hard_regression` or `rejected_unbounded_collateral`;
    # whichever, but NEVER `accepted_pre_arbiter_improvement`.
    assert decision.reason_code != "accepted_pre_arbiter_improvement"


def test_pre_arbiter_branch_skipped_when_min_gain_pp_explicitly_set():
    """When the caller explicitly sets ``min_gain_pp > 0``, that's a hard
    post-arbiter gate; pre-arbiter cannot rescue."""
    pre_rows = [_row("q1", passing=True), _row("q23", hard=True)]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=91.7,
        candidate_accuracy=91.7,
        target_qids=["q23"],
        pre_rows=pre_rows,
        post_rows=pre_rows,
        min_gain_pp=2.0,
        baseline_pre_arbiter_accuracy=83.3,
        candidate_pre_arbiter_accuracy=87.5,
        min_pre_arbiter_gain_pp=2.0,
    )
    assert decision.accepted is False
    assert decision.reason_code == "rejected_no_gain"


def test_pre_arbiter_branch_skipped_when_pre_arbiter_inputs_omitted():
    """When the caller does not pass pre-arbiter inputs (legacy callers),
    the gate behaves exactly as it did before this PR."""
    pre_rows = [_row("q1", passing=True), _row("q23", hard=True)]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=91.7,
        candidate_accuracy=91.7,
        target_qids=["q23"],
        pre_rows=pre_rows,
        post_rows=pre_rows,
    )
    assert decision.accepted is False
    assert decision.reason_code == "post_arbiter_not_improved"


def test_pre_arbiter_branch_requires_threshold_to_be_met():
    """Pre-arbiter +1pp does not clear the +2pp threshold."""
    pre_rows = [_row("q1", passing=True), _row("q23", hard=True)]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=91.7,
        candidate_accuracy=91.7,
        target_qids=["q23"],
        pre_rows=pre_rows,
        post_rows=pre_rows,
        baseline_pre_arbiter_accuracy=86.0,
        candidate_pre_arbiter_accuracy=87.0,
        min_pre_arbiter_gain_pp=2.0,
    )
    assert decision.accepted is False
    assert decision.reason_code == "post_arbiter_not_improved"
