from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    decide_control_plane_acceptance,
)


def _row(qid: str, arbiter: str, rc: str = "no") -> dict:
    return {
        "id": qid,
        "feedback/arbiter/value": arbiter,
        "feedback/result_correctness/value": rc,
    }


def test_accepts_ag2_shape_with_bounded_regression_debt() -> None:
    pre_rows = [
        _row("q026", "ground_truth_correct"),
        _row("q009", "ground_truth_correct"),
        _row("q021", "ground_truth_correct"),
        _row("q001", "both_correct", "yes"),
    ]
    post_rows = [
        _row("q026", "genie_correct"),
        _row("q009", "both_correct", "yes"),
        _row("q021", "ground_truth_correct"),
        _row("q001", "ground_truth_correct"),
    ]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=86.4,
        candidate_accuracy=90.9,
        target_qids=("q009", "q021"),
        pre_rows=pre_rows,
        post_rows=post_rows,
        min_gain_pp=2.0,
        max_new_hard_regressions=1,
        protected_qids=(),
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_regression_debt"
    assert decision.target_fixed_qids == ("q009",)
    assert decision.target_still_hard_qids == ("q021",)
    assert decision.regression_debt_qids == ("q001",)


def test_rejects_unbounded_collateral_when_debt_exceeds_fixed_count() -> None:
    pre_rows = [
        _row("q009", "ground_truth_correct"),
        _row("q001", "both_correct", "yes"),
        _row("q002", "both_correct", "yes"),
    ]
    post_rows = [
        _row("q009", "both_correct", "yes"),
        _row("q001", "ground_truth_correct"),
        _row("q002", "ground_truth_correct"),
    ]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=80.0,
        candidate_accuracy=85.0,
        target_qids=("q009",),
        pre_rows=pre_rows,
        post_rows=post_rows,
        min_gain_pp=2.0,
        max_new_hard_regressions=1,
        protected_qids=(),
    )
    assert decision.accepted is False
    assert decision.reason_code == "rejected_unbounded_collateral"
    assert decision.regression_debt_qids == ()
    assert decision.out_of_target_regressed_qids == ("q001", "q002")


def test_rejects_regression_of_protected_qid_even_with_net_gain() -> None:
    decision = decide_control_plane_acceptance(
        baseline_accuracy=80.0,
        candidate_accuracy=90.0,
        target_qids=("q009",),
        pre_rows=[
            _row("q009", "ground_truth_correct"),
            _row("q001", "both_correct", "yes"),
        ],
        post_rows=[
            _row("q009", "both_correct", "yes"),
            _row("q001", "ground_truth_correct"),
        ],
        min_gain_pp=2.0,
        max_new_hard_regressions=1,
        protected_qids=("q001",),
    )
    assert decision.accepted is False
    assert decision.reason_code == "rejected_unbounded_collateral"
    assert decision.protected_regressed_qids == ("q001",)


def test_rejects_no_gain_even_when_target_fixed() -> None:
    decision = decide_control_plane_acceptance(
        baseline_accuracy=86.4,
        candidate_accuracy=86.4,
        target_qids=("q009",),
        pre_rows=[_row("q009", "ground_truth_correct")],
        post_rows=[_row("q009", "both_correct", "yes")],
        min_gain_pp=2.0,
        max_new_hard_regressions=1,
        protected_qids=(),
    )
    assert decision.accepted is False
    assert decision.reason_code == "rejected_no_gain"
