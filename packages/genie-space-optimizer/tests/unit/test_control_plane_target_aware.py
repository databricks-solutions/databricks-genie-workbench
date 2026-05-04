"""Optimizer Control-Plane Hardening Plan — Task A.

Tier-1 fix: when below thresholds, the
``accepted_with_attribution_drift`` branch must reject instead of
accepting. The default value of ``thresholds_met=True`` preserves
legacy behaviour; the harness flips this to the actual thresholds
state behind the ``GSO_TARGET_AWARE_ACCEPTANCE`` flag.
"""

from genie_space_optimizer.optimization.control_plane import (
    decide_control_plane_acceptance,
)


def _row(qid, rc, arbiter):
    return {"question_id": qid, "result_correctness": rc, "arbiter": arbiter}


PRE = (
    _row("gs_009", "no", "ground_truth_correct"),
    _row("gs_016", "no", "ground_truth_correct"),
    _row("gs_024", "no", "ground_truth_correct"),
    _row("gs_001", "yes", "both_correct"),
)
POST_DRIFT = (
    _row("gs_009", "no", "ground_truth_correct"),
    _row("gs_016", "yes", "both_correct"),
    _row("gs_024", "no", "ground_truth_correct"),
    _row("gs_001", "yes", "both_correct"),
)


def test_attribution_drift_accepted_when_thresholds_met():
    decision = decide_control_plane_acceptance(
        baseline_accuracy=25.0,
        candidate_accuracy=50.0,
        target_qids=("gs_009",),
        pre_rows=PRE,
        post_rows=POST_DRIFT,
        thresholds_met=True,
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_attribution_drift"


def test_attribution_drift_rejected_when_thresholds_unmet():
    decision = decide_control_plane_acceptance(
        baseline_accuracy=25.0,
        candidate_accuracy=50.0,
        target_qids=("gs_009",),
        pre_rows=PRE,
        post_rows=POST_DRIFT,
        thresholds_met=False,
    )
    assert decision.accepted is False
    assert decision.reason_code == "rejected_below_threshold_no_target_progress"
    assert decision.target_fixed_qids == ()
    assert decision.target_still_hard_qids == ("gs_009",)


def test_default_thresholds_met_preserves_legacy_behavior():
    decision = decide_control_plane_acceptance(
        baseline_accuracy=25.0,
        candidate_accuracy=50.0,
        target_qids=("gs_009",),
        pre_rows=PRE,
        post_rows=POST_DRIFT,
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_attribution_drift"
