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


def test_new_anchor_f2_target_resolution_failed_reproduction(monkeypatch) -> None:
    """Cycle 14-T0 regression test for new-anchor 76457773587391 F2.

    Before T0: target_fixed_qids=() AND target_still_hard_qids=()
    simultaneously, with reason_code=target_qids_not_improved (or
    missing_pre_rows when pre_rows is empty).

    After T0: target_delta_states contains the LOOKUP_FAILED entry,
    reason_code is the typed target_resolution_failed, and the
    invariant suite catches any drift.

    The QID names mirror the new-anchor postmortem evidence so a
    failing assertion in CI is easy to map back to the F2 finding.
    """
    monkeypatch.setenv("GSO_TARGET_DELTA_STRICT", "1")

    # Reproduce the exact F2 input shape: target gs_026 declared,
    # baseline pre_rows do not contain it (the upstream-row-assembly
    # bug that surfaced the issue), candidate's failed-question list
    # excludes it (only gs_018 failed in the candidate).
    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(),
        post_rows=(_row("gs_018", "no", "ground_truth_correct"),),
    )

    # Legacy bucket tuples: still empty (back-compat).
    assert decision.target_fixed_qids == ()
    assert decision.target_still_hard_qids == ()

    # New canonical surface: typed lookup-failed entry.
    assert dict(decision.target_delta_states)["gs_026"] == "lookup_failed"

    # Typed rollback reason replaces the silent legacy code.
    assert decision.reason_code == "target_resolution_failed"
    assert decision.accepted is False

    # I13 must catch any future regression where these three
    # surfaces drift apart.
    from genie_space_optimizer.optimization.invariants import (
        check_i13_target_delta_totality,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "target_qids": list(decision.target_qids),
                    "target_fixed_qids": list(decision.target_fixed_qids),
                    "target_still_hard_qids": list(decision.target_still_hard_qids),
                    "reason_code": decision.reason_code,
                    "target_delta_states": [
                        list(pair) for pair in decision.target_delta_states
                    ],
                },
            }
        ]
    }
    assert check_i13_target_delta_totality(evidence) == []
