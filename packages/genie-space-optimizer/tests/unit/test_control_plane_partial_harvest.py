"""Cycle 14B-T2 — partial-harvest branch in decide_control_plane_acceptance.

The new branch overrides the legacy rejected_unbounded_collateral /
target_qids_not_improved rejections when the candidate satisfies a
RegressionDebtPolicy. Behind GSO_PARTIAL_HARVEST_WITH_DEBT.
"""

from __future__ import annotations


def _row(qid: str, rc: str, arbiter: str) -> dict:
    return {"question_id": qid, "result_correctness": rc, "arbiter": arbiter}


def _soft_row(qid: str) -> dict:
    """Actionable-soft row — rc=yes / arbiter=both_correct with a
    failed non-info judge so row_status returns "soft"."""
    return {
        "question_id": qid,
        "result_correctness": "yes",
        "arbiter": "both_correct",
        "feedback/sql_correctness/value": "no",
    }


def test_partial_harvest_accepts_anchor_case_when_flag_on(monkeypatch) -> None:
    """The new-anchor F1+F3 case: gs_026 fixed (target), gs_018
    soft→hard, +17.4pp aggregate gain, all thresholds met. Pre-flag
    legacy code rejected; flag-on partial-harvest accepts.
    """
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _soft_row("gs_018"),
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _row("gs_018", "no", "ground_truth_correct"),
        ),
        max_new_hard_regressions=0,  # legacy gate would fail
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_partial_harvest_debt"
    assert decision.regression_debt_qids == ("gs_018",)
    assert decision.target_fixed_qids == ("gs_026",)


def test_partial_harvest_falls_back_to_legacy_when_flag_off(monkeypatch) -> None:
    """Flag-off: identical inputs, legacy rejection.

    This is the byte-stable replay guarantee: existing fixtures
    captured before C14B shipped continue to project the legacy
    reason code.
    """
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "0")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _soft_row("gs_018"),
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _row("gs_018", "no", "ground_truth_correct"),
        ),
        max_new_hard_regressions=0,
    )
    assert decision.accepted is False
    assert decision.reason_code in {
        "rejected_unbounded_collateral",
        "target_fixed_offset_by_regression",
    }


def test_partial_harvest_does_not_override_clean_accept(monkeypatch) -> None:
    """When the legacy code would already accept (target fixed, zero
    regressions), the partial-harvest branch must not change the
    reason code to accepted_with_partial_harvest_debt.
    """
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=89.1,
        target_qids=("gs_026",),
        pre_rows=(_row("gs_026", "no", "ground_truth_correct"),),
        post_rows=(_row("gs_026", "yes", "both_correct"),),
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted"


def test_partial_harvest_blocked_by_cumulative_cap(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _soft_row("gs_018"),
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _row("gs_018", "no", "ground_truth_correct"),
        ),
        max_new_hard_regressions=0,
        cumulative_debt=3,  # pilot's cumulative_debt_max=3 — cap hit
        threshold_pass_rate=1.0,
    )
    assert decision.accepted is False
    assert decision.reason_code == "rejected_unbounded_collateral"


def test_partial_harvest_reason_visible_in_format_helper(monkeypatch) -> None:
    """format_control_plane_acceptance_detail must surface the new
    reason code and regression_debt_qids so the postmortem reads
    cleanly without grepping.
    """
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
        format_control_plane_acceptance_detail,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _soft_row("gs_018"),
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _row("gs_018", "no", "ground_truth_correct"),
        ),
        max_new_hard_regressions=0,
    )
    text = format_control_plane_acceptance_detail(decision)
    assert "reason=accepted_with_partial_harvest_debt" in text
    assert "regression_debt_qids=gs_018" in text
