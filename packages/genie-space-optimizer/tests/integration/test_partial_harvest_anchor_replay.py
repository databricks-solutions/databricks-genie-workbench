"""Cycle 14B-T2 — anchor integration test for new-anchor 76457773587391.

The candidate scored +17.4pp (78.3 → 95.7) with one out-of-target
soft→hard regression on gs_018 and the target gs_026 fixed. Pre-T2
this rolled back and threw away the win; post-T2 with the pilot
policy on, it accepts-with-debt.
"""

from __future__ import annotations


def _row(qid: str, rc: str, arbiter: str) -> dict:
    return {"question_id": qid, "result_correctness": rc, "arbiter": arbiter}


def _soft_row(qid: str) -> dict:
    """Actionable-soft row (rc=yes / arbiter=both_correct with a failed
    non-info judge) — row_status returns "soft".
    """
    return {
        "question_id": qid,
        "result_correctness": "yes",
        "arbiter": "both_correct",
        "feedback/sql_correctness/value": "no",
    }


# Synthetic reproduction of the anchor's row shape. gs_026 is the
# named target; gs_018 is the soft-to-hard regression. Other QIDs
# are passing on both sides to keep the aggregate-gain math clean.
ANCHOR_PRE_ROWS = (
    _row("gs_026", "no", "ground_truth_correct"),  # baseline-hard target
    _soft_row("gs_018"),                              # baseline-soft
    _row("gs_001", "yes", "both_correct"),
    _row("gs_002", "yes", "both_correct"),
    _row("gs_003", "yes", "both_correct"),
)
ANCHOR_POST_ROWS = (
    _row("gs_026", "yes", "both_correct"),          # target FIXED
    _row("gs_018", "no", "ground_truth_correct"),   # SOFT_TO_HARD debt
    _row("gs_001", "yes", "both_correct"),
    _row("gs_002", "yes", "both_correct"),
    _row("gs_003", "yes", "both_correct"),
)


def test_anchor_iter1_accepts_with_debt_under_pilot_policy(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=ANCHOR_PRE_ROWS,
        post_rows=ANCHOR_POST_ROWS,
        max_new_hard_regressions=0,
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_partial_harvest_debt"
    assert decision.regression_debt_qids == ("gs_018",)
    assert decision.target_fixed_qids == ("gs_026",)
    # Sanity: the new field from C14-T0 is populated.
    assert dict(decision.target_delta_states)["gs_026"] == "fixed"


def test_anchor_iter1_full_discards_when_flag_off(monkeypatch) -> None:
    """Byte-stable replay: identical inputs, flag off, identical
    legacy outcome.
    """
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "0")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=ANCHOR_PRE_ROWS,
        post_rows=ANCHOR_POST_ROWS,
        max_new_hard_regressions=0,
    )
    assert decision.accepted is False
    assert decision.reason_code in {
        "rejected_unbounded_collateral",
        "target_fixed_offset_by_regression",
    }


def test_anchor_iter1_blocked_when_cumulative_cap_reached(monkeypatch) -> None:
    """Iteration 4 of a hypothetical run that already accumulated 3
    debt QIDs — pilot's cumulative_debt_max=3. The same candidate
    cannot accept further debt.
    """
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=ANCHOR_PRE_ROWS,
        post_rows=ANCHOR_POST_ROWS,
        max_new_hard_regressions=0,
        cumulative_debt=3,
        threshold_pass_rate=1.0,
    )
    assert decision.accepted is False
    assert decision.reason_code in {
        "rejected_unbounded_collateral",
        "target_fixed_offset_by_regression",
    }
