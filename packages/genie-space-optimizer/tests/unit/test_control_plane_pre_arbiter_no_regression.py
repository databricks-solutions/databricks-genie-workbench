"""WU-4 — accepted_pre_arbiter_improvement must require delta >= 0 AND has_causal_fix."""
from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    decide_control_plane_acceptance,
)


def _row(qid: str, status: str) -> dict:
    """Return a minimal eval row matching control_plane.row_is_hard_failure
    semantics (result_correctness=no AND arbiter NOT in correct set =>
    hard)."""
    if status == "passing":
        return {
            "question_id": qid,
            "result_correctness": "yes",
            "arbiter": "both_correct",
        }
    if status == "soft":
        return {
            "question_id": qid,
            "result_correctness": "no",
            "arbiter": "both_correct",  # arbiter overrides → not hard
        }
    # hard: rc=no AND arbiter not in correct verdicts
    return {
        "question_id": qid,
        "result_correctness": "no",
        "arbiter": "neither_correct",
    }


def test_pre_arbiter_branch_rejects_when_post_arbiter_regressed():
    """7now iter-1 reproduction: target fixed, no out-of-target
    regression (collateral_clear), pre-arbiter improved >= 2.0pp,
    but post-arbiter accuracy regressed. Pre-WU-4 this accepted;
    WU-4 rejects with reason ``post_arbiter_regressed_pre_arbiter_only``.

    Row shape is constructed to satisfy:
      - pre_hard != post_hard (avoids the stale_or_candidate_pre_rows guard)
      - target_fixed non-empty (gs_013 hard → passing)
      - out_of_target_regressed empty (gs_014 was hard pre and stays hard)
      - global delta_pp < 0 (explicit accuracy values)
    """
    pre_rows = (
        [_row("gs_013", "hard"), _row("gs_014", "hard")]
        + [_row(f"gs_{i:03d}", "passing") for i in range(1, 24)
           if i not in (13, 14)]
    )
    post_rows = (
        [_row("gs_013", "passing"), _row("gs_014", "hard")]
        + [_row(f"gs_{i:03d}", "passing") for i in range(1, 24)
           if i not in (13, 14)]
    )
    decision = decide_control_plane_acceptance(
        baseline_accuracy=91.3,
        candidate_accuracy=78.3,
        target_qids=("gs_013",),
        pre_rows=pre_rows,
        post_rows=post_rows,
        min_gain_pp=0.0,
        baseline_pre_arbiter_accuracy=80.0,
        candidate_pre_arbiter_accuracy=82.5,
        min_pre_arbiter_gain_pp=2.0,
        thresholds_met=False,
    )
    assert decision.accepted is False, (
        "WU-4 regression: pre-arbiter branch accepted while post-"
        "arbiter accuracy regressed"
    )
    assert decision.reason_code == "post_arbiter_regressed_pre_arbiter_only", (
        f"Expected typed reason; got {decision.reason_code!r}"
    )


def test_pre_arbiter_branch_rejects_when_no_target_fixed():
    """Post-arbiter held steady (no regression) BUT the named target
    did not move. Pre-WU-4 accepted; WU-4 requires has_causal_fix=True."""
    pre_rows = [_row("gs_013", "hard")] + [
        _row(f"gs_{i:03d}", "passing") for i in range(1, 24) if i != 13
    ]
    post_rows = list(pre_rows)  # no movement
    decision = decide_control_plane_acceptance(
        baseline_accuracy=91.3,
        candidate_accuracy=91.3,
        target_qids=("gs_013",),
        pre_rows=pre_rows,
        post_rows=post_rows,
        min_gain_pp=0.0,
        baseline_pre_arbiter_accuracy=80.0,
        candidate_pre_arbiter_accuracy=82.5,
        min_pre_arbiter_gain_pp=2.0,
        thresholds_met=False,
    )
    assert decision.accepted is False
    assert decision.reason_code == "pre_arbiter_improvement_without_causal_fix"


def test_pre_arbiter_branch_still_accepts_when_target_fixed_and_no_regression():
    """Positive case — both new conditions hold."""
    pre_rows = [_row("gs_013", "hard")] + [
        _row(f"gs_{i:03d}", "passing") for i in range(1, 24) if i != 13
    ]
    # Candidate: gs_013 flipped to passing.
    post_rows = [_row("gs_013", "passing")] + [
        _row(f"gs_{i:03d}", "passing") for i in range(1, 24) if i != 13
    ]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=91.3,
        candidate_accuracy=91.3,
        target_qids=("gs_013",),
        pre_rows=pre_rows,
        post_rows=post_rows,
        min_gain_pp=0.0,
        baseline_pre_arbiter_accuracy=80.0,
        candidate_pre_arbiter_accuracy=82.5,
        min_pre_arbiter_gain_pp=2.0,
        thresholds_met=False,
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted_pre_arbiter_improvement"
