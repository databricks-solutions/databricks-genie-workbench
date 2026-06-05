"""Trial 21 W8+C7 — unit tests for classify_run_outcome_from_aggregates.

Pins the scalar-input outcome classifier so the postmortem-replay
suite's Run A fixture (attribution_drift accept with target debt)
classifies consistently across releases.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.outcome import (
    classify_run_outcome_from_aggregates,
)


def test_attribution_drift_accept_with_target_debt_is_aggregate_gain_target_debt():
    """Run A's exact trajectory inputs: any_iteration_accepted + post>pre
    + decision='accepted_with_attribution_drift' + target_still_hard
    non-empty → OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT."""
    outcome = classify_run_outcome_from_aggregates(
        any_iteration_accepted=True,
        any_iteration_post_gt_pre=True,
        last_accepted_decision="accepted_with_attribution_drift",
        target_qids=("airline_ticketing_and_fare_analysis_gs_009",),
        target_fixed_qids=(),
        target_still_hard_qids=(
            "airline_ticketing_and_fare_analysis_gs_009",
        ),
    )
    assert outcome == "OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT"


def test_plain_accept_with_all_targets_fixed_is_improved():
    """Symmetric: plain ``accepted`` decision and every target fixed →
    OPTIMIZER_IMPROVED (the "happy path")."""
    outcome = classify_run_outcome_from_aggregates(
        any_iteration_accepted=True,
        any_iteration_post_gt_pre=True,
        last_accepted_decision="accepted",
        target_qids=("qid_a", "qid_b"),
        target_fixed_qids=("qid_a", "qid_b"),
        target_still_hard_qids=(),
    )
    assert outcome == "OPTIMIZER_IMPROVED"


def test_no_accepts_falls_through_to_tried_no_gain():
    outcome = classify_run_outcome_from_aggregates(
        any_iteration_accepted=False,
        any_iteration_post_gt_pre=False,
        last_accepted_decision="",
    )
    assert outcome == "OPTIMIZER_TRIED_NO_GAIN"


def test_accept_without_post_gt_pre_is_tried_insufficient_gain():
    """Iteration was accepted but the post score did not exceed pre —
    that's the kept_insufficient lane in the aggregate view."""
    outcome = classify_run_outcome_from_aggregates(
        any_iteration_accepted=True,
        any_iteration_post_gt_pre=False,
        last_accepted_decision="kept_insufficient",
    )
    assert outcome == "OPTIMIZER_TRIED_INSUFFICIENT_GAIN"


def test_attribution_drift_decision_value_admits_aggregate_gain_branch():
    """Pin the bug fix: the previous shape rejected
    ``accepted_with_attribution_drift`` because it only admitted the
    bare ``"accepted"`` literal. The fix admits both labels."""
    outcome = classify_run_outcome_from_aggregates(
        any_iteration_accepted=True,
        any_iteration_post_gt_pre=True,
        last_accepted_decision="accepted_with_attribution_drift",
        target_qids=(),
        target_fixed_qids=(),
        target_still_hard_qids=(),
    )
    # No target context → IMPROVED branch.
    assert outcome == "OPTIMIZER_IMPROVED"
