"""Unit tests for tools.policy_replay.classify_payload.

Exercises every gate in the pilot RegressionDebtPolicy via synthetic
ReplayPayload inputs. Uses dataclasses.replace on a base happy
payload so each test is a single-axis change.
"""
from __future__ import annotations

import dataclasses

import pytest

from genie_space_optimizer.optimization.acceptance_policy import (
    regression_debt_policy_pilot_default,
)
from genie_space_optimizer.tools.policy_replay import (
    ReplayPayload,
    classify_payload,
)


def _base_payload() -> ReplayPayload:
    """Minimal happy-path payload that satisfies the pilot policy:
    target fixed, +12pp gain, zero debt, accepted-clean."""
    return ReplayPayload(
        fixture_id="synth_happy",
        run_id="00000000-0000-0000-0000-000000000000",
        iteration=1,
        ag_id="AG1",
        payload_present=True,
        baseline_post_arbiter=80.0,
        candidate_post_arbiter=92.0,
        baseline_pre_arbiter=None,
        candidate_pre_arbiter=None,
        target_qids=("q009",),
        target_fixed_qids=("q009",),
        target_still_hard_qids=(),
        out_of_target_regressed_qids=(),
        soft_to_hard_regressed_qids=(),
        passing_to_hard_regressed_qids=(),
        unknown_to_hard_regressed_qids=(),
        accepted_in_recorded_run=True,
        reason_code_in_recorded_run="accepted",
    )


def test_classify_no_payload_short_circuits() -> None:
    payload = dataclasses.replace(
        _base_payload(),
        fixture_id="synth_absent",
        payload_present=False,
        target_qids=(),
        target_fixed_qids=(),
    )
    classification = classify_payload(
        payload=payload,
        policy=regression_debt_policy_pilot_default(),
        policy_name="pilot",
    )
    assert classification.payload_present is False
    assert classification.accepted is None
    assert classification.reason_code == "no_payload"
    assert classification.first_failed_gate is None


def test_classify_target_fixed_no_debt_returns_clean_accept() -> None:
    """When target is fixed AND there is no debt, the verdict
    short-circuits to no_debt_present (under_policy=True) — the
    legacy `accepted` reason in production. We surface this as
    accepted with reason_code='no_debt_to_harvest' in the
    classifier output so the result is distinguishable from the
    debt-harvest branch."""
    classification = classify_payload(
        payload=_base_payload(),
        policy=regression_debt_policy_pilot_default(),
        policy_name="pilot",
    )
    assert classification.accepted is True
    assert classification.reason_code == "no_debt_to_harvest"
    assert classification.first_failed_gate is None
    assert classification.debt_qids == ()


def test_classify_no_target_fixed_fails_first_gate() -> None:
    """ccf1d60d iter-1 shape: net positive, target_fixed_count=0.
    Pilot policy gates this on min_target_clusters_fixed."""
    payload = dataclasses.replace(
        _base_payload(),
        target_fixed_qids=(),  # no target fixed
        target_still_hard_qids=("q009",),
        out_of_target_regressed_qids=("q012",),
        unknown_to_hard_regressed_qids=("q012",),
    )
    classification = classify_payload(
        payload=payload,
        policy=regression_debt_policy_pilot_default(),
        policy_name="pilot",
    )
    assert classification.accepted is False
    assert classification.first_failed_gate == "min_target_clusters_fixed"
    assert classification.policy_diagnostics["target_clusters_fixed"] == 0
    assert classification.policy_diagnostics["target_clusters_fixed_min"] == 1


def test_classify_aggregate_gain_below_floor_fails_second_gate() -> None:
    """Target IS fixed but +4pp < 10pp pilot floor."""
    payload = dataclasses.replace(
        _base_payload(),
        baseline_post_arbiter=87.0,
        candidate_post_arbiter=91.0,  # +4pp
        out_of_target_regressed_qids=("q012",),
        soft_to_hard_regressed_qids=("q012",),
    )
    classification = classify_payload(
        payload=payload,
        policy=regression_debt_policy_pilot_default(),
        policy_name="pilot",
    )
    assert classification.accepted is False
    assert classification.first_failed_gate == "min_aggregate_improvement_pp"


def test_classify_unknown_to_hard_debt_bucket_disallowed() -> None:
    """Pilot allows ONLY soft_to_hard debt; unknown_to_hard is
    rejected at the bucket gate."""
    payload = dataclasses.replace(
        _base_payload(),
        baseline_post_arbiter=80.0,
        candidate_post_arbiter=92.0,  # +12pp clears the floor
        out_of_target_regressed_qids=("q012",),
        unknown_to_hard_regressed_qids=("q012",),
    )
    classification = classify_payload(
        payload=payload,
        policy=regression_debt_policy_pilot_default(),
        policy_name="pilot",
    )
    assert classification.accepted is False
    assert classification.first_failed_gate == "allowed_debt_buckets"


def test_classify_soft_to_hard_debt_within_pilot_accepts() -> None:
    """Single soft_to_hard debt within max_debt_qids=1 is the only
    pilot-allowed accept-with-debt path."""
    payload = dataclasses.replace(
        _base_payload(),
        baseline_post_arbiter=80.0,
        candidate_post_arbiter=92.0,
        out_of_target_regressed_qids=("q012",),
        soft_to_hard_regressed_qids=("q012",),
    )
    classification = classify_payload(
        payload=payload,
        policy=regression_debt_policy_pilot_default(),
        policy_name="pilot",
    )
    assert classification.accepted is True
    assert classification.reason_code == "accepted_with_partial_harvest_debt"
    assert classification.first_failed_gate is None
    assert classification.debt_qids == ("q012",)
