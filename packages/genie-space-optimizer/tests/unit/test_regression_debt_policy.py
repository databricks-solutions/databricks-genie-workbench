"""Cycle 14B-T1 — RegressionDebtPolicy dataclass + validation.

The policy is constructible only with valid values; invalid
combinations fail at construction so the optimizer never carries
a malformed policy into a decision.
"""

from __future__ import annotations

import pytest


def test_regression_debt_policy_default_is_hard_zero() -> None:
    """Flag-off / unset default = no debt allowed = byte-stable
    behavior on existing replay fixtures.
    """
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
    )

    policy = RegressionDebtPolicy()
    assert policy.max_debt_qids == 0
    assert policy.allowed_debt_buckets == frozenset()
    assert policy.min_aggregate_improvement_pp == 0.0
    assert policy.min_target_clusters_fixed == 0
    assert policy.min_threshold_pass_rate == 0.0
    assert policy.cumulative_debt_max == 0


def test_regression_debt_policy_pilot_default() -> None:
    """The pilot default applied when GSO_PARTIAL_HARVEST_WITH_DEBT=1
    matches the roadmap's stated values exactly.
    """
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
        regression_debt_policy_pilot_default,
    )
    from genie_space_optimizer.optimization.control_plane import DeltaState

    policy = regression_debt_policy_pilot_default()
    assert isinstance(policy, RegressionDebtPolicy)
    assert policy.max_debt_qids == 1
    assert policy.allowed_debt_buckets == frozenset({DeltaState.SOFT_TO_HARD})
    assert policy.min_aggregate_improvement_pp == 10.0
    assert policy.min_target_clusters_fixed == 1
    assert policy.min_threshold_pass_rate == 0.95
    assert policy.cumulative_debt_max == 3


def test_regression_debt_policy_is_frozen() -> None:
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
    )

    policy = RegressionDebtPolicy()
    with pytest.raises((AttributeError, TypeError)):
        policy.max_debt_qids = 5  # type: ignore[misc]


def test_regression_debt_policy_rejects_negative_max_debt() -> None:
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
    )

    with pytest.raises(ValueError, match="max_debt_qids"):
        RegressionDebtPolicy(max_debt_qids=-1)


def test_regression_debt_policy_rejects_pass_rate_outside_unit_interval() -> None:
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
    )

    with pytest.raises(ValueError, match="min_threshold_pass_rate"):
        RegressionDebtPolicy(min_threshold_pass_rate=1.5)
    with pytest.raises(ValueError, match="min_threshold_pass_rate"):
        RegressionDebtPolicy(min_threshold_pass_rate=-0.1)


def test_regression_debt_policy_rejects_cumulative_below_per_iter_max() -> None:
    """cumulative_debt_max must be >= max_debt_qids — otherwise the
    very first iteration that accepts debt would already exceed the
    cumulative budget, making the policy unreachable.
    """
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
    )

    with pytest.raises(ValueError, match="cumulative_debt_max"):
        RegressionDebtPolicy(max_debt_qids=2, cumulative_debt_max=1)


def test_regression_debt_policy_rejects_unknown_bucket() -> None:
    """allowed_debt_buckets must be a subset of DeltaState members."""
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
    )

    with pytest.raises(TypeError, match="DeltaState"):
        RegressionDebtPolicy(allowed_debt_buckets=frozenset({"soft_to_hard"}))


def test_regression_debt_policy_accepts_lookup_failed_bucket() -> None:
    """LOOKUP_FAILED is admissible as a debt bucket — operators may
    intentionally allow it during the C14-T0 pilot phase.
    """
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
    )
    from genie_space_optimizer.optimization.control_plane import DeltaState

    policy = RegressionDebtPolicy(
        max_debt_qids=1,
        allowed_debt_buckets=frozenset({DeltaState.LOOKUP_FAILED}),
        cumulative_debt_max=3,
    )
    assert DeltaState.LOOKUP_FAILED in policy.allowed_debt_buckets


# ── Task 3: regression_debt_policy_from_config() ─────────────────────


def test_regression_debt_policy_from_config_off_returns_hard_zero(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_WITH_DEBT", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_MAX_DEBT_QIDS", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_CUMULATIVE_MAX", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_MIN_AGG_IMPROVEMENT_PP", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_MIN_TARGETS_FIXED", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_MIN_THRESHOLD_PASS_RATE", raising=False)
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_from_config,
    )

    policy = regression_debt_policy_from_config()
    assert policy.max_debt_qids == 0
    assert policy.cumulative_debt_max == 0


def test_regression_debt_policy_from_config_on_returns_pilot(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_MAX_DEBT_QIDS", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_CUMULATIVE_MAX", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_MIN_AGG_IMPROVEMENT_PP", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_MIN_TARGETS_FIXED", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_MIN_THRESHOLD_PASS_RATE", raising=False)
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_from_config,
        regression_debt_policy_pilot_default,
    )

    assert regression_debt_policy_from_config() == regression_debt_policy_pilot_default()


def test_regression_debt_policy_from_config_honours_field_overrides(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_MAX_DEBT_QIDS", "2")
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_CUMULATIVE_MAX", "5")
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_from_config,
    )

    policy = regression_debt_policy_from_config()
    assert policy.max_debt_qids == 2
    assert policy.cumulative_debt_max == 5


# ── Cycle 14B-T1: evaluate_regression_debt — pure helper ──────────────


def _row(qid: str, rc: str, arbiter: str) -> dict:
    return {"question_id": qid, "result_correctness": rc, "arbiter": arbiter}


def _soft_row(qid: str) -> dict:
    """Construct an actionable-soft row (rc=yes / arbiter=both_correct
    with a failed non-info judge) so row_status returns "soft".

    The plan's prose calls _row(qid, "no", "neither_correct") a
    "soft baseline", but under the actual hard-failure predicate
    (rc=="no" AND arbiter not in correct verdicts) that row classifies
    as hard. This helper produces the row shape the plan's intent
    requires.
    """
    return {
        "question_id": qid,
        "result_correctness": "yes",
        "arbiter": "both_correct",
        "feedback/sql_correctness/value": "no",
    }


def _decision_with_one_soft_to_hard_debt():
    """ControlPlaneAcceptance shaped like the new-anchor F1+F3 case:
    target gs_026 fixed, gs_018 went soft→hard, +17.4pp aggregate
    gain, all thresholds met. Pre-policy this rejects via
    rejected_unbounded_collateral; post-policy under pilot it
    should accept-with-debt.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    return decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _soft_row("gs_018"),  # actionable soft baseline
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _row("gs_018", "no", "ground_truth_correct"),  # hard now
        ),
        max_new_hard_regressions=0,  # legacy gate would fail
    )


def test_evaluate_regression_debt_pilot_accepts_anchor_case() -> None:
    """The new-anchor F1+F3 case is the canonical accept-with-debt:
    +17.4pp gain, gs_026 fixed, gs_018 soft→hard, debt under cap.
    """
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    decision = _decision_with_one_soft_to_hard_debt()
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=regression_debt_policy_pilot_default(),
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    assert verdict.under_policy is True
    assert verdict.reason_code == "accepted_with_partial_harvest_debt"
    assert verdict.debt_qids == ("gs_018",)


def test_evaluate_regression_debt_rejects_when_too_many_debt_qids() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
        decide_control_plane_acceptance,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _soft_row("gs_018"),
            _soft_row("gs_004"),  # second soft-to-hard
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _row("gs_018", "no", "ground_truth_correct"),
            _row("gs_004", "no", "ground_truth_correct"),
        ),
        max_new_hard_regressions=0,
    )
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=regression_debt_policy_pilot_default(),  # max_debt_qids=1
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    assert verdict.under_policy is False
    assert verdict.reason_code == "debt_exceeds_per_iter_max"


def test_evaluate_regression_debt_rejects_when_bucket_disallowed() -> None:
    """A passing-to-hard regression (not soft-to-hard) is outside
    the pilot's allowed buckets and must reject regardless of count.
    """
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
        decide_control_plane_acceptance,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _row("gs_018", "yes", "both_correct"),  # passing baseline
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _row("gs_018", "no", "ground_truth_correct"),  # hard now
        ),
        max_new_hard_regressions=0,
    )
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=regression_debt_policy_pilot_default(),
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    assert verdict.under_policy is False
    assert verdict.reason_code == "debt_bucket_disallowed"


def test_evaluate_regression_debt_rejects_when_aggregate_gain_below_floor() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
        decide_control_plane_acceptance,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=82.0,  # +3.7pp, below 10pp floor
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
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=regression_debt_policy_pilot_default(),
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    assert verdict.under_policy is False
    assert verdict.reason_code == "aggregate_gain_below_floor"


def test_evaluate_regression_debt_rejects_when_cumulative_cap_hit() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    decision = _decision_with_one_soft_to_hard_debt()
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=regression_debt_policy_pilot_default(),
        cumulative_debt=3,  # cumulative_debt_max=3 already hit
        threshold_pass_rate=1.0,
    )
    assert verdict.under_policy is False
    assert verdict.reason_code == "cumulative_debt_cap_hit"


def test_evaluate_regression_debt_rejects_when_threshold_pass_rate_low() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    decision = _decision_with_one_soft_to_hard_debt()
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=regression_debt_policy_pilot_default(),
        cumulative_debt=0,
        threshold_pass_rate=0.5,  # below 0.95
    )
    assert verdict.under_policy is False
    assert verdict.reason_code == "threshold_pass_rate_below_floor"


def test_evaluate_regression_debt_rejects_when_no_target_fixed() -> None:
    """When target_delta_states shows zero FIXED, the partial-harvest
    branch must not fire — the `accepted_with_attribution_drift`
    branch already covers attribution-drift accept; partial-harvest
    requires explicit causal fix.
    """
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
        decide_control_plane_acceptance,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
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
            _row("gs_026", "no", "ground_truth_correct"),  # still hard
            _row("gs_018", "no", "ground_truth_correct"),  # also hard
        ),
        max_new_hard_regressions=0,
    )
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=regression_debt_policy_pilot_default(),
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    assert verdict.under_policy is False
    assert verdict.reason_code == "no_target_clusters_fixed"


def test_evaluate_regression_debt_returns_diagnostics() -> None:
    """policy_diagnostics carries the per-field evaluation outcomes
    so the typed decision record can be deterministic."""
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    decision = _decision_with_one_soft_to_hard_debt()
    verdict = evaluate_regression_debt(
        decision=decision,
        policy=regression_debt_policy_pilot_default(),
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    assert verdict.policy_diagnostics["debt_count"] == 1
    assert verdict.policy_diagnostics["debt_count_max"] == 1
    assert verdict.policy_diagnostics["aggregate_gain_pp"] == 17.4
    assert verdict.policy_diagnostics["target_clusters_fixed"] == 1
    assert verdict.policy_diagnostics["cumulative_debt_used"] == 0
