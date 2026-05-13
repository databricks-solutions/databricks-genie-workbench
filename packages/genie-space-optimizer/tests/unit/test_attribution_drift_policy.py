"""Phase 1 Task 2 — attribution_drift_policy_pilot_default.

The new factory returns a RegressionDebtPolicy parameterised for the
attribution-drift acceptance tier (target unfixed + bounded debt +
lower aggregate floor + broader bucket admission). Distinct from
the existing pilot which gates on target_fixed >= 1.

Mirrors the test pattern in test_regression_debt_policy.py.
"""

from __future__ import annotations

import pytest


def test_attribution_drift_policy_pilot_default_field_values() -> None:
    """The new factory returns the design-record's exact values."""
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
        attribution_drift_policy_pilot_default,
    )
    from genie_space_optimizer.optimization.control_plane import DeltaState

    policy = attribution_drift_policy_pilot_default()
    assert isinstance(policy, RegressionDebtPolicy)
    assert policy.max_debt_qids == 1
    assert policy.allowed_debt_buckets == frozenset(
        {DeltaState.SOFT_TO_HARD, DeltaState.LOOKUP_FAILED}
    )
    assert policy.min_aggregate_improvement_pp == 4.0
    assert policy.min_target_clusters_fixed == 0
    assert policy.min_threshold_pass_rate == 0.95
    assert policy.cumulative_debt_max == 3


def test_attribution_drift_policy_pilot_default_is_distinct_from_partial_harvest() -> None:
    """The two pilot policies are genuinely different objects with
    different field values. Catches accidental copy-paste."""
    from genie_space_optimizer.optimization.acceptance_policy import (
        attribution_drift_policy_pilot_default,
        regression_debt_policy_pilot_default,
    )

    drift = attribution_drift_policy_pilot_default()
    harvest = regression_debt_policy_pilot_default()

    # Same dataclass, different parameterisation.
    assert type(drift) is type(harvest)
    assert drift != harvest

    # The defining distinctions:
    assert drift.min_target_clusters_fixed == 0
    assert harvest.min_target_clusters_fixed == 1
    assert drift.min_aggregate_improvement_pp == 4.0
    assert harvest.min_aggregate_improvement_pp == 10.0
    assert len(drift.allowed_debt_buckets) == 2
    assert len(harvest.allowed_debt_buckets) == 1


def test_attribution_drift_policy_from_config_default_off_returns_hard_zero() -> None:
    """When GSO_ATTRIBUTION_DRIFT_WITH_DEBT is unset (default-OFF),
    the from_config helper returns the hard-zero policy — the path
    is unreachable, matching the partial-harvest convention."""
    import os

    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
        attribution_drift_policy_from_config,
    )

    os.environ.pop("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", None)
    policy = attribution_drift_policy_from_config()
    assert policy == RegressionDebtPolicy()  # hard-zero default


def test_attribution_drift_policy_from_config_flag_on_returns_pilot(monkeypatch) -> None:
    """When GSO_ATTRIBUTION_DRIFT_WITH_DEBT=1, the from_config helper
    returns the pilot default."""
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", "1")

    from genie_space_optimizer.optimization.acceptance_policy import (
        attribution_drift_policy_from_config,
        attribution_drift_policy_pilot_default,
    )

    assert attribution_drift_policy_from_config() == (
        attribution_drift_policy_pilot_default()
    )


def test_attribution_drift_policy_validation_rejects_negative_floor() -> None:
    """The shared RegressionDebtPolicy validation rejects invalid
    constructions even on the new factory body."""
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
    )
    from genie_space_optimizer.optimization.control_plane import DeltaState

    with pytest.raises(ValueError, match="min_aggregate_improvement_pp"):
        RegressionDebtPolicy(
            max_debt_qids=1,
            allowed_debt_buckets=frozenset({DeltaState.SOFT_TO_HARD}),
            min_aggregate_improvement_pp=-1.0,
            min_target_clusters_fixed=0,
            min_threshold_pass_rate=0.95,
            cumulative_debt_max=3,
        )


def test_attribution_drift_with_debt_flag_defaults_off(monkeypatch) -> None:
    """The new flag is default-OFF. Phase 0.2 must validate offline
    before this flips. Mirrors the existing GSO_PARTIAL_HARVEST_WITH_DEBT
    default-OFF convention (config.py:5361)."""
    monkeypatch.delenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", raising=False)

    from genie_space_optimizer.common.config import (
        attribution_drift_with_debt_enabled,
    )

    assert attribution_drift_with_debt_enabled() is False


def test_attribution_drift_with_debt_flag_truthy_env_enables(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", "1")

    from genie_space_optimizer.common.config import (
        attribution_drift_with_debt_enabled,
    )

    assert attribution_drift_with_debt_enabled() is True
