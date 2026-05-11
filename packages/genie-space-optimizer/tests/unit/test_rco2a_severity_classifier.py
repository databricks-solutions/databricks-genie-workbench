"""RCO-2a Task 2 — severity-tier classifier tests."""
from __future__ import annotations

import pytest


def test_high_tier_set_is_exactly_i9_through_i13() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        HIGH_TIER_INVARIANT_IDS,
    )
    assert HIGH_TIER_INVARIANT_IDS == frozenset({"I9", "I10", "I11", "I12", "I13"})


@pytest.mark.parametrize("inv_id", ["I9", "I10", "I11", "I12", "I13"])
def test_classify_returns_high_for_high_tier_ids(inv_id: str) -> None:
    from genie_space_optimizer.optimization.contract_health import (
        SeverityTier,
        classify_invariant_severity,
    )
    assert classify_invariant_severity(inv_id) is SeverityTier.HIGH


@pytest.mark.parametrize("inv_id", ["I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8"])
def test_classify_returns_medium_for_medium_tier_ids(inv_id: str) -> None:
    from genie_space_optimizer.optimization.contract_health import (
        SeverityTier,
        classify_invariant_severity,
    )
    assert classify_invariant_severity(inv_id) is SeverityTier.MEDIUM


def test_classify_returns_medium_for_check_failed_sentinel() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        SeverityTier,
        classify_invariant_severity,
    )
    assert classify_invariant_severity("I_CHECK_FAILED") is SeverityTier.MEDIUM


def test_classify_returns_medium_for_unknown_ids() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        SeverityTier,
        classify_invariant_severity,
    )
    assert classify_invariant_severity("I999") is SeverityTier.MEDIUM
    assert classify_invariant_severity("") is SeverityTier.MEDIUM
