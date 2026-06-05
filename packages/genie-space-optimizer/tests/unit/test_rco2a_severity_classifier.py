"""RCO-2a Task 2 — severity-tier classifier tests."""
from __future__ import annotations

import pytest


def test_high_tier_set_pins_the_authoritative_surface() -> None:
    # Pins the HIGH-tier merge-gate surface so new invariants cannot
    # silently inflate it. The set grew past the original I9-I13 base:
    # Plan 10 Phase B added I15-I25 (the silent-leakage family) and the
    # v3 design-doc section 9 added the SM1-SM10 state-machine
    # invariants. I14 is intentionally NOT high-tier.
    from genie_space_optimizer.optimization.contract_health import (
        HIGH_TIER_INVARIANT_IDS,
    )
    assert HIGH_TIER_INVARIANT_IDS == frozenset(
        {
            "I9", "I10", "I11", "I12", "I13", "I15", "I16", "I17", "I18",
            "I19", "I20", "I21", "I22", "I23", "I24", "I25",
            "SM1", "SM2", "SM3", "SM4", "SM5", "SM6", "SM7", "SM8", "SM9",
            "SM10",
        }
    )
    assert "I14" not in HIGH_TIER_INVARIANT_IDS


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
