"""SM1–SM10 are HIGH-tier invariants under contract_health.py."""
from genie_space_optimizer.optimization.contract_health import (
    HIGH_TIER_INVARIANT_IDS,
    SeverityTier,
    classify_invariant_severity,
)


def test_all_sm_ids_in_high_tier():
    for sm in ("SM1", "SM2", "SM3", "SM4", "SM5", "SM6", "SM7", "SM8", "SM9", "SM10"):
        assert sm in HIGH_TIER_INVARIANT_IDS, f"{sm} missing from HIGH_TIER_INVARIANT_IDS"
        assert classify_invariant_severity(sm) == SeverityTier.HIGH
