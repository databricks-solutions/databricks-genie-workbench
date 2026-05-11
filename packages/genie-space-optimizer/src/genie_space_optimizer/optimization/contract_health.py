"""RCO-2a — Contract Health summary and merge-gate keystone.

Pure module. No I/O, no Spark, no MLflow. Consumes evidence already
produced by upstream stages (invariant violations, Phase H strict
validation marker payload, bundle-completeness markers, replay
validity) and produces a typed ``ContractHealthSummary`` whose
``merge_gate_status`` is the canonical RCO-2 merge-gate result.

The strict-mode default flip is deferred to RCO-2b (see
``docs/2026-05-12-rco-2b-deferral.md``). RCO-2a wires the categories;
the production exit path still defaults to warn-and-degrade.
"""

from __future__ import annotations

import enum


HIGH_TIER_INVARIANT_IDS: frozenset[str] = frozenset(
    {"I9", "I10", "I11", "I12", "I13"}
)


class SeverityTier(enum.Enum):
    HIGH = "high"
    MEDIUM = "medium"


def classify_invariant_severity(invariant_id: str) -> SeverityTier:
    """Map a canonical invariant ID to its severity tier.

    Unknown IDs and the ``I_CHECK_FAILED`` sentinel both classify as
    MEDIUM. The HIGH tier is intentionally narrow (exactly I9–I13) so
    new invariants do not silently inflate the merge-gate surface.
    """
    if invariant_id in HIGH_TIER_INVARIANT_IDS:
        return SeverityTier.HIGH
    return SeverityTier.MEDIUM
