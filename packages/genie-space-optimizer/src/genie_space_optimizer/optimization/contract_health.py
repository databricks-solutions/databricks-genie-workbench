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
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


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


class MergeGateStatus(enum.Enum):
    """RCO-2a merge-gate result category.

    The category is computed deterministically from invariant
    violations + Phase H validation status + bundle completeness +
    replay validity. RCO-2a wires the category into stdout (via
    ``GSO_CONTRACT_HEALTH_V1``) and into the operator transcript, but
    the production job exit code is NOT yet driven by it. RCO-2b will
    flip ``MERGE_GATE_BLOCKED`` to a non-zero task exit.
    """

    HEALTHY = "healthy"
    WARN = "warn"
    MERGE_GATE_BLOCKED = "merge_gate_blocked"


@dataclass(frozen=True)
class ContractHealthSummary:
    """Pure end-of-run contract-health summary (RCO-2a keystone).

    Built by ``build_contract_health_summary`` from evidence already
    emitted by upstream stages. Serialized into stdout via
    ``contract_health_summary_marker`` and rendered into the operator
    process transcript's ``Contract Health`` stage section.
    """

    optimization_run_id: str
    merge_gate_status: MergeGateStatus
    high_tier_violations: tuple[Mapping[str, Any], ...]
    medium_tier_violations: tuple[Mapping[str, Any], ...]
    phase_h_listing_status: str
    phase_h_validator_status: str
    bundle_status: str
    replay_is_valid: bool
    replay_violation_count: int

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "optimization_run_id": str(self.optimization_run_id),
            "merge_gate_status": self.merge_gate_status.value,
            "high_tier_violations": [
                dict(v) for v in self.high_tier_violations
            ],
            "medium_tier_violations": [
                dict(v) for v in self.medium_tier_violations
            ],
            "phase_h_listing_status": str(self.phase_h_listing_status),
            "phase_h_validator_status": str(self.phase_h_validator_status),
            "bundle_status": str(self.bundle_status),
            "replay_is_valid": bool(self.replay_is_valid),
            "replay_violation_count": int(self.replay_violation_count),
        }

    @classmethod
    def from_json_dict(cls, blob: Mapping[str, Any]) -> "ContractHealthSummary":
        return cls(
            optimization_run_id=str(blob.get("optimization_run_id") or ""),
            merge_gate_status=MergeGateStatus(
                str(blob.get("merge_gate_status") or "warn")
            ),
            high_tier_violations=tuple(
                dict(v) for v in (blob.get("high_tier_violations") or [])
            ),
            medium_tier_violations=tuple(
                dict(v) for v in (blob.get("medium_tier_violations") or [])
            ),
            phase_h_listing_status=str(blob.get("phase_h_listing_status") or ""),
            phase_h_validator_status=str(blob.get("phase_h_validator_status") or ""),
            bundle_status=str(blob.get("bundle_status") or ""),
            replay_is_valid=bool(blob.get("replay_is_valid")),
            replay_violation_count=int(blob.get("replay_violation_count") or 0),
        )
