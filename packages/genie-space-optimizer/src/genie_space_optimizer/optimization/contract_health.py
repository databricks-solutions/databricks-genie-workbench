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


def _classify_phase_h(payload: Mapping[str, Any] | None) -> tuple[str, str]:
    """Extract (listing_status, validator_status) from the Phase H marker
    payload. ``None`` (marker not emitted) means both are ``skipped``."""
    if not payload:
        return ("skipped", "skipped")
    listing = str(payload.get("listing_status") or "skipped")
    validator = str(payload.get("validator_status") or "skipped")
    return (listing, validator)


def _classify_bundle(
    failed: Sequence[Mapping[str, Any]],
    incomplete: Sequence[Mapping[str, Any]] | None,
) -> str:
    """Reduce bundle-completeness markers to a single status string."""
    if failed:
        return "assembly_failed"
    if incomplete:
        return "incomplete"
    return "complete"


def build_contract_health_summary(
    *,
    optimization_run_id: str,
    invariant_violations: Sequence[Mapping[str, Any]],
    phase_h_strict_validation: Mapping[str, Any] | None,
    bundle_assembly_failed: Sequence[Mapping[str, Any]],
    bundle_assembly_incomplete: Sequence[Mapping[str, Any]] | None,
    replay_validation: Mapping[str, Any] | None,
) -> ContractHealthSummary:
    """Aggregate evidence into a typed contract-health summary.

    The ``merge_gate_status`` is computed deterministically:
      - any HIGH-tier invariant violation        → MERGE_GATE_BLOCKED
      - Phase H ``listing_status`` == ``failed`` → MERGE_GATE_BLOCKED
      - Phase H ``validator_status`` == ``failed``→ MERGE_GATE_BLOCKED
      - bundle_assembly_failed non-empty         → MERGE_GATE_BLOCKED
      - otherwise, if MEDIUM violations OR Phase H skipped OR
        bundle incomplete OR replay invalid       → WARN
      - else                                     → HEALTHY

    No side effects. Safe to call from anywhere.
    """
    high: list[Mapping[str, Any]] = []
    medium: list[Mapping[str, Any]] = []
    for v in invariant_violations or ():
        inv_id = str((v or {}).get("invariant_id") or "")
        if classify_invariant_severity(inv_id) is SeverityTier.HIGH:
            high.append(dict(v))
        else:
            medium.append(dict(v))

    listing_status, validator_status = _classify_phase_h(phase_h_strict_validation)
    bundle_status = _classify_bundle(
        bundle_assembly_failed or (),
        bundle_assembly_incomplete,
    )

    rv = dict(replay_validation or {})
    replay_is_valid = bool(rv.get("is_valid", True)) if rv else True
    replay_violation_count = int(rv.get("violation_count") or 0)

    blocking = (
        bool(high)
        or listing_status == "failed"
        or validator_status == "failed"
        or bundle_status == "assembly_failed"
    )
    warning = (
        bool(medium)
        or listing_status == "skipped"
        or validator_status == "skipped"
        or bundle_status == "incomplete"
        or (bool(rv) and not replay_is_valid)
    )

    if blocking:
        status = MergeGateStatus.MERGE_GATE_BLOCKED
    elif warning:
        status = MergeGateStatus.WARN
    else:
        status = MergeGateStatus.HEALTHY

    return ContractHealthSummary(
        optimization_run_id=str(optimization_run_id),
        merge_gate_status=status,
        high_tier_violations=tuple(high),
        medium_tier_violations=tuple(medium),
        phase_h_listing_status=listing_status,
        phase_h_validator_status=validator_status,
        bundle_status=bundle_status,
        replay_is_valid=replay_is_valid,
        replay_violation_count=replay_violation_count,
    )


class MergeGateBlockedError(Exception):
    """RCO-2b — raised by ``enforce_merge_gate`` when the contract-health
    summary reports ``merge_gate_blocked``.

    Carries the structured fields a postmortem analyzer cares about
    (status, HIGH-tier violation count, optimization run id) so the
    surfaced error message in Databricks job-run logs is self-describing
    without needing to re-parse stdout.
    """

    def __init__(
        self,
        *,
        merge_gate_status: str,
        high_tier_violation_count: int,
        optimization_run_id: str,
    ) -> None:
        self.merge_gate_status = str(merge_gate_status)
        self.high_tier_violation_count = int(high_tier_violation_count)
        self.optimization_run_id = str(optimization_run_id)
        super().__init__(
            f"merge_gate_status={self.merge_gate_status} "
            f"high_tier_violations={self.high_tier_violation_count} "
            f"optimization_run_id={self.optimization_run_id}"
        )
