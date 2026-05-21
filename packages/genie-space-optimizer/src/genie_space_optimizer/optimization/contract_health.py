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
    {"I9", "I10", "I11", "I12", "I13", "I15", "I16",
     "I17", "I18", "I19", "I20", "I21"}
)
# Plan 10 Phase B (2026-05-19) — I15 + I16 are HIGH-tier because both
# fire on the exact silent-leakage pattern the plan exists to prevent
# (LLM dispatch entered but exited silently / fall-through to legacy
# archetype path despite Plan 9 activation). Adding them to the HIGH
# tier ensures the merge-gate summary surfaces these violations
# loudly even though RCO-2b's gate-blocking flip is still deferred.
#
# Plan 11 (2026-05-20) — I17/I18 enforce per-QID Stage 1 and per-cluster
# Stage 3 coverage so missing markers fail the merge gate loudly instead
# of silently shrinking the synthesis surface. I19/I20 cap the LLM
# repair/narrow exhaustion rate at 20%, matching the I15/I16 framing of
# "silent leakage of the new dispatch path."


class SeverityTier(enum.Enum):
    HIGH = "high"
    MEDIUM = "medium"


def classify_invariant_severity(invariant_id: str) -> SeverityTier:
    """Map a canonical invariant ID to its severity tier.

    Unknown IDs and the ``I_CHECK_FAILED`` sentinel both classify as
    MEDIUM. The HIGH tier is intentionally narrow (I9–I13 + Plan 10
    Phase B I15–I16) so new invariants do not silently inflate the
    merge-gate surface.
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
    # P-E2 — observe-only leakage counts. Always carries both
    # known call_site keys (zero when no records). Tuple-of-pairs
    # rather than dict for hashable / frozen-dataclass compat.
    proposal_stage_forbidden_ag_observed_count_by_call_site: tuple[
        tuple[str, int], ...
    ] = (
        ("cluster_driven_synthesis", 0),
        ("force_lever6", 0),
    )

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
            "proposal_stage_forbidden_ag_observed_count_by_call_site": [
                [str(k), int(v)]
                for k, v in
                self.proposal_stage_forbidden_ag_observed_count_by_call_site
            ],
        }

    @classmethod
    def from_json_dict(cls, blob: Mapping[str, Any]) -> "ContractHealthSummary":
        raw_counts = blob.get(
            "proposal_stage_forbidden_ag_observed_count_by_call_site"
        ) or [
            ["cluster_driven_synthesis", 0],
            ["force_lever6", 0],
        ]
        counts = tuple(
            (str(item[0]), int(item[1])) for item in raw_counts
        )
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
            proposal_stage_forbidden_ag_observed_count_by_call_site=counts,
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


_PROPOSAL_STAGE_OBSERVED_CALL_SITES: tuple[str, ...] = (
    "cluster_driven_synthesis",
    "force_lever6",
)


def build_contract_health_summary(
    *,
    optimization_run_id: str,
    invariant_violations: Sequence[Mapping[str, Any]],
    phase_h_strict_validation: Mapping[str, Any] | None,
    bundle_assembly_failed: Sequence[Mapping[str, Any]],
    bundle_assembly_incomplete: Sequence[Mapping[str, Any]] | None,
    replay_validation: Mapping[str, Any] | None,
    proposal_stage_forbidden_ag_observed_records: Sequence[
        Mapping[str, Any]
    ] = (),
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

    # P-E2 — aggregate observe-only leakage counts by call_site.
    # Always seed both known keys at 0 so the merge-gate marker
    # shape is byte-stable across runs (no missing keys).
    leakage_counts: dict[str, int] = {
        site: 0 for site in _PROPOSAL_STAGE_OBSERVED_CALL_SITES
    }
    for rec in proposal_stage_forbidden_ag_observed_records or ():
        if str((rec or {}).get("reason_code") or "") != (
            "proposal_stage_forbidden_ag_observed"
        ):
            continue
        site = str(((rec or {}).get("metrics") or {}).get("call_site") or "")
        if site in leakage_counts:
            leakage_counts[site] += 1
        # Unknown call_site → silently skipped (defense in depth).
    leakage_tuple = tuple(
        (site, leakage_counts[site])
        for site in _PROPOSAL_STAGE_OBSERVED_CALL_SITES
    )

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
        proposal_stage_forbidden_ag_observed_count_by_call_site=leakage_tuple,
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


def enforce_merge_gate(loop_out: Mapping[str, Any]) -> None:
    """RCO-2b — raise ``MergeGateBlockedError`` iff the lever-loop's
    contract-health summary reports ``merge_gate_blocked``.

    Called by ``jobs/run_lever_loop.py`` between task-values publishing
    and ``dbutils.notebook.exit(...)``. Task values are published first
    so postmortem tooling can read the failing run's debug payload;
    the raise marks the Databricks task as failed so downstream
    ``finalize`` / ``deploy`` tasks skip.

    Missing / ``None`` / empty ``contract_health_summary`` is a no-op:
    RCO-2a's emit path is fail-soft (swallows all exceptions). RCO-2b
    only enforces on a known-blocked payload, never on absence — a
    silently-skipped emit must not block the run.
    """
    payload = (loop_out or {}).get("contract_health_summary")
    if not payload:
        return
    if not isinstance(payload, Mapping):
        return
    status = str(payload.get("merge_gate_status") or "")
    if status != MergeGateStatus.MERGE_GATE_BLOCKED.value:
        return
    high_tier = payload.get("high_tier_violations") or ()
    raise MergeGateBlockedError(
        merge_gate_status=status,
        high_tier_violation_count=len(list(high_tier)),
        optimization_run_id=str(payload.get("optimization_run_id") or ""),
    )
