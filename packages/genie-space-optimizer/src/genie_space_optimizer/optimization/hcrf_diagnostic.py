"""P4 C6 — OBSERVE-FIRST diagnostic marker for HCRF rejections and
hypothetical Branch C candidates.

This module is purely observational. It does NOT change any
blast-radius gate behavior, does NOT flip any flag default, and
does NOT downgrade any rejection reason. It emits a single,
deterministic ``GSO_HCRF_DIAGNOSTIC_V1`` marker so that one replay
over d139 + e943 can confirm whether the downgrade rule and the
``GSO_L6_NARROW_REPLACEMENT_BRANCH_C`` default flip are safe to
apply in a follow-up PR.

Marker shape (sorted JSON payload):
    GSO_HCRF_DIAGNOSTIC_V1 {
      "adjacent_qid_regression_evidence_count": int,
      "hypothetical_branch_c_candidate_count": int,
      "intent_id": str,
      "outside_target_qids": list[str],
      "outside_target_qids_count": int,
      "outside_target_qids_currently_hard_count": int,
      "patch_type": str,
      "why_fired": str,
      "would_have_stamped": bool
    }

Field semantics (pinned by P4 plan):
  * ``why_fired`` — the verdict reason string from the blast-radius
    helper (e.g. ``high_collateral_risk_flagged``,
    ``blast_radius_exceeds_threshold``, ``passing_dependents_missing``).
  * ``outside_target_qids_currently_hard_count`` — number of
    collateral qids that are themselves in the run's live_hard set;
    when this equals the total outside count, the
    ``shared_cause_collateral_warning`` downgrade *would* be eligible
    if the flag were flipped.
  * ``adjacent_qid_regression_evidence_count`` — number of collateral
    qids for which the run carries regression evidence (i.e. the qid
    has both pre- and post-apply scores recorded). Approximated as 0
    in the OBSERVE-FIRST PR; producers can plumb a richer count
    through ``TransformerContext`` in a follow-up.
  * ``hypothetical_branch_c_candidate_count`` — number of Branch C
    narrow-replacement candidates that *would* be minted if the flag
    were on. Approximated as 0 in OBSERVE-FIRST; producers can
    invoke ``build_narrow_l6_replacement`` in dry-run mode in a
    follow-up.
  * ``would_have_stamped`` — True when the downgrade rule (today
    gated behind ``shared_cause_blast_radius_enabled`` /
    ``lever_aware_blast_radius_enabled``) WOULD have stamped a
    safe-with-warning verdict had the flag been on. Computed
    deterministically from the verdict reason and the
    outside-hard overlap.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable, Mapping


HCRF_DIAGNOSTIC_MARKER_PREFIX: str = "GSO_HCRF_DIAGNOSTIC_V1"

# Verdict reasons that the C6 OBSERVE-FIRST diagnostic considers
# "HCRF-eligible" — i.e. the marker is emitted whenever the gate
# fires for one of these reasons. Other safe verdicts are silent.
_HCRF_ELIGIBLE_REASONS: frozenset[str] = frozenset({
    "high_collateral_risk_flagged",
    "blast_radius_exceeds_threshold",
    "passing_dependents_missing",
    "shared_cause_collateral_warning",
    "non_semantic_collateral_warning",
})


@dataclass(frozen=True, slots=True)
class HcrfDiagnostic:
    """Pinned diagnostic payload for one HCRF gate firing."""

    patch_type: str
    intent_id: str
    why_fired: str
    outside_target_qids: tuple[str, ...]
    outside_target_qids_currently_hard_count: int
    adjacent_qid_regression_evidence_count: int
    hypothetical_branch_c_candidate_count: int
    would_have_stamped: bool

    def to_jsonable(self) -> dict:
        return {
            "adjacent_qid_regression_evidence_count": int(
                self.adjacent_qid_regression_evidence_count
            ),
            "hypothetical_branch_c_candidate_count": int(
                self.hypothetical_branch_c_candidate_count
            ),
            "intent_id": str(self.intent_id),
            "outside_target_qids": list(self.outside_target_qids),
            "outside_target_qids_count": len(self.outside_target_qids),
            "outside_target_qids_currently_hard_count": int(
                self.outside_target_qids_currently_hard_count
            ),
            "patch_type": str(self.patch_type),
            "why_fired": str(self.why_fired),
            "would_have_stamped": bool(self.would_have_stamped),
        }


def is_hcrf_eligible_reason(reason: str) -> bool:
    """Pure predicate: is this verdict reason in the HCRF
    diagnostic set?"""
    return str(reason or "") in _HCRF_ELIGIBLE_REASONS


def compute_would_have_stamped(
    *,
    why_fired: str,
    outside_target_qids: Iterable[str],
    live_hard_qids: Iterable[str],
) -> bool:
    """OBSERVE-FIRST: would the existing downgrade rules have stamped
    a safe-with-warning verdict for this HCRF firing if the flags
    were flipped on?

    The two downgrade paths today (both flag-gated) are:
      * ``shared_cause_collateral_warning`` — fires when ALL
        outside_target qids are in ``live_hard_qids``.
      * ``non_semantic_collateral_warning`` — fires when the patch
        type is non-semantic; not detectable here without the patch
        body, so callers compute it externally.

    This helper returns True for the shared-cause path only; the
    non-semantic path is computed at the call site (``compute_for_*``).
    """
    if why_fired != "high_collateral_risk_flagged":
        return False
    outside = tuple(str(q) for q in outside_target_qids if str(q))
    hard = frozenset(str(q) for q in live_hard_qids if str(q))
    return bool(outside) and all(q in hard for q in outside)


def compute_hcrf_diagnostic(
    *,
    patch_type: str,
    intent_id: str,
    why_fired: str,
    outside_target_qids: Iterable[str],
    live_hard_qids: Iterable[str],
    adjacent_qid_regression_evidence_count: int = 0,
    hypothetical_branch_c_candidate_count: int = 0,
    non_semantic_downgrade_eligible: bool = False,
) -> HcrfDiagnostic:
    """Build a pinned :class:`HcrfDiagnostic` from the gate verdict
    and run context. Pure: no I/O.

    ``adjacent_qid_regression_evidence_count`` and
    ``hypothetical_branch_c_candidate_count`` are passed through;
    callers that don't have the data yet leave them at 0 (OBSERVE-
    FIRST default).
    """
    outside_t = tuple(sorted({str(q) for q in outside_target_qids if str(q)}))
    hard = frozenset(str(q) for q in live_hard_qids if str(q))
    hard_count = sum(1 for q in outside_t if q in hard)

    would_stamp_shared_cause = compute_would_have_stamped(
        why_fired=why_fired,
        outside_target_qids=outside_t,
        live_hard_qids=hard,
    )
    return HcrfDiagnostic(
        patch_type=str(patch_type or ""),
        intent_id=str(intent_id or ""),
        why_fired=str(why_fired or ""),
        outside_target_qids=outside_t,
        outside_target_qids_currently_hard_count=hard_count,
        adjacent_qid_regression_evidence_count=int(
            adjacent_qid_regression_evidence_count or 0
        ),
        hypothetical_branch_c_candidate_count=int(
            hypothetical_branch_c_candidate_count or 0
        ),
        would_have_stamped=bool(
            would_stamp_shared_cause or non_semantic_downgrade_eligible
        ),
    )


def hcrf_diagnostic_marker(diag: HcrfDiagnostic) -> str:
    """Render a single-line ``GSO_HCRF_DIAGNOSTIC_V1`` marker.

    Pure: no I/O. Caller is responsible for ``print()`` with flush
    so the marker survives Databricks stdout buffering.
    """
    return (
        f"{HCRF_DIAGNOSTIC_MARKER_PREFIX} "
        f"{json.dumps(diag.to_jsonable(), sort_keys=True)}"
    )


def hcrf_diagnostic_marker_from_verdict(
    *,
    verdict: Mapping[str, object],
    patch_type: str,
    intent_id: str,
    live_hard_qids: Iterable[str],
    adjacent_qid_regression_evidence_count: int = 0,
    hypothetical_branch_c_candidate_count: int = 0,
    non_semantic_downgrade_eligible: bool = False,
) -> str | None:
    """Convenience: take a ``patch_blast_radius_is_safe`` verdict dict
    and produce the marker line, or ``None`` if the verdict reason is
    not HCRF-eligible (no marker emission).

    Pure: no I/O.
    """
    reason = str(verdict.get("reason") or "")
    if not is_hcrf_eligible_reason(reason):
        return None
    outside = tuple(
        str(q) for q in (
            verdict.get("passing_dependents_outside_target") or ()
        )
    )
    diag = compute_hcrf_diagnostic(
        patch_type=patch_type,
        intent_id=intent_id,
        why_fired=reason,
        outside_target_qids=outside,
        live_hard_qids=live_hard_qids,
        adjacent_qid_regression_evidence_count=adjacent_qid_regression_evidence_count,
        hypothetical_branch_c_candidate_count=hypothetical_branch_c_candidate_count,
        non_semantic_downgrade_eligible=non_semantic_downgrade_eligible,
    )
    return hcrf_diagnostic_marker(diag)
