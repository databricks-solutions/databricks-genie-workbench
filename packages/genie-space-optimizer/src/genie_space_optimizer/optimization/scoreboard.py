"""Operator scoreboard — Track 6 (Phase B pre-work).

Eight metrics that compute a 7-second read of an iteration's state
plus a dominant-signal classification (GATE_OR_CAP_GAP / EVIDENCE_GAP
/ PROPOSAL_GAP / MODEL_CEILING). The metrics are pure functions over
``LoopSnapshot``; the harness builds the snapshot from its in-memory
state at end-of-iteration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Stages whose presence in a qid's journey signals the qid was
# meaningfully addressed by the loop (vs dropped early at clustering
# or grounding).
_TERMINAL_JOURNEY_STAGES: frozenset[str] = frozenset({
    "applied_targeted",
    "applied_broad_ag_scope",
    "applied",
    "accepted",
    "accepted_with_regression_debt",
})


@dataclass(frozen=True)
class LoopSnapshot:
    """Structured view of one loop iteration's state.

    The harness produces one ``LoopSnapshot`` per accepted-or-rolled-
    back candidate. The fields are intentionally narrow — every metric
    function reads only the fields it needs.
    """

    question_ids: list[str]
    hard_cluster_qids: dict[str, str]  # qid -> cluster_id
    journey_events_per_qid: dict[str, list[str]]  # qid -> stage names
    proposed_patches: list[dict[str, Any]]
    applied_patches: list[dict[str, Any]]
    rolled_back_patches: list[dict[str, Any]]
    malformed_proposals_at_cap_count: int
    rollback_records: list[dict[str, Any]]
    terminal_unactionable_qids: set[str]
    baseline_accuracy: float
    candidate_accuracy: float
    trace_id_fallback_recovered: int
    trace_id_fallback_total: int


def journey_completeness_pct(snap: LoopSnapshot) -> float:
    """Fraction of qids that reached a terminal journey stage."""
    qids = snap.question_ids or []
    if not qids:
        return 0.0
    completed = sum(
        1
        for qid in qids
        if any(
            stage in _TERMINAL_JOURNEY_STAGES
            for stage in snap.journey_events_per_qid.get(qid, [])
        )
    )
    return completed / len(qids)


def hard_cluster_coverage_pct(snap: LoopSnapshot) -> float:
    """Fraction of distinct hard clusters that received at least one
    applied patch.
    """
    distinct_clusters = set(snap.hard_cluster_qids.values())
    if not distinct_clusters:
        return 0.0
    covered: set[str] = set()
    for patch in snap.applied_patches or []:
        cid = str(patch.get("cluster_id") or "").strip()
        if cid:
            covered.add(cid)
    return len(covered & distinct_clusters) / len(distinct_clusters)


def causal_patch_survival_pct(snap: LoopSnapshot) -> float:
    """Fraction of proposed patches that landed in the applied set.

    Reads ``proposal_id`` only — applied patches must reference the
    same proposal to count as survived. Split-children inherit the
    parent's proposal_id under Track 1's metadata contract, so a
    rewrite proposal that splits into K children and lands in the
    applied set counts as one survived proposal, not K.
    """
    proposed_ids = {
        str(p.get("proposal_id") or "")
        for p in snap.proposed_patches or []
        if p.get("proposal_id")
    }
    if not proposed_ids:
        return 0.0
    applied_parent_ids: set[str] = set()
    for patch in snap.applied_patches or []:
        # Prefer parent_proposal_id (split-child) then proposal_id.
        pid = str(
            patch.get("parent_proposal_id")
            or patch.get("proposal_id")
            or ""
        ).strip()
        if pid:
            applied_parent_ids.add(pid)
    return len(applied_parent_ids & proposed_ids) / len(proposed_ids)


def malformed_proposals_at_cap(snap: LoopSnapshot) -> int:
    """Count of proposals the cap rejected as malformed."""
    return int(snap.malformed_proposals_at_cap_count)


def rollback_attribution_complete_pct(snap: LoopSnapshot) -> float:
    """Fraction of rollbacks that carry both ``rollback_reason`` and
    ``rollback_class``.
    """
    records = snap.rollback_records or []
    if not records:
        return 1.0  # Vacuous truth: no rollbacks => no missing attribution.
    complete = sum(
        1
        for r in records
        if str(r.get("rollback_reason") or "").strip()
        and str(r.get("rollback_class") or "").strip()
    )
    return complete / len(records)


def terminal_unactionable_qids(snap: LoopSnapshot) -> int:
    """Count of qids the loop labelled unactionable at terminal."""
    return len(snap.terminal_unactionable_qids or set())


def accuracy_delta(snap: LoopSnapshot) -> float:
    """Signed accuracy delta: ``candidate - baseline``."""
    return float(snap.candidate_accuracy) - float(snap.baseline_accuracy)


def trace_id_fallback_rate_metric(snap: LoopSnapshot) -> float:
    """Fraction of rows that required fallback trace recovery."""
    total = int(snap.trace_id_fallback_total)
    if total <= 0:
        return 0.0
    return float(snap.trace_id_fallback_recovered) / float(total)


def compute_scoreboard(snap: LoopSnapshot) -> dict[str, Any]:
    """Compute all eight metrics plus a ``dominant_signal``.

    Dominant signal is the operator's first-read failure bucket:

      * ``PROPOSAL_GAP`` — no proposals at all for hard clusters.
      * ``GATE_OR_CAP_GAP`` — proposals exist but did not survive
        (low ``causal_patch_survival_pct`` or non-zero
        ``malformed_proposals_at_cap``).
      * ``EVIDENCE_GAP`` — patches survived but qids ended up
        terminal-unactionable (judge/RCA evidence ran out).
      * ``MODEL_CEILING`` — patches survived, no terminal-
        unactionable qids, but ``accuracy_delta`` is non-positive.

    The order above is the priority — first match wins.
    """
    metrics = {
        "journey_completeness_pct": journey_completeness_pct(snap),
        "hard_cluster_coverage_pct": hard_cluster_coverage_pct(snap),
        "causal_patch_survival_pct": causal_patch_survival_pct(snap),
        "malformed_proposals_at_cap": malformed_proposals_at_cap(snap),
        "rollback_attribution_complete_pct": rollback_attribution_complete_pct(snap),
        "terminal_unactionable_qids": terminal_unactionable_qids(snap),
        "accuracy_delta": accuracy_delta(snap),
        "trace_id_fallback_rate": trace_id_fallback_rate_metric(snap),
    }

    if not snap.proposed_patches:
        dominant = "PROPOSAL_GAP"
    elif (
        metrics["causal_patch_survival_pct"] < 0.5
        or metrics["malformed_proposals_at_cap"] > 0
    ):
        dominant = "GATE_OR_CAP_GAP"
    elif metrics["terminal_unactionable_qids"] > 0:
        dominant = "EVIDENCE_GAP"
    elif metrics["accuracy_delta"] <= 0.0:
        dominant = "MODEL_CEILING"
    else:
        dominant = "HEALTHY"

    metrics["dominant_signal"] = dominant
    return metrics
