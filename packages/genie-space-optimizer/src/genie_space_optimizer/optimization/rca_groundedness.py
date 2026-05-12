"""Unified RCA-groundedness gate for AGs, proposals, and patches.

Phase C Task 4: today's grounding logic is spread across
``proposal_grounding.explain_causal_relevance``,
``proposal_grounding.proposal_direction_contradicts_counterfactual``,
``rca_execution.next_grounding_remediation``, and several inline
predicates in ``harness.py``. This module is the single canonical
"is this RCA-grounded?" function; callers route AG, proposal, and
patch shapes through one entry point and consume one verdict shape.

The verdict's ``reason_code`` is always one of the existing
:class:`ReasonCode` values (``RCA_GROUNDED``, ``RCA_UNGROUNDED``,
``NO_CAUSAL_TARGET``, ``MISSING_TARGET_QIDS``); no new enum values
are introduced. This keeps the Phase B trace contract stable.

The patch-level path is a thin wrapper over the existing
``proposal_grounding`` predicates so the cap and blast-radius gate
remain authoritative for patches. The new logic is the AG and
proposal paths, which previously had no consolidated emit site.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Literal, Mapping, Sequence

from genie_space_optimizer.optimization.rca_decision_trace import ReasonCode

TargetKind = Literal["ag", "proposal", "patch"]


@dataclass(frozen=True)
class GroundednessVerdict:
    """Result of running an AG/proposal/patch through the gate.

    ``finding_id`` is the matched :class:`RcaFinding.rca_id` when
    accepted; ``""`` when not.
    """

    accepted: bool
    reason_code: ReasonCode
    finding_id: str


def _attr(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _qids_from_target(target: Any, kind: TargetKind) -> tuple[str, ...]:
    if kind == "ag":
        raw = _attr(target, "affected_questions", ())
    else:
        raw = _attr(target, "target_qids", ())
    return tuple(str(q) for q in (raw or ()) if str(q))


def _terms_from_target(target: Any, kind: TargetKind) -> tuple[str, ...]:
    """Surface terms a target could plausibly ground against.

    For an AG, we use the AG's affected-questions plus any
    ``root_cause_summary`` / ``source_cluster_ids`` strings — the AG
    itself rarely carries detailed grounding terms before a proposal
    is built. For a proposal/patch, the grounding terms come from
    ``target``, ``intent``, and the patch DSL fields the existing
    ``rca_execution._patch_grounding_terms`` helper already extracts.
    """
    fields = (
        "target",
        "target_object",
        "target_table",
        "table",
        "column",
        "metric",
        "snippet_name",
        "expression",
        "sql",
        "intent",
        "new_text",
        "description",
        "root_cause_summary",
    )
    terms: list[str] = []
    for field in fields:
        v = _attr(target, field, None)
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            terms.append(v.strip().lower())
        elif isinstance(v, (list, tuple)):
            for child in v:
                if isinstance(child, str) and child.strip():
                    terms.append(child.strip().lower())
    return tuple(dict.fromkeys(terms))


def _finding_terms(finding: Any) -> tuple[str, ...]:
    """A finding's grounding surface — the tokens a proposal must
    overlap with to be considered RCA-grounded."""
    out: list[str] = []
    for field in ("grounding_terms", "blame_set", "counterfactual_fixes",
                  "touched_objects", "rationales"):
        v = _attr(finding, field, ())
        if not v:
            continue
        for tok in v:
            s = str(tok or "").strip().lower()
            if s:
                out.append(s)
    return tuple(dict.fromkeys(out))


def _has_term_overlap(target_terms: Iterable[str], finding_terms: Iterable[str]) -> bool:
    target_set = {t for t in target_terms if t}
    finding_set = {t for t in finding_terms if t}
    if not target_set or not finding_set:
        return False
    if target_set & finding_set:
        return True
    # Substring overlap: a finding term may appear inside a target's
    # ``intent`` string ("Add WHERE active = true...") and vice versa.
    for t in target_set:
        for f in finding_set:
            if t in f or f in t:
                return True
    return False


def is_rca_grounded(
    target: Any,
    findings: Sequence[Any],
    *,
    target_kind: TargetKind,
) -> GroundednessVerdict:
    """Decide whether ``target`` (an AG / proposal / patch) is grounded
    against any of ``findings``.

    Decision tree:

    1. If ``findings`` is empty → ``RCA_UNGROUNDED`` (the loop has
       nothing to ground against).
    2. If ``target`` has no qids → ``MISSING_TARGET_QIDS``.
    3. If no finding's ``target_qids`` overlaps the target's qids →
       ``NO_CAUSAL_TARGET``.
    4. If a finding overlaps qids but no finding's grounding terms
       overlap the target's surface terms → ``RCA_UNGROUNDED``.
       *Exception:* AG shape skips the term-overlap check because AGs
       rarely carry grounding terms before proposals are built; qid
       overlap alone grounds an AG.
    5. Otherwise → ``RCA_GROUNDED`` with the matched finding's id.
    """
    if not findings:
        return GroundednessVerdict(False, ReasonCode.RCA_UNGROUNDED, "")

    target_qids = _qids_from_target(target, target_kind)
    if not target_qids:
        return GroundednessVerdict(False, ReasonCode.MISSING_TARGET_QIDS, "")

    target_qid_set = set(target_qids)
    candidates_with_qid_overlap: list[Any] = []
    for finding in findings:
        f_qids = {str(q) for q in (_attr(finding, "target_qids", ()) or ()) if str(q)}
        if f_qids & target_qid_set:
            candidates_with_qid_overlap.append(finding)

    if not candidates_with_qid_overlap:
        return GroundednessVerdict(False, ReasonCode.NO_CAUSAL_TARGET, "")

    if target_kind == "ag":
        # AGs rarely carry grounding terms; qid overlap alone grounds
        # an AG. Pick the first matching finding deterministically.
        first = candidates_with_qid_overlap[0]
        return GroundednessVerdict(
            True,
            ReasonCode.RCA_GROUNDED,
            str(_attr(first, "rca_id", "") or ""),
        )

    target_terms = _terms_from_target(target, target_kind)
    for finding in candidates_with_qid_overlap:
        if _has_term_overlap(target_terms, _finding_terms(finding)):
            return GroundednessVerdict(
                True,
                ReasonCode.RCA_GROUNDED,
                str(_attr(finding, "rca_id", "") or ""),
            )
    return GroundednessVerdict(False, ReasonCode.RCA_UNGROUNDED, "")


# ---- Plan P-D (2026-05-12) — RCA Ungrounded Recovery Policy ---------


def _evidence_present_for_qids(
    qids: Sequence[str],
    evidence_snapshot: Mapping[str, Any],
) -> bool:
    """Plan P-D — return True iff at least one of the cluster's
    qids has a non-empty entry in either evidence pack
    (``_failure_buckets`` or ``_asi_metadata``).

    The keys mirror what :func:`harness._regenerate_rca_for_cluster`
    pulls out of the metadata snapshot.
    """
    fb = (evidence_snapshot or {}).get("_failure_buckets") or {}
    asi = (evidence_snapshot or {}).get("_asi_metadata") or {}
    for q in qids or ():
        if fb.get(str(q)) or asi.get(str(q)):
            return True
    return False


def classify_rca_ungrounded(
    *,
    cluster: Mapping[str, Any],
    findings: Sequence[Any],
    evidence_snapshot: Mapping[str, Any],
) -> "RcaUngroundedReason":
    """Plan P-D (2026-05-12) — classify *why* a cluster's
    ``rca_card`` is falsy at AG-emit.

    Decision tree (priority order matters — first match wins):

    1. ``not cluster.get("rca_id")`` → ``NO_PARENT_RCA``
       (preserves the existing T3 trigger label)
    2. cluster has no qids → ``MISSING_TARGET_QIDS``
       (contract bug — escalate, not retry)
    3. neither evidence pack has any entry for any cluster qid →
       ``NO_EVIDENCE_AVAILABLE`` (driver would no-op)
    4. ``findings`` empty → ``NO_FINDINGS``
       (broader evidence pack may help)
    5. some finding overlaps qids but no grounding-term overlap →
       ``NO_TERM_OVERLAP``
    6. no finding overlaps qids → ``NO_CAUSAL_TARGET``
    7. else → ``UNKNOWN`` (defensive)

    The classifier is pure: no Spark, Databricks, MLflow, LLM, or
    flag reads. Tests inject the cluster + findings + evidence
    snapshot directly. Production calls supply the snapshot via the
    Stage 2 metadata accumulator already present in
    ``_run_lever_loop``.
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        RcaUngroundedReason,
    )

    if not cluster:
        return RcaUngroundedReason.UNKNOWN

    # If the cluster is already grounded the caller should not be
    # calling the classifier; defensively return UNKNOWN rather than
    # mis-classify a grounded cluster as ungrounded.
    if bool(cluster.get("rca_card")):
        return RcaUngroundedReason.UNKNOWN

    if not str(cluster.get("rca_id") or ""):
        return RcaUngroundedReason.NO_PARENT_RCA

    qids = tuple(
        str(q) for q in (cluster.get("question_ids") or []) if q
    )
    if not qids:
        return RcaUngroundedReason.MISSING_TARGET_QIDS

    if not _evidence_present_for_qids(qids, evidence_snapshot or {}):
        return RcaUngroundedReason.NO_EVIDENCE_AVAILABLE

    if not findings:
        return RcaUngroundedReason.NO_FINDINGS

    qid_set = set(qids)
    candidates = [
        f for f in findings
        if {str(q) for q in (_attr(f, "target_qids", ()) or ())}
        & qid_set
    ]
    if not candidates:
        return RcaUngroundedReason.NO_CAUSAL_TARGET

    target_terms_for_cluster = tuple(
        s.lower()
        for s in (
            str(cluster.get("root_cause") or ""),
            *(str(q) for q in qids),
        )
        if s
    )
    for finding in candidates:
        if _has_term_overlap(target_terms_for_cluster, _finding_terms(finding)):
            # Cluster IS grounded — caller should not be here. Fall
            # through to UNKNOWN so the orchestrator skips regen.
            return RcaUngroundedReason.UNKNOWN
    return RcaUngroundedReason.NO_TERM_OVERLAP
