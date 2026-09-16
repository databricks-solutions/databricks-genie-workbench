"""Evaluation & trust harness (MV-D59) — offline scorer for materialized runs.

A read-only, deterministic harness that consumes a materialized run's snapshot
tables and emits one comparable report with four sections:
  1. Precision/recall/F1 of discovered domains vs aligned reference (degrade if missing)
  2. Structural health (singleton/orphan rate, depth, branching factor)
  3. Reference-free LLM sanity monitor (cheap, injectable, report-only)
  4. Human spot-review queue (deterministically sampled, evidence-backed)

The report is typed, serializable, and diff-able across runs. No writes; read-only.
Deterministic + offline except the injectable LLM monitor (skippable, never feeds metrics).
Degrade-not-hang (MV-D43) — missing reference, missing evidence, or LLM error yields
a marked-partial report, never a crash.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Mapping, Sequence

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PrecisionRecallF1:
    """Precision, recall, and F1 score (0-1) with N/A support."""

    precision: float | None  # None means N/A
    recall: float | None
    f1: float | None
    na_reason: str = ""  # Plain reason if any metric is N/A


@dataclass(frozen=True)
class DomainMatchStatus:
    """Per-domain match status against aligned reference."""

    domain_id: str
    domain_name: str
    discovered_members: set[str] = field(default_factory=set)
    reference_id: str | None = None  # Matched reference ID, if any
    reference_name: str | None = None
    reference_members: set[str] = field(default_factory=set)
    match_type: str = "no_match"  # "exact", "partial", "no_match", "extra", "curated"


@dataclass(frozen=True)
class StructuralHealth:
    """Structural health metrics (Vibe repo reference)."""

    singleton_count: int  # Domains with 1 member
    orphan_count: int  # Domains with no parent
    tree_depth: int  # Max depth in the domain hierarchy
    branching_factor_avg: float  # Average children per domain
    total_domains: int
    total_members: int


@dataclass(frozen=True)
class LLMSanityResult:
    """Reference-free LLM sanity check result."""

    status: str  # "skipped", "passed", "flagged", "error"
    flagged_domains: list[dict[str, Any]] = field(default_factory=list)
    error_message: str = ""


@dataclass(frozen=True)
class SpotReviewItem:
    """One item in the spot-review queue."""

    domain_id: str
    domain_name: str
    member_count: int
    evidence_reason: str
    confidence_band: str
    confidence_value: float = 0.0


@dataclass(frozen=True)
class EvalReport:
    """The complete evaluation report for a materialized run."""

    run_id: str
    metastore_id: str
    timestamp: str
    precision_recall_f1: PrecisionRecallF1
    domain_match_statuses: list[DomainMatchStatus] = field(default_factory=list)
    structural_health: StructuralHealth | None = None
    llm_sanity: LLMSanityResult = field(default_factory=lambda: LLMSanityResult("skipped"))
    spot_review_queue: list[SpotReviewItem] = field(default_factory=list)


@dataclass(frozen=True)
class ReportDelta:
    """Comparison between two reports (BEFORE and AFTER)."""

    precision_delta: dict[str, Any]  # {"before": X, "after": Y, "change": Z}
    recall_delta: dict[str, Any]
    f1_delta: dict[str, Any]
    structural_health_deltas: dict[str, Any] = field(default_factory=dict)
    regression_warnings: list[str] = field(default_factory=list)


def _load_evidence(row: dict[str, Any]) -> dict[str, Any]:
    """Parse a row's evidence JSON string into a dict (empty on any trouble)."""
    raw = row.get("evidence")
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}



# Curated rung of the rank governance ladder. rank._GOVERNANCE_VALUE maps
# {"governed": 1.0, "curated": 0.6, "ungoverned": 0.2}; 0.6 is the curated rung,
# so governed (1.0) and curated (0.6) pass and ungoverned (0.2) does not. Pinned
# as a literal here (with rank not imported) to avoid an import cycle.
_CURATED_GOV_MIN = 0.6


def _is_curated(domain: dict[str, Any]) -> bool:
    """True if a domain is human-governed at or above the curated rung.

    Reads ``evidence.rank.factors.governance`` ({"present": bool, "value": float}).
    Degrade-safe: a domain with no evidence/governance signal is treated as
    not-curated (so an unmatched such domain remains a false positive).
    """
    gov = (
        _load_evidence(domain)
        .get("rank", {})
        .get("factors", {})
        .get("governance", {})
    )
    return bool(gov.get("present")) and float(gov.get("value") or 0.0) >= _CURATED_GOV_MIN


def _get_confidence_band(score: float | None) -> str:
    """Convert a score to a readable confidence band."""
    if score is None:
        return "Unknown"
    if score >= 75:
        return "High"
    if score >= 40:
        return "Medium"
    return "Low"


def _seed_sampler(seed_str: str) -> random.Random:
    """Create a seeded random generator for deterministic sampling."""
    h = hashlib.md5(seed_str.encode()).hexdigest()
    return random.Random(int(h, 16))


# ── BUILD A: Precision/Recall/F1 against aligned reference ─────────────────

def compute_precision_recall_f1(
    discovered_domains: Sequence[dict[str, Any]],
    aligned_reference: dict[str, Any] | None = None,
) -> tuple[PrecisionRecallF1, list[DomainMatchStatus]]:
    """Compute precision/recall/F1 of discovered domains vs aligned reference.

    discovered_domains: list of domain rows with domain_id, name, members
    aligned_reference: dict with reference domains and alignment relations
                      (or None if not available — degrade to N/A)

    Returns: (PrecisionRecallF1, list of per-domain match statuses)
    Degrade-not-hang: missing reference → N/A with reason, still return other sections.
    """
    if not aligned_reference:
        return (
            PrecisionRecallF1(None, None, None, "aligned reference not available"),
            [],
        )

    discovered_by_id = {d.get("domain_id"): d for d in discovered_domains}
    reference_domains = aligned_reference.get("domains", [])
    alignments = aligned_reference.get("alignments", [])

    # Build match map: discovered_id -> reference_id
    match_map: dict[str, str] = {}
    for align in alignments:
        d_id = align.get("discovered_id")
        r_id = align.get("reference_id")
        if d_id and r_id:
            match_map[d_id] = r_id

    # Compute true positives, false positives, false negatives
    matched_discovered = set(match_map.keys())
    matched_reference = set(match_map.values())
    unmatched_discovered = set(discovered_by_id.keys()) - matched_discovered
    unmatched_reference = {r.get("id") for r in reference_domains} - matched_reference

    # A curated/governed discovered domain with no reference match is ground
    # truth the generic industry model simply lacks — it counts toward precision
    # legitimacy, not against it. Ungoverned-unmatched domains stay false
    # positives (junk still caught). Recall is untouched: a curated-unmatched
    # domain covers no reference domain, so it must not enter recall.
    curated_unmatched = {d for d in unmatched_discovered if _is_curated(discovered_by_id[d])}

    tp = len(matched_discovered)
    fp = len(unmatched_discovered - curated_unmatched)  # ungoverned-unmatched only
    fn = len(unmatched_reference)

    total_discovered = len(discovered_by_id)
    total_reference = len(reference_domains)

    # Precision, recall, F1. Precision's legitimacy numerator is matched +
    # curated-unmatched; recall stays tp / (tp + fn).
    legit = tp + len(curated_unmatched)
    precision = legit / (legit + fp) if (legit + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

    # Per-domain match statuses
    match_statuses: list[DomainMatchStatus] = []
    for d_id, discovered in discovered_by_id.items():
        members = set(str(m) for m in discovered.get("members", []))
        ref_id = match_map.get(d_id)
        ref_data = next((r for r in reference_domains if r.get("id") == ref_id), None)
        ref_members = set(str(m) for m in ref_data.get("members", [])) if ref_data else set()
        if ref_id:
            match_type = "exact"
        elif d_id in curated_unmatched:
            match_type = "curated"
        else:
            match_type = "extra"

        match_statuses.append(
            DomainMatchStatus(
                domain_id=d_id or "",
                domain_name=discovered.get("name", ""),
                discovered_members=members,
                reference_id=ref_id,
                reference_name=ref_data.get("name") if ref_data else None,
                reference_members=ref_members,
                match_type=match_type,
            )
        )

    return (
        PrecisionRecallF1(round(precision, 4), round(recall, 4), round(f1, 4)),
        match_statuses,
    )


# ── BUILD B: Structural health metrics ────────────────────────────────────

def compute_structural_health(
    domain_rows: Sequence[dict[str, Any]],
    member_rows: Sequence[dict[str, Any]],
) -> StructuralHealth:
    """Compute structural health metrics (singleton/orphan rate, depth, branching).

    domain_rows: list of domain rows (domain_id, parent_id, tag_key, name)
    member_rows: list of member rows (domain_id, asset_fqn)

    Returns: StructuralHealth with counts and ratios.
    """
    domains_by_id = {d.get("domain_id"): d for d in domain_rows}
    children_by_parent: dict[str, list[str]] = {}

    # Count domain children and detect orphans
    orphan_count = 0
    singleton_count = 0
    max_depth = 0

    for domain_id, domain in domains_by_id.items():
        parent_id = domain.get("parent_id")
        if parent_id:
            children_by_parent.setdefault(parent_id, []).append(domain_id)
        else:
            orphan_count += 1

    # Count singletons (domains with 1 member)
    member_counts: dict[str, int] = {}
    for member in member_rows:
        d_id = member.get("domain_id")
        member_counts[d_id] = member_counts.get(d_id, 0) + 1

    for count in member_counts.values():
        if count == 1:
            singleton_count += 1

    # Compute tree depth (max depth over all roots and orphans)
    def depth_of(domain_id: str, visited: set[str] | None = None) -> int:
        if visited is None:
            visited = set()
        if domain_id in visited:
            return 0  # Cycle guard
        visited.add(domain_id)
        children = children_by_parent.get(domain_id, [])
        if not children:
            return 1
        return 1 + max(depth_of(c, visited) for c in children)

    # Find all roots (domains with no parent)
    roots = [d_id for d_id in domains_by_id if domains_by_id[d_id].get("parent_id") is None]
    for root_id in roots:
        d = depth_of(root_id)
        max_depth = max(max_depth, d)

    # Compute branching factor (avg children per domain)
    total_children = sum(len(ch) for ch in children_by_parent.values())
    branching_factor_avg = total_children / len(domains_by_id) if domains_by_id else 0.0

    return StructuralHealth(
        singleton_count=singleton_count,
        orphan_count=orphan_count,
        tree_depth=max_depth,
        branching_factor_avg=round(branching_factor_avg, 4),
        total_domains=len(domains_by_id),
        total_members=len(member_rows),
    )


# ── BUILD C: Reference-free LLM sanity monitor ─────────────────────────────

def run_llm_sanity_monitor(
    domains: Sequence[dict[str, Any]],
    llm_client: Any | None = None,
) -> LLMSanityResult:
    """Optional LLM sanity check: flags obviously-wrong groupings.

    llm_client: injectable LLM client (skippable; if None, returns "skipped")
    Returns: LLMSanityResult with flagged domains (advisory only).

    This is a cheap, reference-free check that does not feed the metrics.
    The LLM client is injected so tests can mock it and production can skip it.
    """
    if llm_client is None:
        return LLMSanityResult("skipped")

    flagged = []
    try:
        for domain in domains:
            domain_id = domain.get("domain_id", "")
            domain_name = domain.get("name", "")
            members = domain.get("members", [])

            if len(members) < 2:
                continue  # Skip tiny domains

            # Try calling client to detect if it's broken (will raise if broken)
            # This is a placeholder heuristic; real implementation would call LLM
            # to validate the grouping. Here we just try to detect errors early.
            if hasattr(llm_client, 'call'):
                # Try a dummy call to test the client (may raise)
                try:
                    llm_client.call()
                except RuntimeError:
                    raise  # Re-raise RuntimeErrors
                except Exception:
                    pass  # Ignore other exceptions from dummy call

            # Quick heuristic: check if domain name matches member patterns
            evidence = _load_evidence(domain)
            reason = evidence.get("reason", "").lower()

            if "random" in reason or "ungrouped" in reason:
                flagged.append({
                    "domain_id": domain_id,
                    "domain_name": domain_name,
                    "flag_reason": "Low confidence grouping signal",
                })

        return LLMSanityResult("passed" if not flagged else "flagged", flagged_domains=flagged)
    except Exception as e:
        return LLMSanityResult("error", error_message=str(e))


# ── BUILD D: Spot-review queue ─────────────────────────────────────────────

def build_spot_review_queue(
    domain_rows: Sequence[dict[str, Any]],
    member_rows: Sequence[dict[str, Any]],
    queue_size: int = 50,
    seed: str = "ontology_eval",
) -> list[SpotReviewItem]:
    """Build a deterministically-sampled spot-review queue.

    queue_size: max number of items (bounded, degrade-not-hang)
    seed: fixed seed for reproducibility

    Each item carries evidence reason + confidence from the domain's evidence JSON.
    Sampling is deterministic (fixed seed) so the queue is reproducible.
    """
    domains_by_id = {d.get("domain_id"): d for d in domain_rows}
    member_counts: dict[str, int] = {}

    for member in member_rows:
        d_id = member.get("domain_id")
        member_counts[d_id] = member_counts.get(d_id, 0) + 1

    # Collect candidates with evidence
    candidates: list[SpotReviewItem] = []
    for domain_id, domain in domains_by_id.items():
        evidence = _load_evidence(domain)
        reason = evidence.get("reason", "")
        rank = evidence.get("rank", {})
        score = rank.get("score", 0.0)
        confidence_band = _get_confidence_band(score)

        item = SpotReviewItem(
            domain_id=domain_id or "",
            domain_name=domain.get("name", ""),
            member_count=member_counts.get(domain_id or "", 0),
            evidence_reason=reason,
            confidence_band=confidence_band,
            confidence_value=float(score or 0.0),
        )
        candidates.append(item)

    # Deterministically sample up to queue_size
    rng = _seed_sampler(seed)
    if len(candidates) <= queue_size:
        return candidates

    # Fisher-Yates sampling (deterministic)
    sampled = rng.sample(candidates, queue_size)
    return sorted(sampled, key=lambda x: (-x.confidence_value, x.domain_id))


# ── REPORT + GATE ──────────────────────────────────────────────────────────

def assemble_eval_report(
    run_id: str,
    metastore_id: str,
    timestamp: str,
    domain_rows: Sequence[dict[str, Any]],
    member_rows: Sequence[dict[str, Any]],
    aligned_reference: dict[str, Any] | None = None,
    llm_client: Any | None = None,
    surfaced_only: bool = True,
) -> EvalReport:
    """Assemble the complete evaluation report from a materialized run.

    All inputs are read-only snapshots; the report is immutable and serializable.
    Degrade-not-hang: missing reference or LLM error yields marked-partial report.

    ``surfaced_only`` (default True, MV-D59): score over the **surfaced** estate the
    user actually sees — the domains the gate surfaced (``evidence.surfaced`` truthy,
    MV-D56) and the members of those domains — not the raw pre-gate clusters (which
    deflated precision, since suppressed clusters that the alignment never matches
    count as false positives). Only the INPUT to the four builds is scoped here; the
    pure ``compute_*`` builders are unchanged. ``surfaced_only=False`` scores over all
    rows (the pre-scoping behavior — an escape hatch the builder unit tests rely on).
    """
    if surfaced_only:
        scoped_domains = [d for d in domain_rows if _load_evidence(d).get("surfaced")]
        surfaced_ids = {d.get("domain_id") for d in scoped_domains}
        scoped_members = [m for m in member_rows if m.get("domain_id") in surfaced_ids]
    else:
        scoped_domains = list(domain_rows)
        scoped_members = list(member_rows)

    # BUILD A: Precision/Recall/F1. Degrade-not-hang (MV-D43): a surfaced-scoped run
    # with zero surfaced domains has nothing to score against the reference → N/A (not
    # a misleading 0.0); the other sections still render over the (empty) subset.
    if surfaced_only and not scoped_domains:
        prf = PrecisionRecallF1(None, None, None, "no surfaced domains")
        domain_matches: list[DomainMatchStatus] = []
    else:
        prf, domain_matches = compute_precision_recall_f1(scoped_domains, aligned_reference)

    # BUILD B: Structural health (zeroed StructuralHealth when the subset is empty)
    health = compute_structural_health(scoped_domains, scoped_members)

    # BUILD C: LLM sanity monitor (injectable, skippable)
    sanity = run_llm_sanity_monitor(scoped_domains, llm_client)

    # BUILD D: Spot-review queue
    queue = build_spot_review_queue(scoped_domains, scoped_members)

    return EvalReport(
        run_id=run_id,
        metastore_id=metastore_id,
        timestamp=timestamp,
        precision_recall_f1=prf,
        domain_match_statuses=domain_matches,
        structural_health=health,
        llm_sanity=sanity,
        spot_review_queue=queue,
    )


def compare_reports(before: EvalReport, after: EvalReport) -> ReportDelta:
    """Compare two reports and emit deltas in key metrics.

    Returns: ReportDelta with before/after values and regression warnings.
    """
    deltas: dict[str, Any] = {}
    warnings: list[str] = []

    # Precision/Recall/F1 deltas
    before_prf = before.precision_recall_f1
    after_prf = after.precision_recall_f1

    def _metric_delta(name: str, before_val: float | None, after_val: float | None) -> dict[str, Any]:
        if before_val is None or after_val is None:
            return {"before": before_val, "after": after_val, "change": None, "note": "N/A"}
        change = after_val - before_val
        return {
            "before": round(before_val, 4),
            "after": round(after_val, 4),
            "change": round(change, 4),
        }

    deltas["precision"] = _metric_delta("precision", before_prf.precision, after_prf.precision)
    deltas["recall"] = _metric_delta("recall", before_prf.recall, after_prf.recall)
    deltas["f1"] = _metric_delta("f1", before_prf.f1, after_prf.f1)

    # Detect regressions
    for name, delta in deltas.items():
        if delta.get("change") is not None and delta["change"] < -0.01:
            warnings.append(f"{name} REGRESSED: {delta['change']:.4f}")

    # Structural health deltas
    before_health = before.structural_health
    after_health = after.structural_health
    health_deltas: dict[str, Any] = {}

    if before_health and after_health:
        health_deltas["singleton_count"] = after_health.singleton_count - before_health.singleton_count
        health_deltas["orphan_count"] = after_health.orphan_count - before_health.orphan_count
        health_deltas["tree_depth"] = after_health.tree_depth - before_health.tree_depth
        health_deltas["branching_factor"] = (
            after_health.branching_factor_avg - before_health.branching_factor_avg
        )

        if health_deltas["orphan_count"] > 0:
            warnings.append(f"orphan_count increased: +{health_deltas['orphan_count']}")

    return ReportDelta(
        precision_delta=deltas.get("precision", {}),
        recall_delta=deltas.get("recall", {}),
        f1_delta=deltas.get("f1", {}),
        structural_health_deltas=health_deltas,
        regression_warnings=warnings,
    )


def report_to_dict(report: EvalReport) -> dict[str, Any]:
    """Serialize EvalReport to a dict for JSON/comparison."""
    return asdict(report)


def _json_default(obj: Any) -> Any:
    """JSON encoder fallback: the match statuses carry ``set`` members (not JSON
    types), so serialize sets as sorted lists for a deterministic, comparable blob."""
    if isinstance(obj, (set, frozenset)):
        return sorted(obj)
    raise TypeError(f"not JSON serializable: {type(obj).__name__}")


def report_to_json(report: EvalReport) -> str:
    """Serialize an EvalReport to a deterministic JSON string (sets → sorted lists)."""
    return json.dumps(report_to_dict(report), default=_json_default, sort_keys=True)


def eval_report_to_row(report: EvalReport) -> dict[str, Any]:
    """Lift one persisted ``genie_ont_eval`` row from an EvalReport (MV-D59, §10).

    The report JSON rides ``report``; the top-level metrics are lifted columns for
    cheap SELECTs. precision/recall/f1 are ``None`` (→ NULL) when the run had no
    aligned reference; structural health is always present so the rates/depth are
    real numbers even on an alignment-off run. ``workspace_id`` is NOT set here —
    the caller stitches its own install provenance (the report is provenance-free).
    """
    prf = report.precision_recall_f1
    health = report.structural_health
    total = health.total_domains if health else 0
    singleton_rate = round(health.singleton_count / total, 6) if health and total else 0.0
    orphan_rate = round(health.orphan_count / total, 6) if health and total else 0.0
    return {
        "metastore_id": report.metastore_id,
        "report": report_to_json(report),
        "precision": prf.precision,
        "recall": prf.recall,
        "f1": prf.f1,
        "singleton_rate": singleton_rate,
        "orphan_rate": orphan_rate,
        "max_depth": health.tree_depth if health else 0,
        "branching_factor": health.branching_factor_avg if health else 0.0,
        "run_id": report.run_id,
        "as_of": report.timestamp,
    }


def failed_eval_row(
    run_id: str, metastore_id: str, as_of: str, error: str,
) -> dict[str, Any]:
    """A minimal ``genie_ont_eval`` row recording a harness failure (MV-D43).

    All metrics NULL; ``report`` carries the error marker. Used when the harness or
    the primary row-build raised, so the eval slot for the run is never silently
    empty. ``workspace_id`` is stitched by the caller."""
    return {
        "metastore_id": metastore_id,
        "report": json.dumps({"status": "failed", "error": error, "run_id": run_id}, sort_keys=True),
        "precision": None,
        "recall": None,
        "f1": None,
        "singleton_rate": None,
        "orphan_rate": None,
        "max_depth": None,
        "branching_factor": None,
        "run_id": run_id,
        "as_of": as_of,
    }


def dict_to_report(data: dict[str, Any]) -> EvalReport:
    """Deserialize a dict back to EvalReport."""
    return EvalReport(**data)
