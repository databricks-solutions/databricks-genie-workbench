"""Proposal grounding: drop patches whose targets do not appear in any
failing question's surface, then keep small auditable bundles.

The retail run shipped 8-patch bundles whose targets did not match the
failures the strategist claimed to be addressing. AG2 in particular
patched ``zone_combination`` / ``market_combination`` column
descriptions even though the failing benchmarks (Q011's missing
``YEAR(date_key_2)`` GROUP BY, Q009's wrong customer-count measure)
had nothing to do with those columns. This module enforces a
deterministic relevance check before apply:

* Every patch declares its targets via standard fields (``column``,
  ``target``, ``metric``, ``join_target``, ``instruction_section``,
  ``table``, ``section_name``, ``snippet_name``).
* Every failing eval row exposes its surface — column names from
  generated/expected SQL plus tokenized NL response.
* A patch's relevance is the fraction of its targets that overlap
  the union of failing-row surfaces.

Crucially, the comparison is purely local — schema identifiers and
tokenized NL only. No benchmark text is sent to an LLM or a remote
service, so the leakage firewall (Bug #4) stays intact.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable

from genie_space_optimizer.common.config import (
    IGNORED_OPTIMIZATION_JUDGES as _CONFIG_IGNORED_OPTIMIZATION_JUDGES,
)
from genie_space_optimizer.optimization.eval_row_access import (
    extract_failure_surface as _extract_failure_surface,
    row_qid as _row_qid,
    rows_for_qids as _rows_for_qids,
    token_terms as _canonical_token_terms,
)

logger = logging.getLogger(__name__)

# Identifier-like tokens. We're deliberately permissive on what counts
# as an identifier to keep regex parity with sqlglot for unparseable
# SQL (the AG2 `GROUP BY ALL` case).
_IDENT_RE = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b")

# Common patch fields that name an identifier the patch claims to
# influence. Order does not matter; the union goes into the targets
# set.
_PATCH_TARGET_KEYS: tuple[str, ...] = (
    "column",
    "target",
    "target_object",
    "target_table",
    "metric",
    "join_target",
    "instruction_section",
    "table",
    "section_name",
    "snippet_name",
)

def _normalize(token: str) -> str:
    return str(token).strip().lower()


_IGNORED_METADATA_PREFIXES: frozenset[str] = frozenset(
    _CONFIG_IGNORED_OPTIMIZATION_JUDGES
)
"""Judges whose ``*/metadata`` keys never seed grounding targets.

Sourced from ``common.config.IGNORED_OPTIMIZATION_JUDGES`` (driven by
``GSO_IGNORED_OPTIMIZATION_JUDGES``) so the optimizer engine has a
single, env-controllable ignored-judge policy.
"""


def extract_patch_targets(patch: dict) -> set[str]:
    """Return identifiers a patch claims to influence.

    Each value is split on whitespace to handle multi-word ``target``
    entries (e.g. ``"TIME GROUPING"`` becomes ``{"time grouping",
    "time", "grouping"}``) — we keep the joined form for exact-phrase
    matches and the parts for fuzzy ones.
    """
    targets: set[str] = set()
    for key in _PATCH_TARGET_KEYS:
        val = patch.get(key)
        if not isinstance(val, str) or not val.strip():
            continue
        joined = _normalize(val)
        targets.add(joined)
        if "." in joined:
            for dotted_part in joined.split("."):
                dotted_part = dotted_part.strip()
                if dotted_part:
                    targets.add(dotted_part)
        for part in _IDENT_RE.findall(joined):
            if part:
                targets.add(part)
    return targets


def extract_failure_surface(row: dict) -> set[str]:
    """Return identifiers + NL tokens visible in a failing eval row.

    Delegates to the canonical eval-row accessor so slash-style MLflow
    keys, dotted keys, nested dicts, and request/response payloads all
    contribute the same surface tokens.
    """
    return _extract_failure_surface(
        row,
        ignored_judges=_IGNORED_METADATA_PREFIXES,
    )


_INSTRUCTION_PATCH_TYPES = frozenset({
    "add_instruction",
    "update_instruction",
    "rewrite_instruction",
    "update_instruction_section",
})

_PATCH_BODY_KEYS: tuple[str, ...] = (
    "new_text",
    "proposed_value",
    "description",
    "synonyms",
    "expression",
    "sql",
    "join_spec",
    "structured_sections",
    "column_sections",
    "table_sections",
    "sql_snippet",
    "instruction",
    "rationale",
    "_rca_grounding_terms",
)


def _patch_type(patch: dict) -> str:
    return str(patch.get("type") or patch.get("patch_type") or "")


def _terms_from_any(value: object) -> set[str]:
    return _canonical_token_terms(value)


def extract_patch_grounding_terms(patch: dict) -> set[str]:
    terms = set(extract_patch_targets(patch))
    ptype = _patch_type(patch)
    if ptype in _INSTRUCTION_PATCH_TYPES:
        terms = {
            t for t in terms
            if t not in {"function", "routing", "function routing", "query rules", "asset routing", "constraints"}
        }
    for key in _PATCH_BODY_KEYS:
        if key in patch:
            terms |= _terms_from_any(patch.get(key))
    return terms


_GENERIC_RCA_GROUNDING_TERMS: frozenset[str] = frozenset({
    "query",
    "queries",
    "metric",
    "metrics",
    "measure",
    "measures",
    "sales",
    "value",
    "values",
    "filter",
    "filters",
    "group",
    "grouping",
    "time",
    "date",
    "table",
    "column",
    "columns",
    "genie",
    "expected",
    "actual",
})


def _raw_rca_terms(patch: dict) -> set[str]:
    return _terms_from_any(patch.get("_rca_grounding_terms"))


def _rca_terms(patch: dict) -> set[str]:
    """Specific RCA grounding terms (generic stopwords removed).

    Generic words like ``query`` or ``metric`` cannot full-score a patch on
    their own — they almost always overlap any failure surface. Specific
    identifiers (``zone_vp_name``, ``plural_top_n_collapse``) drive
    grounding; generic overlap is reported via ``generic_rca_overlap``.
    """
    terms = _terms_from_any(patch.get("_rca_grounding_terms"))
    return {
        term for term in terms
        if term not in _GENERIC_RCA_GROUNDING_TERMS and len(term) > 2
    }


def _score_from_sets(
    *,
    targets: set[str],
    surface: set[str],
    rca_terms: set[str],
    raw_rca_terms: set[str],
    row_count: int,
    min_relevance: float = 0.0,
) -> tuple[float, str, set[str], set[str]]:
    if row_count == 0:
        return 0.0, "no_scoped_rows", set(), set()
    if not surface:
        return 0.0, "empty_surface", set(), set()
    overlap = targets & surface
    rca_overlap = rca_terms & surface
    generic_rca_overlap = (raw_rca_terms - rca_terms) & surface
    if rca_overlap:
        return 1.0, "grounded", overlap, rca_overlap
    if generic_rca_overlap:
        return 0.0, "generic_rca_overlap", overlap, generic_rca_overlap
    if not targets:
        return 0.0, "no_targets", overlap, rca_overlap
    if not overlap:
        return 0.0, "no_overlap", overlap, rca_overlap
    score = len(overlap) / len(targets)
    if score < float(min_relevance):
        return score, "below_min_relevance", overlap, rca_overlap
    return score, "grounded", overlap, rca_overlap


def relevance_score(
    patch: dict,
    failing_rows: Iterable[dict],
    *,
    min_relevance: float = 0.0,
) -> float:
    """Fraction of patch targets that appear in any failing row's surface.

    Returns ``0.0`` when the patch has no targets or no failing rows
    are supplied. Always in ``[0.0, 1.0]``.

    When the patch carries explicit ``_rca_grounding_terms`` (RCA-driven
    proposals from the executable RCA control plane), any overlap of those
    RCA terms with the failure surface counts as full grounding — even if
    generic body-target overlap is sparse, because the RCA terms are
    exactly what the harness wanted to ground on.
    """
    targets = extract_patch_grounding_terms(patch)
    rows = list(failing_rows or [])
    union_surface: set[str] = set()
    for row in rows:
        union_surface |= extract_failure_surface(row)
    score, _category, _overlap, _rca_overlap = _score_from_sets(
        targets=targets,
        surface=union_surface,
        rca_terms=_rca_terms(patch),
        raw_rca_terms=_raw_rca_terms(patch),
        row_count=len(rows),
        min_relevance=min_relevance,
    )
    return score


def explain_relevance(
    patch: dict,
    failing_rows: Iterable[dict],
    *,
    min_relevance: float = 0.0,
) -> dict:
    """Return debug details for why a patch did or did not ground.

    This mirrors :func:`relevance_score` while preserving the target,
    surface, overlap, RCA-term, and missing-target sets for audit rows and
    local troubleshooting. The ``failure_category`` makes downstream retry
    logic actionable.
    """
    targets = extract_patch_grounding_terms(patch)
    rca_terms = _rca_terms(patch)
    raw_rca_terms = _raw_rca_terms(patch)
    rows = list(failing_rows or [])
    union_surface: set[str] = set()
    for row in rows:
        union_surface |= extract_failure_surface(row)
    score, category, overlap, rca_overlap = _score_from_sets(
        targets=targets,
        surface=union_surface,
        rca_terms=rca_terms,
        raw_rca_terms=raw_rca_terms,
        row_count=len(rows),
        min_relevance=min_relevance,
    )
    return {
        "score": score,
        "failure_category": category,
        "targets": sorted(targets),
        "surface": sorted(union_surface),
        "surface_size": len(union_surface),
        "overlap": sorted(overlap),
        "rca_terms": sorted(rca_terms),
        "rca_overlap": sorted(rca_overlap),
        "missing_targets": sorted(targets - union_surface),
    }


def _filter_rows_for_qids(rows: Iterable[dict], qids: Iterable[str] | None) -> list[dict]:
    wanted = tuple(str(q) for q in (qids or []) if str(q))
    if not wanted:
        return [r for r in rows or [] if isinstance(r, dict)]
    return _rows_for_qids(rows, wanted)


def causal_relevance_score(
    patch: dict,
    failure_rows: Iterable[dict],
    *,
    target_qids: Iterable[str] | None = None,
    min_relevance: float = 0.0,
) -> float:
    """Score patch relevance against its causal target rows and ASI surface."""
    qids = tuple(target_qids or patch.get("target_qids") or ())
    scoped_rows = _filter_rows_for_qids(failure_rows, qids)
    return relevance_score(patch, scoped_rows, min_relevance=min_relevance)


def explain_causal_relevance(
    patch: dict,
    failure_rows: Iterable[dict],
    *,
    target_qids: Iterable[str] | None = None,
    min_relevance: float = 0.0,
) -> dict:
    """Debug causal grounding with qid scope and ASI-enriched surfaces."""
    qids = tuple(target_qids or patch.get("target_qids") or ())
    scoped_rows = _filter_rows_for_qids(failure_rows, qids)
    details = explain_relevance(patch, scoped_rows, min_relevance=min_relevance)
    details["target_qids"] = list(qids)
    details["scoped_row_count"] = len(scoped_rows)
    return details


def sql_filter_snippet_is_safe(
    patch: dict,
    *,
    passing_dependent_qids: tuple[str, ...],
    max_passing_dependents: int = 0,
) -> dict:
    """Reject broad ``add_sql_snippet_filter`` patches with passing dependents.

    A filter snippet that targets a table used by many passing questions can
    silently break those questions when applied. This pure helper reports
    whether such a filter is safe to apply, without changing other patch
    types.
    """
    patch_type = str(patch.get("type") or patch.get("patch_type") or "")
    if patch_type != "add_sql_snippet_filter":
        return {"safe": True, "reason": "not_sql_filter_snippet"}
    dependents = tuple(str(q) for q in passing_dependent_qids or () if str(q))
    if len(dependents) > int(max_passing_dependents):
        return {
            "safe": False,
            "reason": "broad_sql_filter_has_passing_dependents",
            "passing_dependent_qids": list(dependents[:10]),
        }
    return {"safe": True, "reason": "safe"}


_RCA_KIND_COMPATIBLE_PATCH_TYPES = {
    "missing_filter": frozenset({
        "add_sql_snippet_filter",
        "add_instruction",
        "update_instruction",
        "update_instruction_section",
        "rewrite_instruction",
        "add_example_sql",
    }),
    "missing_temporal_filter": frozenset({
        "add_sql_snippet_filter",
        "add_instruction",
        "update_instruction_section",
        "add_example_sql",
    }),
    "wrong_filter_condition": frozenset({
        "add_sql_snippet_filter",
        "add_instruction",
        "update_instruction_section",
        "add_example_sql",
    }),
    "missing_measure": frozenset({
        "add_sql_snippet_measure",
        "add_sql_snippet_calculation",
        "update_column_description",
        "add_example_sql",
    }),
    "missing_aggregation": frozenset({
        "add_sql_snippet_calculation",
        "add_instruction",
        "update_instruction_section",
        "add_example_sql",
    }),
    "wrong_asset_routing": frozenset({
        "update_table_description",
        "add_join",
        "add_instruction",
        "update_instruction_section",
    }),
}


def proposal_is_defect_compatible(proposal: dict) -> dict:
    """Return whether a proposal's patch type is compatible with its RCA defect."""
    rca_kind = str(
        proposal.get("rca_kind")
        or proposal.get("root_cause")
        or proposal.get("asi_failure_type")
        or ""
    ).strip().lower()
    patch_type = str(proposal.get("patch_type") or proposal.get("type") or "").strip()
    allowed = _RCA_KIND_COMPATIBLE_PATCH_TYPES.get(rca_kind)
    if allowed is None:
        return {"compatible": True, "reason": "unknown_rca_kind"}
    if patch_type in allowed:
        return {"compatible": True, "reason": "compatible"}
    return {
        "compatible": False,
        "reason": "patch_type_incompatible_with_rca_kind",
        "rca_kind": rca_kind,
        "patch_type": patch_type,
        "allowed_patch_types": sorted(allowed),
    }


_GLOBAL_INSTRUCTION_SECTIONS = frozenset({
    "QUERY RULES",
    "ASSET ROUTING",
    "CONSTRAINTS",
    "AGGREGATION RULES",
})


def instruction_patch_scope_is_safe(
    patch: dict,
    *,
    ag_target_qids: tuple[str, ...],
) -> dict:
    """Reject broad instruction rewrites that have no counterfactual footprint.

    ``rewrite_instruction`` and ``update_instruction_section`` patches that
    touch global sections (``QUERY RULES``, ``ASSET ROUTING``, etc.) without
    a specific target or counterfactual dependents can change behavior on
    many passing questions. The blast-radius gate cannot see them because
    they don't carry a table/column footprint, so this second classifier
    drops them before the patch cap.
    """
    ptype = str(patch.get("type") or patch.get("patch_type") or "")
    if ptype not in {"add_instruction", "rewrite_instruction", "update_instruction_section"}:
        return {"safe": True, "reason": "not_instruction_rewrite"}
    # Track B: a section-split child of a scanned rewrite_instruction
    # must carry passing_dependents from the parent. Missing the field
    # on a split-child indicates a propagation bug in
    # ``_split_rewrite_instruction_patch`` — fail loud rather than imply
    # safety from absence.
    if patch.get("_split_from") == "rewrite_instruction" and patch.get("passing_dependents") is None:
        return {
            "safe": False,
            "reason": "split_child_missing_passing_dependents",
            "section_name": str(patch.get("section_name") or "(none)"),
        }
    if patch.get("passing_dependents") is not None:
        return {"safe": True, "reason": "has_counterfactual_dependents"}

    section = str(
        patch.get("section_name")
        or patch.get("instruction_section")
        or ""
    ).upper().strip()
    has_specific_target = bool(
        patch.get("target_qids")
        or patch.get("_grounding_target_qids")
        or patch.get("target_object")
        or patch.get("target_table")
        or patch.get("column")
    )
    if ptype in {"add_instruction", "rewrite_instruction"} or section in _GLOBAL_INSTRUCTION_SECTIONS:
        if not has_specific_target:
            return {
                "safe": False,
                "reason": "global_instruction_scope_without_dependents",
                "section_name": section or "(full rewrite)",
            }
    return {"safe": True, "reason": "narrow_instruction_scope"}


def patch_blast_radius_is_safe(
    patch: dict,
    *,
    ag_target_qids: tuple[str, ...],
    max_outside_target: int = 0,
) -> dict:
    """Reject patches whose passing-dependents footprint exceeds the AG target.

    The counterfactual scan in ``harness.py`` stamps two fields on every
    proposal: ``passing_dependents`` (qids that currently pass and rely on
    the patch's target) and ``high_collateral_risk`` (set when dependents
    >= 2 * affected). This helper turns those informational stamps into a
    deterministic gate decision used right before the patch cap.
    """
    target_set = {str(q) for q in ag_target_qids or () if str(q)}
    raw_dependents = patch.get("passing_dependents")
    if raw_dependents is None:
        return {"safe": True, "reason": "no_passing_dependents_field"}
    dependents = [str(q) for q in (raw_dependents or []) if str(q)]
    outside = [q for q in dependents if q not in target_set]

    if patch.get("high_collateral_risk") and outside:
        return {
            "safe": False,
            "reason": "high_collateral_risk_flagged",
            "passing_dependents_outside_target": outside[:20],
        }
    if len(outside) > int(max_outside_target):
        return {
            "safe": False,
            "reason": "blast_radius_exceeds_threshold",
            "passing_dependents_outside_target": outside[:20],
        }
    if not outside:
        return {"safe": True, "reason": "no_passing_dependents_outside_target"}
    return {"safe": True, "reason": "within_threshold"}


def select_patch_bundle(
    proposals: list[dict],
    *,
    max_patches: int,
    min_relevance: float = 0.0,
    failing_rows_by_proposal: dict[str, list[dict]] | None = None,
    clusters_by_proposal: dict[str, dict] | None = None,
) -> list[dict]:
    """Drop ungrounded proposals; keep up to ``max_patches`` survivors.

    Order: by relevance score DESC (stable within a tie). The harness
    is responsible for any upstream diversity sorting; this function
    only enforces the relevance floor, the asset/blame alignment guard,
    and the size cap.

    When a proposal does not carry a precomputed ``relevance_score``,
    we compute it from ``failing_rows_by_proposal[proposal['id']]``.
    When ``clusters_by_proposal`` is supplied, any proposal whose
    target assets are disjoint from its cluster's lineage and that
    does not carry a ``cross_asset_justification`` is dropped with
    ``_drop_reason="asset_not_in_cluster_lineage"``.

    Empty input is a no-op.
    """
    if not proposals:
        return []
    from genie_space_optimizer.optimization.proposal_asset_alignment import (
        proposal_aligns_with_cluster,
    )

    rows_by_proposal = failing_rows_by_proposal or {}
    clusters = clusters_by_proposal or {}
    scored: list[tuple[dict, float]] = []
    for p in proposals:
        cluster = clusters.get(str(p.get("id", "")))
        alignment = proposal_aligns_with_cluster(p, cluster)
        if not alignment["aligned"]:
            p["_drop_reason"] = alignment["reason"]
            p["_alignment_proposal_assets"] = list(alignment["proposal_assets"])
            p["_alignment_cluster_assets"] = list(alignment["cluster_assets"])
            continue

        score = p.get("relevance_score")
        if score is None:
            failing = rows_by_proposal.get(str(p.get("id", "")), [])
            score = relevance_score(p, failing)
        scored.append((p, float(score)))

    grounded = [(p, s) for p, s in scored if s >= min_relevance]
    # Stable sort: Python's sort is stable, so equal-relevance
    # proposals retain their incoming order.
    grounded.sort(key=lambda ps: -ps[1])
    return [p for p, _s in grounded[:max_patches]]
