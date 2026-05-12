"""Phase 1 Action 1.1 — deterministic-first RCA card builder.

This module contains the pure logic for building an :class:`RCACard`
from per-QID ASI metadata, generated SQL, reference SQL, and Unity
Catalog lineage. The high-level entry point ``build_card`` is wired
into the legacy ``optimization.rca.build_rca_card`` stub so that the
existing recovery-policy orchestrator (Plan P-D) can invoke it
without signature changes.

Design constraints (verbatim from the Phase 1 plan, Action 1.1):

* Every field except ``rationale`` is derived deterministically.
* The self-grounding check rejects any card whose ``root_cause`` does
  not match the cluster's dominant ASI signal OR whose
  ``grounding_terms`` cannot all be located in ASI blame, generated
  SQL, or reference SQL.
* The optional LLM normalization layer (``rca_card_llm.normalize``)
  may only rewrite ``rationale``. Any attempt to mutate other fields
  is rejected and the deterministic rationale is kept.
* On any successful return, the caller mutates the cluster object so
  ``cluster["rca_card_id"]`` is non-empty and ``cluster["rca_card"]``
  is a truthy dict carrying the new card_id.

This module is intentionally narrow: it knows about RcaKind,
RCACard, and ASI metadata shape. Cluster-level orchestration
(self-grounding, LLM, post-condition assertions) is handled by the
top-level ``optimization.rca.build_rca_card`` integration.
"""

from __future__ import annotations

import re as _re
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Mapping, Optional

from genie_space_optimizer.optimization.rca import (
    RCACard as _RCACard,
    RcaKind,
    _safe_rca_kind,
    patch_family_for_rca_kind,
)


def dominant_root_cause(asi_by_qid: Mapping[str, dict]) -> RcaKind:
    """Return the cluster-dominant RcaKind across per-QID ASI metadata.

    Resolution rules (first-match wins):
      1. Empty input or all-unmappable signals → ``RcaKind.UNKNOWN``.
      2. Single qid → that qid's mapped RcaKind (or UNKNOWN).
      3. Multiple qids → simple-majority winner over the mapped
         RcaKind values, with lexical tie-breaking on
         ``RcaKind.value`` for determinism.

    The mapper for a single qid reuses the existing
    ``optimization.rca._safe_rca_kind`` so the vocabulary is
    centralised — adding a new ASI failure_type only requires
    extending that helper.
    """
    if not asi_by_qid:
        return RcaKind.UNKNOWN

    kinds: list[RcaKind] = []
    for _qid, metadata in asi_by_qid.items():
        if not isinstance(metadata, dict):
            continue
        failure_type = str(metadata.get("failure_type") or "").strip()
        if not failure_type:
            continue
        kinds.append(_safe_rca_kind(metadata.get("rca_kind"), failure_type, metadata))

    if not kinds:
        return RcaKind.UNKNOWN

    counts = Counter(kinds)
    max_count = max(counts.values())
    top = sorted(
        (k for k, c in counts.items() if c == max_count),
        key=lambda k: k.value,
    )
    return top[0]


def grounding_terms_from_asi(asi_by_qid: Mapping[str, dict]) -> frozenset[str]:
    """Aggregate every ``blame_set`` entry across cluster qids into a
    deduplicated frozenset of grounding terms.

    Skips entries whose metadata is not a dict, whose ``blame_set`` is
    missing/empty, or whose entries are non-string. Empty input
    returns an empty frozenset.
    """
    out: set[str] = set()
    for metadata in asi_by_qid.values():
        if not isinstance(metadata, dict):
            continue
        blame = metadata.get("blame_set")
        if not blame:
            continue
        for term in blame:
            if isinstance(term, str) and term:
                out.add(term)
    return frozenset(out)


# Closed mapping from RcaKind → ``intended_patch_shape``. Each value
# is a short verb-phrase identifier the strategist consumes to scope
# proposal generation. Distinct from ``patch_family`` (which names
# the proposal generator) — the shape is the *intent* expressed in
# the card so the strategist can refuse a proposal that lands in the
# right family but the wrong shape.
_INTENDED_PATCH_SHAPE: dict[RcaKind, str] = {
    RcaKind.METRIC_VIEW_ROUTING_CONFUSION: "route_to_correct_metric_view_with_contrast",
    RcaKind.MEASURE_SWAP: "disambiguate_measure_with_contrastive_example",
    RcaKind.CANONICAL_DIMENSION_MISSED: "guide_canonical_dimension_use",
    RcaKind.MISSING_REQUIRED_DIMENSION: "require_missing_dimension_in_grouping",
    RcaKind.EXTRA_DEFENSIVE_FILTER: "remove_unrequested_defensive_filter",
    RcaKind.JOIN_SPEC_MISSING_OR_WRONG: "specify_explicit_join_spec",
    RcaKind.FILTER_LOGIC_MISMATCH: "correct_filter_predicate_to_match_question",
    RcaKind.GRAIN_OR_GROUPING_MISMATCH: "match_question_grain_in_group_by",
    RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING: "register_synonym_for_entity",
    RcaKind.SQL_EXPRESSION_MISSING: "add_sql_snippet_for_missing_expression",
    RcaKind.EXAMPLE_SQL_SHAPE_NEEDED: "add_example_sql_for_question_shape",
    RcaKind.FUNCTION_OR_TVF_NOT_INVOKED: "route_to_function_or_tvf",
    RcaKind.FUNCTION_ROUTING_MISMATCH: "route_to_function_or_tvf",
    RcaKind.TOP_N_CARDINALITY_COLLAPSE: "enforce_explicit_top_n_cardinality",
    RcaKind.TIME_WINDOW_LOGIC_MISMATCH: "correct_time_window_logic",
    RcaKind.ASSET_TYPE_ROUTING_MISMATCH: "route_to_asset_type",
    RcaKind.UNKNOWN: "generic_judge_clarification",
}


def intended_patch_shape_for_root_cause(kind: RcaKind) -> str:
    """Map an ``RcaKind`` to its intended patch shape (short identifier)."""
    return _INTENDED_PATCH_SHAPE.get(kind, "generic_judge_clarification")


# Forbidden patch-family map. Each entry lists patch families the
# proposal generator MUST NOT emit for the given root cause. The list
# is conservative — only families that are known to misfire on the
# named root cause appear here. Other families are silently allowed;
# the strategist's own gates filter beyond these.
_FORBIDDEN_FAMILIES: dict[RcaKind, frozenset[str]] = {
    RcaKind.TOP_N_CARDINALITY_COLLAPSE: frozenset({
        "avoid_unrequested_defensive_filters",
        "filter_logic_guidance",
    }),
    RcaKind.MEASURE_SWAP: frozenset({
        "avoid_unrequested_defensive_filters",
    }),
    RcaKind.EXTRA_DEFENSIVE_FILTER: frozenset({
        "filter_logic_guidance",  # adding more filter logic is the wrong direction
    }),
    RcaKind.MISSING_REQUIRED_DIMENSION: frozenset({
        "avoid_unrequested_defensive_filters",
    }),
    RcaKind.GRAIN_OR_GROUPING_MISMATCH: frozenset({
        "avoid_unrequested_defensive_filters",
    }),
    RcaKind.TIME_WINDOW_LOGIC_MISMATCH: frozenset({
        "avoid_unrequested_defensive_filters",
    }),
    # Defensive default: every other RcaKind has no forbidden families
    # until evidence justifies adding one. Empty set is the "any
    # generator is permitted" signal.
}


def allowed_and_forbidden_patch_families(
    kind: RcaKind,
) -> tuple[frozenset[str], frozenset[str]]:
    """Return ``(allowed, forbidden)`` patch-family sets for the root cause.

    ``allowed`` is a singleton frozenset containing the canonical
    family from ``patch_family_for_rca_kind`` — the strategist may
    pick this family or any family not listed in ``forbidden``.
    ``forbidden`` is the closed set of families that misfire on this
    root cause (see ``_FORBIDDEN_FAMILIES``). The two sets are
    disjoint by construction (the canonical family is removed from
    forbidden if it ever appeared there).
    """
    canonical = patch_family_for_rca_kind(kind)
    allowed = frozenset({canonical})
    forbidden = _FORBIDDEN_FAMILIES.get(kind, frozenset()) - allowed
    return allowed, forbidden


@dataclass(frozen=True)
class SelfGroundingResult:
    """Outcome of the deterministic self-grounding check.

    ``ok=True`` means the proposed card may be returned. ``ok=False``
    means the builder must emit ``RCA_CARD_SELF_CHECK_FAILED`` and
    return ``None``.

    ``failure_reason`` is one of:
      * ``root_cause_disagrees_with_dominant_asi`` — the proposed
        ``root_cause`` differs from ``dominant_root_cause(asi_by_qid)``.
      * ``ungrounded_term`` — at least one grounding term cannot be
        located in ASI blame, generated SQL, or reference SQL across
        any cluster qid. ``ungrounded_terms`` lists the offenders
        (sorted, for stable telemetry).
    """

    ok: bool
    failure_reason: str | None = None
    ungrounded_terms: tuple[str, ...] = ()


def self_grounding_check(
    *,
    proposed_root_cause: RcaKind,
    proposed_grounding_terms: frozenset[str],
    asi_by_qid: Mapping[str, dict],
    generated_sql_by_qid: Mapping[str, str],
    reference_sql_by_qid: Mapping[str, str],
) -> SelfGroundingResult:
    """Deterministic self-grounding check.

    Two conditions must hold:
      1. ``proposed_root_cause == dominant_root_cause(asi_by_qid)``.
      2. Every ``term`` in ``proposed_grounding_terms`` appears in at
         least one of:
           * the union of ``blame_set`` across qids in ``asi_by_qid``;
           * any value of ``generated_sql_by_qid``;
           * any value of ``reference_sql_by_qid``.
         Match is case-insensitive substring (terms are typically
         column / table identifiers or short failure-type strings).
    """
    dominant = dominant_root_cause(asi_by_qid)
    if proposed_root_cause != dominant:
        return SelfGroundingResult(
            ok=False, failure_reason="root_cause_disagrees_with_dominant_asi",
        )

    blame_terms: set[str] = set()
    for metadata in asi_by_qid.values():
        if not isinstance(metadata, dict):
            continue
        for entry in metadata.get("blame_set") or ():
            if isinstance(entry, str) and entry:
                blame_terms.add(entry.lower())

    sql_corpus_lower = " ".join(
        str(s).lower()
        for s in list(generated_sql_by_qid.values()) + list(reference_sql_by_qid.values())
        if s
    )

    ungrounded: list[str] = []
    for term in proposed_grounding_terms:
        term_l = term.lower()
        if term_l in blame_terms:
            continue
        if term_l in sql_corpus_lower:
            continue
        ungrounded.append(term)

    if ungrounded:
        return SelfGroundingResult(
            ok=False,
            failure_reason="ungrounded_term",
            ungrounded_terms=tuple(sorted(ungrounded)),
        )

    return SelfGroundingResult(ok=True)


def _safe_card_id(cluster_id: str, root_cause: RcaKind) -> str:
    safe = _re.sub(r"[^a-zA-Z0-9_]+", "_", cluster_id or "unknown")
    return f"card_{safe}_{root_cause.value}"


def _deterministic_rationale(
    root_cause: RcaKind,
    grounding_terms: frozenset[str],
    intended_patch_shape: str,
) -> str:
    """Short, factual rationale assembled from the typed fields.

    Stable — does not include free-form text. The LLM normalizer can
    rewrite this into more polished prose without losing information.
    """
    terms = ", ".join(sorted(grounding_terms)) or "(no grounding terms)"
    return (
        f"Root cause: {root_cause.value}. Grounded on: {terms}. "
        f"Intended patch shape: {intended_patch_shape}."
    )


def build_card(
    *,
    cluster_id: str,
    qids: tuple[str, ...],
    asi_by_qid: Mapping[str, dict],
    generated_sql_by_qid: Mapping[str, str],
    reference_sql_by_qid: Mapping[str, str],
    llm_caller: Optional[Callable[[str], str]] = None,
) -> tuple[Optional[_RCACard], Optional[str], Optional[str]]:
    """End-to-end deterministic-first card build.

    Returns ``(card, self_check_failure_reason, llm_skip_reason)``.

    * ``card`` is the final ``RCACard`` (with optionally LLM-polished
      rationale) when self-grounding passes; ``None`` when it fails.
    * ``self_check_failure_reason`` is the typed reason from
      :class:`SelfGroundingResult` when the card could not be built;
      ``None`` on success.
    * ``llm_skip_reason`` is set when LLM normalization was attempted
      but skipped; ``None`` when normalization succeeded OR was not
      requested (``llm_caller is None``).
    """
    root_cause = dominant_root_cause(asi_by_qid)
    grounding = grounding_terms_from_asi(asi_by_qid)

    # Synthesize intended patch shape and family sets from the root.
    shape = intended_patch_shape_for_root_cause(root_cause)
    allowed, forbidden = allowed_and_forbidden_patch_families(root_cause)

    # Build the candidate card with a deterministic rationale.
    rationale = _deterministic_rationale(root_cause, grounding, shape)
    card = _RCACard(
        card_id=_safe_card_id(cluster_id, root_cause),
        cluster_id=cluster_id,
        qids=qids,
        root_cause=root_cause,
        grounding_terms=grounding,
        intended_patch_shape=shape,
        allowed_patch_families=allowed,
        forbidden_patch_families=forbidden,
        rationale=rationale,
    )

    check = self_grounding_check(
        proposed_root_cause=root_cause,
        proposed_grounding_terms=grounding,
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid=generated_sql_by_qid,
        reference_sql_by_qid=reference_sql_by_qid,
    )
    if not check.ok:
        return None, check.failure_reason, None

    # Optional LLM polish. Only mutates rationale.
    llm_skip_reason: Optional[str] = None
    if llm_caller is not None:
        from genie_space_optimizer.optimization.rca_card_llm import (
            normalize_card_rationale,
        )

        outcome = normalize_card_rationale(card=card, llm_caller=llm_caller)
        if outcome.skipped:
            llm_skip_reason = outcome.skip_reason
        else:
            card = outcome.card

    return card, None, llm_skip_reason
