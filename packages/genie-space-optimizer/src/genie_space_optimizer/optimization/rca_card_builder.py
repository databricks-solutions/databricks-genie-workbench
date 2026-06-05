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

Trial 19 B6 — back-compat boundary
----------------------------------

The closed dicts ``_INTENDED_PATCH_SHAPE`` and ``_FORBIDDEN_FAMILIES``
plus the ``RcaKind`` enum entries in this module are **back-compat
readers only**. Trial 19 Workstream B promotes Stage 1's free-text
``rca_kind_label`` and ``intended_patch_shape`` strings to the
authoritative classifications consumed by Stage 2 / Stage 3 prompts:

* B1 — ``dominant_root_cause_label`` aggregates the free-text labels
  for the cluster (the legacy ``dominant_root_cause`` is preserved
  as a back-compat alias that returns ``RcaKind.UNKNOWN`` when the
  flag is OFF).
* B2 — ``intended_patch_shape_for_root_cause`` prefers the LLM-
  emitted card string when present, falling back to
  ``_INTENDED_PATCH_SHAPE`` only for pre-Trial-19 Delta rows.
* B3 — The Stage 3 prompt's "patch-family fit rules" section
  supersedes ``allowed_and_forbidden_patch_families`` for new rows;
  the dict survives only to gate replays of pre-Trial-19 fixtures.

When the Trial 19 ``GSO_TRIAL19_LLM_FIRST_RCA`` flag is OFF, every
consumer falls back to the closed dicts + ``RcaKind`` enum here for
byte-stable replay of pre-Trial-19 fixtures. Do NOT add new entries
to either dict; new failure modes / repair intents / family
constraints are LLM-emitted and read verbatim.
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


_UNKNOWN_LABEL = "unknown"


def _extract_rca_label(metadata: Mapping[str, object]) -> str:
    """Extract an authoritative free-text rca_kind_label string from ASI metadata.

    Trial 19 B1 — preference order:

      1. Stage 1's LLM-emitted ``rca_kind_label`` (Trial 19 B5).
      2. Pre-Trial-19 ``rca_kind`` field, normalized via
         ``_safe_rca_kind`` and returned as ``.value`` (so old Delta
         rows still produce a non-empty label).
      3. Empty string when neither is present (caller treats as
         "unknown" but the empty marker lets the aggregator
         distinguish "the LLM declined" from "no LLM ran yet").
    """
    if not isinstance(metadata, dict):
        return ""
    label_raw = metadata.get("rca_kind_label")
    if isinstance(label_raw, str) and label_raw.strip():
        return label_raw.strip()
    failure_type = str(metadata.get("failure_type") or "").strip()
    rca_kind_raw = metadata.get("rca_kind")
    if failure_type or rca_kind_raw is not None:
        try:
            kind = _safe_rca_kind(rca_kind_raw, failure_type, metadata)
            return kind.value
        except Exception:
            return ""
    return ""


def dominant_root_cause_label(asi_by_qid: Mapping[str, dict]) -> str:
    """Trial 19 B1 — return the cluster-dominant free-text RCA label string.

    Resolution rules (first-match wins):

      1. Empty input or every qid resolves to empty → ``"unknown"``.
      2. Single qid → that qid's label (or ``"unknown"`` if empty).
      3. Multiple qids → simple-majority winner over the labels.
         Crucially, an ``"unknown"`` / empty label never beats a
         typed label, even if it is the most common — this fixes
         the Trial 18 postmortem failure where a mixed cluster of
         four QIDs (one typed ``top_n_cardinality_collapse``, three
         ``unknown``) collapsed to ``UNKNOWN`` and blocked the
         structural archetype.

    Ties between two typed labels are broken by:

      * Higher cluster confidence wins, if any qid's metadata carries
        a numeric ``confidence`` and the average per-label
        confidence differs.
      * Otherwise lexical on the label string for determinism.

    The mapper for a single qid reuses ``_extract_rca_label`` so
    Trial 19's free-text ``rca_kind_label`` wins, with back-compat to
    the closed ``RcaKind`` enum via ``_safe_rca_kind``.
    """
    if not asi_by_qid:
        return _UNKNOWN_LABEL

    labels: list[str] = []
    confidence_by_label: dict[str, list[float]] = {}
    for _qid, metadata in asi_by_qid.items():
        label = _extract_rca_label(metadata)
        if not label:
            continue
        labels.append(label)
        if isinstance(metadata, dict):
            conf_raw = metadata.get("confidence")
            try:
                conf_val = float(conf_raw) if conf_raw is not None else None
            except (TypeError, ValueError):
                conf_val = None
            if conf_val is not None:
                confidence_by_label.setdefault(label, []).append(conf_val)

    if not labels:
        return _UNKNOWN_LABEL

    counts = Counter(labels)

    typed_counts = {
        lbl: c for lbl, c in counts.items() if lbl != _UNKNOWN_LABEL
    }
    if typed_counts:
        counts = Counter(typed_counts)

    max_count = max(counts.values())
    candidates = [lbl for lbl, c in counts.items() if c == max_count]

    if len(candidates) > 1 and confidence_by_label:
        def _avg_conf(lbl: str) -> float:
            vals = confidence_by_label.get(lbl, [])
            return sum(vals) / len(vals) if vals else 0.0
        candidates.sort(key=lambda lbl: (-_avg_conf(lbl), lbl))
    else:
        candidates.sort()
    return candidates[0]


def dominant_root_cause(asi_by_qid: Mapping[str, dict]) -> RcaKind:
    """Return the cluster-dominant RcaKind across per-QID ASI metadata.

    Trial 19 B1 — **back-compat alias.** The new authoritative
    aggregator is :func:`dominant_root_cause_label` (returns a
    free-text string). This function now delegates to it and parses
    the resulting label back into ``RcaKind`` for pre-Trial-19
    callers that still expect the enum. New code paths gated by
    ``GSO_TRIAL19_LLM_FIRST_RCA`` consume the label directly.

    Behavior preserved for pre-Trial-19 inputs (rows carrying only
    ``failure_type`` / ``rca_kind`` fields, no ``rca_kind_label``):
    the label aggregator's selection round-trips back to the same
    enum the legacy implementation would have returned. Inputs that
    DO carry an ``rca_kind_label`` (Trial 19 Stage 1 output) gain
    the new "never collapse typed → UNKNOWN" semantics — required
    for Workstream B to unblock structural repair.
    """
    if not asi_by_qid:
        return RcaKind.UNKNOWN

    label = dominant_root_cause_label(asi_by_qid)
    if not label or label == _UNKNOWN_LABEL:
        return RcaKind.UNKNOWN
    try:
        return _safe_rca_kind(label, label, {})
    except Exception:
        return RcaKind.UNKNOWN


def grounding_terms_from_asi(asi_by_qid: Mapping[str, dict]) -> frozenset[str]:
    """Aggregate every ``blame_set`` entry across cluster qids into a
    deduplicated frozenset of grounding terms.

    Also unions in aggregation / filter atoms from any per-qid
    ``sql_diff`` payload — Trial-5 Run A's ``wrong_aggregation``
    clusters had non-empty SqlDiff data but the builder produced
    empty ``grounding_terms``, leading to ``rca_card_grounded=False``
    and the C3 stalemate. SqlDiff atoms are checked against
    generated / reference SQL by ``self_grounding_check`` downstream,
    so non-matching atoms simply fail self-grounding (the existing
    contract) — they never silently corrupt a card.

    Skips entries whose metadata is not a dict, whose ``blame_set``
    is missing/empty, or whose entries are non-string. Empty input
    returns an empty frozenset.
    """
    from genie_space_optimizer.optimization.sql_diff_grounding import (
        extract_aggregation_terms,
        extract_filter_terms,
    )

    out: set[str] = set()
    for metadata in asi_by_qid.values():
        if not isinstance(metadata, dict):
            continue
        blame = metadata.get("blame_set")
        if blame:
            for term in blame:
                if isinstance(term, str) and term:
                    out.add(term)
        sql_diff = metadata.get("sql_diff")
        for term in extract_aggregation_terms(sql_diff):
            out.add(term)
        for term in extract_filter_terms(sql_diff):
            out.add(term)
    return frozenset(out)


def grounding_terms_from_fix_text(
    *,
    asi_by_qid: Mapping[str, dict],
    generated_sql_by_qid: Mapping[str, str],
    reference_sql_by_qid: Mapping[str, str],
) -> frozenset[str]:
    """Plan 4a — mine SQL identifiers from per-qid ``counterfactual_fix``
    and ``wrong_clause`` text, intersected with the SQL corpus.

    The intersect is what guarantees every term returned here will
    pass ``self_grounding_check`` via the SQL channel. Callers
    should union the result with ``grounding_terms_from_asi`` and
    pass the union into ``self_grounding_check``.

    Empty inputs → empty frozenset.
    """
    from genie_space_optimizer.optimization.sql_diff_grounding import (
        extract_sql_identifiers_from_text,
    )

    if not asi_by_qid:
        return frozenset()

    sql_corpus_lower = " ".join(
        str(s).lower()
        for s in list(generated_sql_by_qid.values())
        + list(reference_sql_by_qid.values())
        if s
    )
    if not sql_corpus_lower:
        return frozenset()

    proposed: set[str] = set()
    for metadata in asi_by_qid.values():
        if not isinstance(metadata, dict):
            continue
        fix_text = str(metadata.get("counterfactual_fix") or "")
        wrong_clause = str(metadata.get("wrong_clause") or "")
        combined = f"{fix_text}\n{wrong_clause}".strip()
        if not combined:
            continue
        for term in extract_sql_identifiers_from_text(combined):
            proposed.add(term)

    surviving = {term for term in proposed if term.lower() in sql_corpus_lower}
    return frozenset(surviving)


# Trial 19 B6 — BACK-COMPAT READER ONLY.
#
# Closed mapping from ``RcaKind`` → ``intended_patch_shape`` (a short
# verb-phrase identifier the strategist consumes to scope proposal
# generation). Trial 19 Workstream B2 makes the LLM-emitted
# ``PerQidDiagnosis.intended_patch_shape`` string authoritative; this
# dict is read ONLY as a fallback when Stage 1 emits an empty intent
# (pre-Trial-19 Delta rows or rare LLM omissions). Do NOT extend with
# new entries — new intents are named by Stage 1 verbatim. When the
# Trial 19 ``GSO_TRIAL19_LLM_FIRST_RCA`` flag is OFF, the dict is the
# sole source for byte-stable replay of pre-Trial-19 fixtures.
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


def _extract_intended_patch_shape(metadata: Mapping[str, object]) -> str:
    """Trial 19 B2 — pull the LLM-emitted ``intended_patch_shape`` string.

    Returns the trimmed string when present and non-empty; otherwise
    returns an empty string (caller decides whether to fall back to
    the closed-dict back-compat map).
    """
    if not isinstance(metadata, dict):
        return ""
    raw = metadata.get("intended_patch_shape")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return ""


def intended_patch_shape_for_root_cause(
    kind: RcaKind,
    *,
    asi_by_qid: Mapping[str, dict] | None = None,
) -> str:
    """Map an ``RcaKind`` to its intended patch shape (short identifier).

    Trial 19 B2 — when ``asi_by_qid`` is supplied AND any cluster qid's
    metadata carries a non-empty ``intended_patch_shape`` string from
    Stage 1 (Trial 19 B5), that LLM-emitted string is authoritative
    and returned verbatim. Ties between qids are broken by majority
    vote; lexical sort breaks vote-ties for determinism.

    When ``asi_by_qid`` is None (legacy callers) OR every qid lacks a
    non-empty string (pre-Trial-19 Delta rows), the closed
    ``_INTENDED_PATCH_SHAPE`` dict is consulted as a back-compat
    fallback so existing fixtures remain byte-stable.
    """
    if asi_by_qid:
        shapes: list[str] = []
        for metadata in asi_by_qid.values():
            shape = _extract_intended_patch_shape(metadata)
            if shape:
                shapes.append(shape)
        if shapes:
            counts = Counter(shapes)
            max_count = max(counts.values())
            top = sorted(s for s, c in counts.items() if c == max_count)
            return top[0]
    return _INTENDED_PATCH_SHAPE.get(kind, "generic_judge_clarification")


# Trial 19 B6 — BACK-COMPAT READER ONLY.
#
# Forbidden patch-family map. Each entry lists patch families the
# proposal generator MUST NOT emit for the given root cause. Trial 19
# Workstream B3 moves this enforcement out of code and into the
# Stage 3 ``plan11_synthesize`` prompt as "patch-family fit rules"
# the LLM reasons over — the closed dict survives only to gate
# pre-Trial-19 Delta rows whose ``rca_kind`` enum value still drives
# proposal generation. Do NOT extend with new entries — new
# constraints are named in the Stage 3 prompt verbatim. When the
# Trial 19 ``GSO_TRIAL19_LLM_FIRST_RCA`` flag is OFF, the dict is the
# sole source for byte-stable replay of pre-Trial-19 fixtures.
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
    soft_grounding_sources: "Sequence[dict] | None" = None,
) -> SelfGroundingResult:
    """Deterministic self-grounding check (Phase 1 Action 1.1 + Addendum).

    Two conditions must hold:
      1. ``proposed_root_cause == dominant_root_cause(asi_by_qid)``.
         The dominant-root-cause check uses **hard cluster ASI only**.
         Soft sources are evidence, not authority.
      2. Every ``term`` in ``proposed_grounding_terms`` appears in at
         least one of:
           * the union of ``blame_set`` across qids in ``asi_by_qid``;
           * any value of ``generated_sql_by_qid``;
           * any value of ``reference_sql_by_qid``;
           * (Phase 1 Addendum) the union of ``blame_set`` AND
             ``counterfactual_fix`` text across qids in any
             ``soft_grounding_sources[i]["asi_by_qid"]``.

    Adding soft sources strengthens the check (more sources can
    ground) without weakening it — the term still has to appear in
    at least one source. Match is case-insensitive substring.
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

    # Phase 1 Addendum — soft-cluster ASI blame + counterfactual text as
    # additional grounding corpus. Empty when caller passes None / ()
    # (default), so existing call sites are byte-stable.
    soft_corpus_parts: list[str] = []
    for src in soft_grounding_sources or ():
        if not isinstance(src, dict):
            continue
        for soft_meta in (src.get("asi_by_qid") or {}).values():
            if not isinstance(soft_meta, dict):
                continue
            for entry in soft_meta.get("blame_set") or ():
                if isinstance(entry, str) and entry:
                    soft_corpus_parts.append(entry.lower())
            cf = str(soft_meta.get("counterfactual_fix") or "")
            if cf:
                soft_corpus_parts.append(cf.lower())
    soft_corpus_lower = " ".join(soft_corpus_parts)

    ungrounded: list[str] = []
    for term in proposed_grounding_terms:
        term_l = term.lower()
        if term_l in blame_terms:
            continue
        if term_l in sql_corpus_lower:
            continue
        if soft_corpus_lower and term_l in soft_corpus_lower:
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
    soft_clusters: "Sequence[dict] | None" = None,
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

    # Plan 4a — union text-mined grounding terms (intersected with
    # SQL corpus) into the proposed set. The intersect guarantees
    # every text-mined term passes self_grounding via the SQL
    # channel. Flag-gated so a regression can be reverted at runtime.
    from genie_space_optimizer.common.config import (
        rca_card_fix_text_grounding_enabled,
    )
    if rca_card_fix_text_grounding_enabled():
        text_terms = grounding_terms_from_fix_text(
            asi_by_qid=asi_by_qid,
            generated_sql_by_qid=generated_sql_by_qid,
            reference_sql_by_qid=reference_sql_by_qid,
        )
        if text_terms:
            grounding = frozenset(grounding | text_terms)

    # Synthesize intended patch shape and family sets from the root.
    # Trial 19 B2 — pass the per-QID ASI map so any LLM-emitted
    # ``intended_patch_shape`` string overrides the closed-dict
    # fallback. Old callers that hand-build a card without metadata
    # remain byte-stable via the empty-asi fallback path.
    shape = intended_patch_shape_for_root_cause(
        root_cause, asi_by_qid=asi_by_qid,
    )
    allowed, forbidden = allowed_and_forbidden_patch_families(root_cause)

    # Phase 1 Addendum — match soft-cluster evidence when supplied.
    # Empty tuple when ``soft_clusters`` is None / () so existing
    # callers are byte-stable. The matcher is pure-deterministic;
    # the cluster mutation in build_rca_card stores the matches on
    # the cluster object for Phase 2 Section B's harness.
    soft_matches: tuple[_SoftEvidenceMatch, ...] = ()
    if soft_clusters:
        soft_matches = match_soft_evidence(
            hard_root_cause=root_cause,
            hard_asi_by_qid=asi_by_qid,
            soft_clusters=tuple(soft_clusters),
        )

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
        supporting_soft_evidence=soft_matches,
    )

    check = self_grounding_check(
        proposed_root_cause=root_cause,
        proposed_grounding_terms=grounding,
        asi_by_qid=asi_by_qid,
        generated_sql_by_qid=generated_sql_by_qid,
        reference_sql_by_qid=reference_sql_by_qid,
        soft_grounding_sources=tuple(soft_clusters) if soft_clusters else (),
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


# ---------------------------------------------------------------------------
# Phase 1 Addendum — deterministic soft-evidence matcher.
# ---------------------------------------------------------------------------

from typing import Sequence as _Sequence

from genie_space_optimizer.optimization.rca import (
    SoftEvidenceMatch as _SoftEvidenceMatch,
)


# Closed root-family map. Each RcaKind belongs to exactly one family.
# Families discipline the matcher — a soft cluster in a different
# family is never paired with a hard cluster.
_RCA_FAMILY: dict[RcaKind, str] = {
    # filter_* — ASI strings ``missing_filter`` / ``wrong_filter`` /
    # ``wrong_filter_condition`` all map to FILTER_LOGIC_MISMATCH; the
    # time-window mismatch is grouped here too because its repair shape
    # (default time-window filter) is filter-shaped.
    RcaKind.FILTER_LOGIC_MISMATCH: "filter",
    RcaKind.TIME_WINDOW_LOGIC_MISMATCH: "filter",
    RcaKind.EXTRA_DEFENSIVE_FILTER: "filter",
    # top_n_* — cardinality-preserving top-N family.
    RcaKind.TOP_N_CARDINALITY_COLLAPSE: "top_n",
    # routing_* — dimension / metric-view / join routing ambiguity.
    RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING: "routing",
    RcaKind.METRIC_VIEW_ROUTING_CONFUSION: "routing",
    RcaKind.JOIN_SPEC_MISSING_OR_WRONG: "routing",
    RcaKind.CANONICAL_DIMENSION_MISSED: "routing",
    RcaKind.MISSING_REQUIRED_DIMENSION: "routing",
    RcaKind.ASSET_TYPE_ROUTING_MISMATCH: "routing",
    RcaKind.FUNCTION_ROUTING_MISMATCH: "routing",
    # measure_* — measure-swap / aggregation-mismatch / grain.
    RcaKind.MEASURE_SWAP: "measure",
    RcaKind.GRAIN_OR_GROUPING_MISMATCH: "measure",
    RcaKind.SQL_EXPRESSION_MISSING: "measure",
    RcaKind.FUNCTION_OR_TVF_NOT_INVOKED: "measure",
    RcaKind.EXAMPLE_SQL_SHAPE_NEEDED: "measure",
}


_SQL_CLAUSE_KEYWORDS = ("WHERE", "GROUP BY", "ORDER BY", "LIMIT", "JOIN")
_SOFT_EVIDENCE_TOKEN_SKIP = frozenset({
    "a", "an", "the", "add", "filter", "where", "to", "on", "and", "or",
})


def _root_family(rca_kind: RcaKind) -> str:
    return _RCA_FAMILY.get(rca_kind, "")


def _normalize_token(token: str) -> str:
    """Lowercase, strip surrounding punctuation, and drop table prefix.

    ``f.time_window`` and ``time_window`` both normalize to
    ``time_window`` so the matcher pairs counterfactuals that
    reference the same column with or without a table alias.
    """
    t = token.strip().strip(".,;:'\"`").lower()
    if "." in t:
        # Strip table prefix: ``f.time_window`` → ``time_window``.
        t = t.rsplit(".", 1)[-1]
    return t


def _tokens_from_text(text: str) -> set[str]:
    """Extract symbolic tokens (column / value / predicate hints) from
    a counterfactual-fix string. Strips articles, quoting, and
    punctuation. Returns a set of normalized lowercase tokens."""
    if not text:
        return set()
    raw = text.replace("=", " ").replace(",", " ").split()
    return {
        t for t in (_normalize_token(w) for w in raw)
        if t and t not in _SOFT_EVIDENCE_TOKEN_SKIP and len(t) > 1
    }


def _wrong_clause_keyword(wrong_clause: str) -> str:
    upper = (wrong_clause or "").upper()
    for kw in _SQL_CLAUSE_KEYWORDS:
        if kw in upper:
            return kw
    return ""


def match_soft_evidence(
    *,
    hard_root_cause: RcaKind,
    hard_asi_by_qid: Mapping[str, dict],
    soft_clusters: _Sequence[dict],
) -> tuple[_SoftEvidenceMatch, ...]:
    """Phase 1 Addendum — pair soft-cluster qids to a hard cluster's
    RCA via shared evidence.

    Eligibility (both must hold):
      1. Same root family.
      2. AT LEAST ONE of: shared_blame, matching_counterfactual,
         matching_wrong_clause.

    Returns matches sorted by ``(soft_cluster_id, soft_qid)`` so the
    output is deterministic and replay byte-stable.
    """
    hard_family = _root_family(hard_root_cause)
    if not hard_family:
        return ()

    # Aggregate hard-side evidence once.
    hard_blame: set[str] = set()
    hard_cf_tokens: set[str] = set()
    hard_clauses: set[tuple[str, str]] = set()  # (keyword, column-token)
    for meta in hard_asi_by_qid.values():
        if not isinstance(meta, dict):
            continue
        for entry in meta.get("blame_set") or ():
            if isinstance(entry, str) and entry:
                for tok in entry.replace("=", " ").split():
                    norm = _normalize_token(tok)
                    if norm:
                        hard_blame.add(norm)
        cf_tokens = _tokens_from_text(str(meta.get("counterfactual_fix") or ""))
        hard_cf_tokens |= cf_tokens
        kw = _wrong_clause_keyword(str(meta.get("wrong_clause") or ""))
        if kw:
            for col_tok in cf_tokens:
                hard_clauses.add((kw, col_tok))

    matches: list[_SoftEvidenceMatch] = []
    for soft in soft_clusters or ():
        soft_root = soft.get("dominant_root_cause")
        if not isinstance(soft_root, RcaKind):
            continue
        if _root_family(soft_root) != hard_family:
            continue
        soft_cluster_id = str(soft.get("cluster_id") or "")
        for soft_qid, meta in (soft.get("asi_by_qid") or {}).items():
            if not isinstance(meta, dict):
                continue
            soft_blame = {
                _normalize_token(tok)
                for entry in (meta.get("blame_set") or ())
                if isinstance(entry, str) and entry
                for tok in entry.replace("=", " ").split()
            } - {""}
            soft_cf = str(meta.get("counterfactual_fix") or "")
            soft_cf_tokens = _tokens_from_text(soft_cf)
            soft_clause_kw = _wrong_clause_keyword(str(meta.get("wrong_clause") or ""))

            evidence_token: str | None = None
            match_kind: str | None = None

            shared_blame_overlap = sorted(soft_blame & hard_blame)
            if shared_blame_overlap:
                match_kind = "shared_blame"
                evidence_token = shared_blame_overlap[0]
            elif soft_cf_tokens & hard_cf_tokens:
                match_kind = "matching_counterfactual"
                evidence_token = sorted(soft_cf_tokens & hard_cf_tokens)[0]
            elif soft_clause_kw:
                wrong_clause_hits = sorted(
                    col for (kw, col) in hard_clauses
                    if kw == soft_clause_kw and col in soft_cf_tokens
                )
                if wrong_clause_hits:
                    match_kind = "matching_wrong_clause"
                    evidence_token = wrong_clause_hits[0]

            if match_kind and evidence_token:
                matches.append(_SoftEvidenceMatch(
                    soft_qid=str(soft_qid),
                    soft_cluster_id=soft_cluster_id,
                    match_kind=match_kind,
                    evidence_token=evidence_token,
                    soft_counterfactual=soft_cf,
                ))

    matches.sort(key=lambda m: (m.soft_cluster_id, m.soft_qid))
    return tuple(matches)
