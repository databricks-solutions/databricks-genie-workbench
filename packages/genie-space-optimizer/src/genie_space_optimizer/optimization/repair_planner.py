"""Phase 2 Action 2.1 — Repair Planner.

The Repair Planner maps a cluster's Phase 1 ``RCACard`` onto one of
five named ``RepairArchetype``s and emits a structured ``RepairKit``
the downstream proposal generator consumes. This module hosts the
classification function; the kit-construction function lives in
``optimization.repair_kit`` so kit-aware patch-cap logic can read it
without importing the planner.

All functions are pure (no I/O, no logger, no clock). The Phase 2
``GSO_REPAIR_PLANNER`` flag gates the call site, not these functions
themselves — they are safe to call any time.
"""

from __future__ import annotations

from typing import Mapping, Optional

from genie_space_optimizer.optimization.rca import RCACard, RcaKind
from genie_space_optimizer.optimization.repair_archetypes import (
    REPAIR_ARCHETYPES,
    RepairArchetype,
)


_TIME_WINDOW_TOKENS: frozenset[str] = frozenset({
    "time_window", "mtd", "ytd", "qtd", "wtw",
})

_PAYMENT_TOKENS: frozenset[str] = frozenset({
    "payment_amt", "payment_currency_cd",
})

_DIMENSION_TERM_FALLBACK_PATTERNS: tuple[str, ...] = (
    "_name", "_description", "_combination",
)


def _has_plural_intent(cluster: Mapping[str, object]) -> bool:
    intent = str(cluster.get("asi_question_intent") or "").strip().lower()
    return intent == "plural"


def _has_explicit_cardinality(cluster: Mapping[str, object]) -> bool:
    intent = str(cluster.get("asi_question_intent") or "").strip().lower()
    if intent == "exact_cardinality":
        return True
    raw = cluster.get("asi_explicit_cardinality")
    return isinstance(raw, int) and raw > 0


def _has_dimension_term(card: RCACard) -> bool:
    """Heuristic: any grounding term that looks like a dimension column."""
    for term in card.grounding_terms:
        lowered = term.lower()
        if any(p in lowered for p in _DIMENSION_TERM_FALLBACK_PATTERNS):
            return True
    return False


def classify_cluster_archetype(
    *,
    card: RCACard,
    cluster: Mapping[str, object],
    additional_archetypes: tuple[RepairArchetype, ...] = (),
) -> Optional[RepairArchetype]:
    """Return the named RepairArchetype the cluster's RCACard fits into,
    or ``None`` when no archetype matches.

    Resolution order: priority within each ``RcaKind`` is the order in
    which the predicates are evaluated below. The two
    ``TOP_N_CARDINALITY_COLLAPSE`` archetypes (``top_n_exact_cardinality``
    and ``plural_top_n_collapse``) are evaluated specific-first
    (exact cardinality first, plural fallback).

    Section E (archetype learning) callers may pass
    ``additional_archetypes`` to merge provisional archetypes synthesised
    in-loop. Default empty tuple keeps Section A behaviour unchanged.
    """
    kind = card.root_cause

    # TOP_N_CARDINALITY_COLLAPSE — specific (exact cardinality) before
    # general (plural intent).
    if kind == RcaKind.TOP_N_CARDINALITY_COLLAPSE:
        if _has_explicit_cardinality(cluster):
            return _archetype("top_n_exact_cardinality")
        if _has_plural_intent(cluster):
            return _archetype("plural_top_n_collapse")
        return _match_provisional(card, cluster, additional_archetypes)

    # TIME_WINDOW_LOGIC_MISMATCH — requires a time-window token in
    # grounding to fire.
    if kind == RcaKind.TIME_WINDOW_LOGIC_MISMATCH:
        if card.grounding_terms & _TIME_WINDOW_TOKENS:
            return _archetype("default_time_window_filter")
        return _match_provisional(card, cluster, additional_archetypes)

    # SYNONYM_OR_ENTITY_MATCH_MISSING / METRIC_VIEW_ROUTING_CONFUSION —
    # dimension term required (heuristic: looks like a dimension column).
    if kind in (
        RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING,
        RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
    ):
        if _has_dimension_term(card):
            return _archetype("dimension_disambiguation")
        return _match_provisional(card, cluster, additional_archetypes)

    # MEASURE_SWAP with payment tokens.
    if kind == RcaKind.MEASURE_SWAP:
        if card.grounding_terms & _PAYMENT_TOKENS:
            return _archetype("payment_reporting_amount_semantics")
        return _match_provisional(card, cluster, additional_archetypes)

    return _match_provisional(card, cluster, additional_archetypes)


def _match_provisional(
    card: RCACard,
    cluster: Mapping[str, object],
    additional_archetypes: tuple[RepairArchetype, ...],
) -> Optional[RepairArchetype]:
    """Phase 2 Section E hook — match against provisional archetypes
    when no canonical archetype fires. Each provisional archetype
    matches when card.root_cause is in its applicable_rca_kinds AND
    either its required_grounding_tokens is empty or shares at least
    one token with card.grounding_terms.
    """
    for arch in additional_archetypes:
        if card.root_cause not in arch.applicable_rca_kinds:
            continue
        if arch.required_grounding_tokens and not (
            card.grounding_terms & arch.required_grounding_tokens
        ):
            continue
        return arch
    return None


def _archetype(name: str) -> RepairArchetype:
    for arch in REPAIR_ARCHETYPES:
        if arch.name == name:
            return arch
    raise AssertionError(f"REPAIR_ARCHETYPES missing entry: {name!r}")


def plan_repair(
    *,
    card: RCACard,
    cluster: Mapping[str, object],
    propagation_root_cause: str,
    additional_archetypes: tuple[RepairArchetype, ...] = (),
) -> Optional[dict]:
    """Return a structured ``RepairKit`` (as dict) for the cluster, or
    ``None`` when no archetype classifies the cluster.

    The returned dict has the following stable shape:

    * ``repair_archetype`` — str, one of the five named archetype names.
    * ``card_id`` — str, the source RCACard id.
    * ``cluster_id`` — str, the source cluster id.
    * ``target_qids`` — tuple[str, ...], the qids the kit claims to fix.
    * ``grounding_terms`` — tuple[str, ...], sorted copy of card.grounding_terms.
    * ``priority_step`` — str, one of ``PRIORITY_ORDER`` (after
      propagation conditioning).
    * ``expected_causal_effect`` — str, archetype's
      ``expected_causal_effect_template``.
    * ``required_companions`` — tuple[str, ...], priority steps that
      MUST also be present in the kit.
    * ``pre_eval_propagation_verification`` — bool, ``True`` when
      ``propagation_root_cause == 'propagation_lag'``.
    """
    from genie_space_optimizer.optimization.repair_priority import (
        select_priority_step,
    )

    archetype = classify_cluster_archetype(
        card=card,
        cluster=cluster,
        additional_archetypes=additional_archetypes,
    )
    if archetype is None:
        return None

    priority_step = select_priority_step(
        archetype=archetype,
        propagation_root_cause=propagation_root_cause,
    )

    required_companions: list[str] = []
    if (
        propagation_root_cause == "instruction_insufficient_force"
        and priority_step == "narrow_l6_snippet"
    ):
        required_companions.append("narrow_l6_snippet")

    return {
        "repair_archetype": archetype.name,
        "card_id": card.card_id,
        "cluster_id": str(cluster.get("cluster_id") or card.cluster_id),
        "target_qids": tuple(card.qids),
        "grounding_terms": tuple(sorted(card.grounding_terms)),
        "priority_step": priority_step,
        "expected_causal_effect": archetype.expected_causal_effect_template,
        "required_companions": tuple(required_companions),
        "pre_eval_propagation_verification": (
            propagation_root_cause == "propagation_lag"
        ),
        "provenance": archetype.provenance,
        "lifecycle_state": archetype.lifecycle_state,
    }


def apply_repair_planner_to_clusters(
    *,
    clusters: list[dict],
    propagation_root_cause: str,
    additional_archetypes: tuple[RepairArchetype, ...] = (),
) -> dict[str, int]:
    """Walk ``clusters``, classify each one whose ``rca_card`` is present,
    and stamp the resulting kit dict onto ``cluster['_repair_kit']``.

    Returns a count summary so the caller can emit decision records
    without re-iterating the clusters list.
    """
    classified = 0
    no_archetype_match = 0
    skipped_no_card = 0

    for cluster in clusters or []:
        card = cluster.get("rca_card")
        if not isinstance(card, RCACard):
            skipped_no_card += 1
            continue
        kit = plan_repair(
            card=card,
            cluster=cluster,
            propagation_root_cause=propagation_root_cause,
            additional_archetypes=additional_archetypes,
        )
        if kit is None:
            no_archetype_match += 1
            continue
        cluster["_repair_kit"] = kit
        classified += 1

    return {
        "classified": classified,
        "no_archetype_match": no_archetype_match,
        "skipped_no_card": skipped_no_card,
    }
