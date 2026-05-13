"""Tests for classify_cluster_archetype (Phase 2 Action 2.1)."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import RCACard, RcaKind


def _card(
    *,
    root_cause: RcaKind,
    grounding_terms: frozenset[str] = frozenset(),
    qids: tuple[str, ...] = ("gs_026",),
) -> RCACard:
    return RCACard(
        card_id="card_test",
        cluster_id="H001",
        qids=qids,
        root_cause=root_cause,
        grounding_terms=grounding_terms,
        intended_patch_shape="cardinality_preserving_top_n_guidance",
        allowed_patch_families=frozenset({"cardinality_preserving_top_n_guidance"}),
        forbidden_patch_families=frozenset(),
        rationale="test",
    )


def test_classify_returns_plural_top_n_for_collapse_with_plural_intent() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        classify_cluster_archetype,
    )

    card = _card(root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE)
    cluster = {"cluster_id": "H001", "asi_question_intent": "plural"}
    result = classify_cluster_archetype(card=card, cluster=cluster)
    assert result is not None
    assert result.name == "plural_top_n_collapse"


def test_classify_returns_top_n_exact_cardinality_when_explicit() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        classify_cluster_archetype,
    )

    card = _card(root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE)
    cluster = {
        "cluster_id": "H_009",
        "asi_question_intent": "exact_cardinality",
        "asi_explicit_cardinality": 2,
    }
    result = classify_cluster_archetype(card=card, cluster=cluster)
    assert result is not None
    assert result.name == "top_n_exact_cardinality"


def test_classify_returns_default_time_window_filter_with_token_in_grounding() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        classify_cluster_archetype,
    )

    card = _card(
        root_cause=RcaKind.TIME_WINDOW_LOGIC_MISMATCH,
        grounding_terms=frozenset({"time_window", "mtd"}),
    )
    cluster = {"cluster_id": "H_021"}
    result = classify_cluster_archetype(card=card, cluster=cluster)
    assert result is not None
    assert result.name == "default_time_window_filter"


def test_classify_returns_dimension_disambiguation_for_synonym_miss() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        classify_cluster_archetype,
    )

    card = _card(
        root_cause=RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING,
        grounding_terms=frozenset({"market_description"}),
    )
    cluster = {"cluster_id": "H_001"}
    result = classify_cluster_archetype(card=card, cluster=cluster)
    assert result is not None
    assert result.name == "dimension_disambiguation"


def test_classify_returns_dimension_disambiguation_for_mv_routing() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        classify_cluster_archetype,
    )

    card = _card(
        root_cause=RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
        grounding_terms=frozenset({"zone_vp_name"}),
    )
    cluster = {"cluster_id": "H_026"}
    result = classify_cluster_archetype(card=card, cluster=cluster)
    assert result is not None
    assert result.name == "dimension_disambiguation"


def test_classify_returns_payment_amount_semantics_with_payment_token() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        classify_cluster_archetype,
    )

    card = _card(
        root_cause=RcaKind.MEASURE_SWAP,
        grounding_terms=frozenset({"payment_amt"}),
    )
    cluster = {"cluster_id": "H_pay"}
    result = classify_cluster_archetype(card=card, cluster=cluster)
    assert result is not None
    assert result.name == "payment_reporting_amount_semantics"


def test_classify_returns_none_when_no_archetype_matches() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        classify_cluster_archetype,
    )

    card = _card(
        root_cause=RcaKind.UNKNOWN,
        grounding_terms=frozenset({"something_unrelated"}),
    )
    cluster = {"cluster_id": "H_misc"}
    result = classify_cluster_archetype(card=card, cluster=cluster)
    assert result is None


def test_classify_returns_none_when_card_root_cause_does_not_match_any_archetype() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        classify_cluster_archetype,
    )

    card = _card(
        root_cause=RcaKind.JOIN_SPEC_MISSING_OR_WRONG,
        grounding_terms=frozenset({"orders.customer_id"}),
    )
    cluster = {"cluster_id": "H_join"}
    result = classify_cluster_archetype(card=card, cluster=cluster)
    assert result is None


def test_classify_default_time_window_requires_token_in_grounding() -> None:
    """Without a time-window token in grounding, fall through to no-match."""
    from genie_space_optimizer.optimization.repair_planner import (
        classify_cluster_archetype,
    )

    card = _card(
        root_cause=RcaKind.TIME_WINDOW_LOGIC_MISMATCH,
        grounding_terms=frozenset({"unrelated_term"}),
    )
    cluster = {"cluster_id": "H_t"}
    result = classify_cluster_archetype(card=card, cluster=cluster)
    assert result is None


def test_plan_repair_classifies_against_additional_archetypes_when_no_canonical_match() -> None:
    """When the cluster's RCACard does not match any canonical archetype
    but matches a provisional one passed via additional_archetypes,
    plan_repair must classify against the provisional."""
    from genie_space_optimizer.optimization.repair_archetypes import RepairArchetype
    from genie_space_optimizer.optimization.repair_planner import plan_repair
    from genie_space_optimizer.optimization.rca import RCACard, RcaKind

    provisional = RepairArchetype(
        name="brand_dimension_disambiguation_provisional",
        applicable_rca_kinds=frozenset({RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING}),
        required_grounding_tokens=frozenset({"snack_brand"}),
        evidence_predicates=frozenset(),
        default_priority_step="repair_kit",
        expected_causal_effect_template="x",
        rationale="provisional",
    )
    card = RCACard(
        card_id="c1", cluster_id="S001", qids=("gs_002",),
        root_cause=RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING,
        grounding_terms=frozenset({"snack_brand"}),
        intended_patch_shape="entity_disambiguation",
        allowed_patch_families=frozenset(),
        forbidden_patch_families=frozenset(),
        rationale="t",
    )
    cluster = {"cluster_id": "S001", "question_ids": ["gs_002"]}
    kit = plan_repair(
        card=card, cluster=cluster, propagation_root_cause="unknown",
        additional_archetypes=(provisional,),
    )
    assert kit is not None
    assert kit["repair_archetype"] == "brand_dimension_disambiguation_provisional"


def test_plan_repair_default_additional_archetypes_is_empty_tuple_no_behaviour_change() -> None:
    """Default empty tuple - identical behaviour to Section A baseline."""
    from genie_space_optimizer.optimization.repair_planner import plan_repair
    from genie_space_optimizer.optimization.rca import RCACard, RcaKind

    card = RCACard(
        card_id="c1", cluster_id="H001", qids=("gs_026",),
        root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        grounding_terms=frozenset({"rank_eq_1"}),
        intended_patch_shape="cardinality_preserving_top_n_guidance",
        allowed_patch_families=frozenset(),
        forbidden_patch_families=frozenset(),
        rationale="t",
    )
    cluster = {"cluster_id": "H001", "asi_question_intent": "plural",
               "question_ids": ["gs_026"]}
    kit_default = plan_repair(
        card=card, cluster=cluster, propagation_root_cause="unknown",
    )
    kit_explicit = plan_repair(
        card=card, cluster=cluster, propagation_root_cause="unknown",
        additional_archetypes=(),
    )
    assert kit_default == kit_explicit
