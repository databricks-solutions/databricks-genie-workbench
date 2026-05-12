from __future__ import annotations

from genie_space_optimizer.optimization.rca import RCACard, RcaKind


def test_rca_card_dataclass_fields() -> None:
    card = RCACard(
        card_id="card_cluster_1_top_n_cardinality_collapse",
        cluster_id="cluster_1",
        qids=("gs_026",),
        root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        grounding_terms=frozenset({"plural_top_n_collapse", "zone_vp_name"}),
        intended_patch_shape="enforce_explicit_top_n_cardinality",
        allowed_patch_families=frozenset({"cardinality_preserving_top_n_guidance"}),
        forbidden_patch_families=frozenset({"avoid_unrequested_defensive_filters"}),
        rationale="Top-N collapse on plural zone_vp_name; require ORDER BY+LIMIT.",
    )
    assert card.card_id == "card_cluster_1_top_n_cardinality_collapse"
    assert card.root_cause == RcaKind.TOP_N_CARDINALITY_COLLAPSE
    assert "plural_top_n_collapse" in card.grounding_terms
    assert "cardinality_preserving_top_n_guidance" in card.allowed_patch_families


def test_rca_card_is_frozen() -> None:
    import dataclasses
    card = RCACard(
        card_id="x", cluster_id="c", qids=(), root_cause=RcaKind.UNKNOWN,
        grounding_terms=frozenset(), intended_patch_shape="",
        allowed_patch_families=frozenset(), forbidden_patch_families=frozenset(),
        rationale="",
    )
    try:
        card.rationale = "mutated"  # type: ignore[misc]
        raised = False
    except dataclasses.FrozenInstanceError:
        raised = True
    assert raised, "RCACard must be frozen"


from genie_space_optimizer.optimization.rca_card_builder import (
    allowed_and_forbidden_patch_families,
    dominant_root_cause,
    grounding_terms_from_asi,
    intended_patch_shape_for_root_cause,
)


def test_dominant_root_cause_simple_majority() -> None:
    asi_by_qid = {
        "gs_026": {"failure_type": "plural_top_n_collapse", "blame_set": ["zone_vp_name"]},
        "gs_027": {"failure_type": "plural_top_n_collapse", "blame_set": ["category_top"]},
        "gs_028": {"failure_type": "missing_filter", "blame_set": ["region"]},
    }
    assert dominant_root_cause(asi_by_qid) == RcaKind.TOP_N_CARDINALITY_COLLAPSE


def test_dominant_root_cause_tie_broken_by_lexical_order() -> None:
    asi_by_qid = {
        "gs_001": {"failure_type": "plural_top_n_collapse"},
        "gs_002": {"failure_type": "missing_filter"},
    }
    # FILTER_LOGIC_MISMATCH ("filter_logic_mismatch") < TOP_N_CARDINALITY_COLLAPSE
    # ("top_n_cardinality_collapse") lexically — tie breaker picks FILTER_LOGIC_MISMATCH.
    assert dominant_root_cause(asi_by_qid) == RcaKind.FILTER_LOGIC_MISMATCH


def test_dominant_root_cause_empty_returns_unknown() -> None:
    assert dominant_root_cause({}) == RcaKind.UNKNOWN


def test_dominant_root_cause_all_unmappable_returns_unknown() -> None:
    asi_by_qid = {"gs_001": {"failure_type": ""}}
    assert dominant_root_cause(asi_by_qid) == RcaKind.UNKNOWN


def test_grounding_terms_aggregate_blame_set_across_qids() -> None:
    asi_by_qid = {
        "gs_026": {"blame_set": ["zone_vp_name", "plural_top_n_collapse"]},
        "gs_027": {"blame_set": ["plural_top_n_collapse", "category_top"]},
    }
    terms = grounding_terms_from_asi(asi_by_qid)
    assert terms == frozenset({"zone_vp_name", "plural_top_n_collapse", "category_top"})


def test_grounding_terms_skips_empty_and_non_dict_metadata() -> None:
    asi_by_qid = {"gs_026": {}, "gs_027": "garbage", "gs_028": {"blame_set": ["x"]}}
    assert grounding_terms_from_asi(asi_by_qid) == frozenset({"x"})


def test_intended_patch_shape_known_root_causes() -> None:
    assert (
        intended_patch_shape_for_root_cause(RcaKind.TOP_N_CARDINALITY_COLLAPSE)
        == "enforce_explicit_top_n_cardinality"
    )
    assert (
        intended_patch_shape_for_root_cause(RcaKind.MEASURE_SWAP)
        == "disambiguate_measure_with_contrastive_example"
    )
    assert (
        intended_patch_shape_for_root_cause(RcaKind.UNKNOWN)
        == "generic_judge_clarification"
    )


def test_allowed_and_forbidden_patch_families_are_complementary() -> None:
    allowed, forbidden = allowed_and_forbidden_patch_families(
        RcaKind.TOP_N_CARDINALITY_COLLAPSE
    )
    # Allowed should contain the canonical mapping for TOP_N collapse
    # (whatever patch_family_for_rca_kind returns for it).
    assert len(allowed) == 1
    # Forbidden should NOT include the allowed family
    assert allowed.isdisjoint(forbidden)
    # Forbidden should include something obviously wrong for top-N (defensive
    # filters often misfire as proposals against top-N issues).
    assert "avoid_unrequested_defensive_filters" in forbidden
