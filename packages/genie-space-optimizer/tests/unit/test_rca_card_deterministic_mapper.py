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
