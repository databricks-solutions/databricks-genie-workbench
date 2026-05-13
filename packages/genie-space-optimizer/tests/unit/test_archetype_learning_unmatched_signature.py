"""Tests for Section E Tier 1 signature computation."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import RCACard, RcaKind


def _card(
    *,
    cluster_id: str = "C1",
    root_cause: RcaKind = RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING,
    grounding: tuple[str, ...] = ("snack_brand", "beverage_brand"),
    intended: str = "entity_disambiguation",
) -> RCACard:
    return RCACard(
        card_id=f"card_{cluster_id}",
        cluster_id=cluster_id,
        qids=("gs_x",),
        root_cause=root_cause,
        grounding_terms=frozenset(grounding),
        intended_patch_shape=intended,
        allowed_patch_families=frozenset(),
        forbidden_patch_families=frozenset(),
        rationale="t",
    )


def test_signature_is_deterministic_for_same_card_and_intent() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        compute_unmatched_signature,
    )

    card = _card()
    sig1 = compute_unmatched_signature(card=card, asi_question_intent="single")
    sig2 = compute_unmatched_signature(card=card, asi_question_intent="single")
    assert sig1 == sig2
    assert isinstance(sig1, str)
    assert len(sig1) >= 8


def test_signature_changes_with_root_cause() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        compute_unmatched_signature,
    )

    sig_a = compute_unmatched_signature(
        card=_card(root_cause=RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING),
        asi_question_intent="single",
    )
    sig_b = compute_unmatched_signature(
        card=_card(root_cause=RcaKind.MEASURE_SWAP),
        asi_question_intent="single",
    )
    assert sig_a != sig_b


def test_signature_is_order_independent_in_grounding_terms() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        compute_unmatched_signature,
    )

    sig_a = compute_unmatched_signature(
        card=_card(grounding=("snack_brand", "beverage_brand")),
        asi_question_intent="single",
    )
    sig_b = compute_unmatched_signature(
        card=_card(grounding=("beverage_brand", "snack_brand")),
        asi_question_intent="single",
    )
    assert sig_a == sig_b


def test_signature_changes_with_intended_patch_shape() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        compute_unmatched_signature,
    )

    sig_a = compute_unmatched_signature(
        card=_card(intended="entity_disambiguation"),
        asi_question_intent="single",
    )
    sig_b = compute_unmatched_signature(
        card=_card(intended="metric_routing"),
        asi_question_intent="single",
    )
    assert sig_a != sig_b


def test_signature_changes_with_question_intent() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        compute_unmatched_signature,
    )

    sig_a = compute_unmatched_signature(card=_card(), asi_question_intent="single")
    sig_b = compute_unmatched_signature(card=_card(), asi_question_intent="plural")
    assert sig_a != sig_b
