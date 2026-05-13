"""Tests for the harness-level Repair Planner wiring (Phase 2 Action 2.1).

The wiring is a small, flag-gated helper that walks each cluster, calls
``plan_repair`` when the cluster has an RCACard, and stamps the returned
kit dict onto ``cluster['_repair_kit']``. These tests exercise the helper
directly so the harness body need not be patched in test."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import RCACard, RcaKind


def _card() -> RCACard:
    return RCACard(
        card_id="card_H001",
        cluster_id="H001",
        qids=("gs_026",),
        root_cause=RcaKind.TOP_N_CARDINALITY_COLLAPSE,
        grounding_terms=frozenset({"rank_eq_1"}),
        intended_patch_shape="cardinality_preserving_top_n_guidance",
        allowed_patch_families=frozenset({"cardinality_preserving_top_n_guidance"}),
        forbidden_patch_families=frozenset(),
        rationale="test",
    )


def test_apply_repair_planner_to_clusters_stamps_kit_on_each_cluster_with_card() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        apply_repair_planner_to_clusters,
    )

    clusters = [
        {
            "cluster_id": "H001",
            "rca_card": _card(),
            "asi_question_intent": "plural",
            "question_ids": ["gs_026"],
        },
        {
            "cluster_id": "H002",
            "rca_card": None,
            "question_ids": ["gs_021"],
        },
    ]
    summary = apply_repair_planner_to_clusters(
        clusters=clusters,
        propagation_root_cause="unknown",
    )

    assert clusters[0]["_repair_kit"] is not None
    assert clusters[0]["_repair_kit"]["repair_archetype"] == "plural_top_n_collapse"
    assert clusters[1].get("_repair_kit") is None
    assert summary == {
        "classified": 1,
        "no_archetype_match": 0,
        "skipped_no_card": 1,
    }


def test_apply_repair_planner_to_clusters_records_no_archetype_match() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        apply_repair_planner_to_clusters,
    )

    card = RCACard(
        card_id="card_H_misc",
        cluster_id="H_misc",
        qids=("gs_x",),
        root_cause=RcaKind.UNKNOWN,
        grounding_terms=frozenset({"unrelated"}),
        intended_patch_shape="diagnostic",
        allowed_patch_families=frozenset(),
        forbidden_patch_families=frozenset(),
        rationale="test",
    )
    clusters = [
        {
            "cluster_id": "H_misc",
            "rca_card": card,
            "question_ids": ["gs_x"],
        },
    ]
    summary = apply_repair_planner_to_clusters(
        clusters=clusters,
        propagation_root_cause="unknown",
    )
    assert clusters[0].get("_repair_kit") is None
    assert summary == {
        "classified": 0,
        "no_archetype_match": 1,
        "skipped_no_card": 0,
    }


def test_apply_repair_planner_to_clusters_is_a_noop_when_clusters_list_empty() -> None:
    from genie_space_optimizer.optimization.repair_planner import (
        apply_repair_planner_to_clusters,
    )

    summary = apply_repair_planner_to_clusters(
        clusters=[], propagation_root_cause="unknown",
    )
    assert summary == {
        "classified": 0,
        "no_archetype_match": 0,
        "skipped_no_card": 0,
    }
