"""Tests for Section E Tier 1 unmatched-pattern record emission."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import RCACard, RcaKind


def _card(cluster_id: str = "S001") -> RCACard:
    return RCACard(
        card_id=f"card_{cluster_id}",
        cluster_id=cluster_id,
        qids=("gs_002", "gs_004"),
        root_cause=RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING,
        grounding_terms=frozenset({"snack_brand", "beverage_brand"}),
        intended_patch_shape="entity_disambiguation",
        allowed_patch_families=frozenset(),
        forbidden_patch_families=frozenset(),
        rationale="t",
    )


def test_emit_unmatched_pattern_record_appends_record_to_run_state() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        emit_unmatched_pattern_record,
    )
    from genie_space_optimizer.optimization.archetype_learning_state import (
        get_state,
        reset_state,
    )

    reset_state("run_T1")
    rec = emit_unmatched_pattern_record(
        run_id="run_T1",
        card=_card(),
        cluster={"cluster_id": "S001", "asi_question_intent": "single",
                 "question_ids": ["gs_002", "gs_004"]},
    )
    state = get_state("run_T1")
    assert rec in state.unmatched_pattern_records
    assert rec.cluster_id == "S001"
    assert rec.root_cause_label == "SYNONYM_OR_ENTITY_MATCH_MISSING"
    assert rec.qids == ("gs_002", "gs_004")


def test_emit_unmatched_pattern_record_returns_existing_signature_for_duplicate_cluster() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        emit_unmatched_pattern_record,
    )
    from genie_space_optimizer.optimization.archetype_learning_state import reset_state

    reset_state("run_T1b")
    rec1 = emit_unmatched_pattern_record(
        run_id="run_T1b",
        card=_card("S001"),
        cluster={"cluster_id": "S001", "asi_question_intent": "single",
                 "question_ids": ["gs_002"]},
    )
    rec2 = emit_unmatched_pattern_record(
        run_id="run_T1b",
        card=_card("S002"),  # Same shape, different cluster
        cluster={"cluster_id": "S002", "asi_question_intent": "single",
                 "question_ids": ["gs_005"]},
    )
    assert rec1.signature_hash == rec2.signature_hash
    assert rec1.cluster_id != rec2.cluster_id
