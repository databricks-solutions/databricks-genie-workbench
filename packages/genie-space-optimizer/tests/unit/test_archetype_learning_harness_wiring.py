"""Helper-level integration tests for Section E harness wiring.

The wiring lives in harness.py inside flag-gated try/except blocks. The
production code calls a small pure helper for each tier so the harness
body need not be patched in test."""

from __future__ import annotations

from genie_space_optimizer.optimization.archetype_learning_state import (
    get_state,
    reset_state,
)
from genie_space_optimizer.optimization.rca import RCACard, RcaKind


def _card(cluster_id: str, root_cause: RcaKind = RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING) -> RCACard:
    return RCACard(
        card_id=f"c_{cluster_id}", cluster_id=cluster_id,
        qids=(f"gs_{cluster_id}",),
        root_cause=root_cause,
        grounding_terms=frozenset({"snack_brand"}),
        intended_patch_shape="entity_disambiguation",
        allowed_patch_families=frozenset(),
        forbidden_patch_families=frozenset(),
        rationale="t",
    )


def test_tier1_helper_emits_one_record_per_unmatched_cluster() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        emit_unmatched_pattern_records_for_unmatched_clusters,
    )

    reset_state("run_W1")
    clusters = [
        {"cluster_id": "S001", "rca_card": _card("S001"),
         "asi_question_intent": "single", "question_ids": ["gs_001"]},
        {"cluster_id": "S002", "rca_card": _card("S002"),
         "asi_question_intent": "single", "question_ids": ["gs_002"]},
        # No card → not unmatched, just absent. Skipped.
        {"cluster_id": "X", "rca_card": None, "question_ids": ["gs_x"]},
    ]
    for c in clusters:
        c["_repair_kit"] = None

    out = emit_unmatched_pattern_records_for_unmatched_clusters(
        run_id="run_W1", clusters=clusters,
    )
    assert len(out) == 2
    assert {r.cluster_id for r in out} == {"S001", "S002"}
    assert len(get_state("run_W1").unmatched_pattern_records) == 2


def test_tier1_helper_skips_clusters_with_a_repair_kit() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        emit_unmatched_pattern_records_for_unmatched_clusters,
    )

    reset_state("run_W2")
    clusters = [
        {"cluster_id": "H001", "rca_card": _card("H001"),
         "asi_question_intent": "plural", "question_ids": ["gs_026"],
         "_repair_kit": {"repair_archetype": "plural_top_n_collapse"}},
    ]
    out = emit_unmatched_pattern_records_for_unmatched_clusters(
        run_id="run_W2", clusters=clusters,
    )
    assert out == []
