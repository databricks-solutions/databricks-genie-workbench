"""Tests for Section E dataclasses and run-scoped state."""

from __future__ import annotations


def test_unmatched_pattern_record_carries_signature_and_evidence() -> None:
    from genie_space_optimizer.optimization.archetype_learning import (
        UnmatchedPatternRecord,
    )

    rec = UnmatchedPatternRecord(
        signature_hash="abc123",
        cluster_id="S001",
        root_cause_label="SYNONYM_OR_ENTITY_MATCH_MISSING",
        grounding_terms=frozenset({"snack_brand", "beverage_brand"}),
        intended_patch_shape="entity_disambiguation",
        asi_question_intent="single",
        qids=("gs_002", "gs_004"),
    )
    assert rec.signature_hash == "abc123"
    assert "snack_brand" in rec.grounding_terms


def test_pattern_candidate_carries_signature_member_count_and_qid_union() -> None:
    from genie_space_optimizer.optimization.archetype_learning import PatternCandidate

    cand = PatternCandidate(
        signature_hash="abc123",
        member_cluster_ids=("S001", "H_other_1", "H_other_2"),
        union_qids=("gs_002", "gs_004", "gs_010"),
        root_cause_label="SYNONYM_OR_ENTITY_MATCH_MISSING",
        grounding_terms=frozenset({"snack_brand", "beverage_brand"}),
        intended_patch_shape="entity_disambiguation",
        asi_question_intent="single",
        member_count=3,
    )
    assert cand.member_count == 3
    assert "gs_010" in cand.union_qids


def test_provisional_archetype_extends_repair_archetype_with_provenance_and_lifecycle() -> None:
    from genie_space_optimizer.optimization.archetype_learning import ProvisionalArchetype
    from genie_space_optimizer.optimization.rca import RcaKind

    pa = ProvisionalArchetype(
        name="brand_dimension_disambiguation_provisional",
        applicable_rca_kinds=frozenset({RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING}),
        required_grounding_tokens=frozenset({"snack_brand", "beverage_brand"}),
        evidence_predicates=frozenset(),
        default_priority_step="repair_kit",
        expected_causal_effect_template=(
            "Add column-level synonyms for brand-axis dimensions; "
            "rewrites WHERE clauses to use canonical column."
        ),
        rationale="provisional — synthesised from pattern_candidate sig=abc123",
        provenance="provisional_archetype",
        lifecycle_state="provisional",
        signature_hash="abc123",
        synthesis_iteration=1,
    )
    assert pa.provenance == "provisional_archetype"
    assert pa.lifecycle_state == "provisional"


def test_archetype_learning_run_state_is_isolated_per_run_id() -> None:
    from genie_space_optimizer.optimization.archetype_learning_state import (
        ArchetypeLearningRunState,
        get_state,
        reset_state,
    )

    reset_state("run_A")
    reset_state("run_B")
    state_a = get_state("run_A")
    state_b = get_state("run_B")
    state_a.unmatched_pattern_records.append("dummy")  # type: ignore[arg-type]
    assert state_b.unmatched_pattern_records == []
    assert isinstance(state_a, ArchetypeLearningRunState)


def test_archetype_learning_run_state_reset_clears_all_collections() -> None:
    from genie_space_optimizer.optimization.archetype_learning_state import (
        get_state,
        reset_state,
    )

    state = get_state("run_C")
    state.unmatched_pattern_records.append("dummy")  # type: ignore[arg-type]
    state.pattern_candidates.append("dummy")  # type: ignore[arg-type]
    state.provisional_archetypes.append("dummy")  # type: ignore[arg-type]
    reset_state("run_C")
    fresh = get_state("run_C")
    assert fresh.unmatched_pattern_records == []
    assert fresh.pattern_candidates == []
    assert fresh.provisional_archetypes == []
