"""Tests for Section E decision-record emitters."""

from __future__ import annotations


def test_unmatched_pattern_record_emitter_carries_signature_and_cluster() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        unmatched_pattern_record_emitted_record,
    )

    rec = unmatched_pattern_record_emitted_record(
        run_id="r", iteration=1,
        cluster_id="S001", signature_hash="abc12345",
        root_cause_label="SYNONYM_OR_ENTITY_MATCH_MISSING",
        grounding_terms=("snack_brand", "beverage_brand"),
        target_qids=("gs_002", "gs_004"),
    )
    assert rec.reason_code == "unmatched_pattern_record_emitted"
    assert "abc12345" in rec.expected_effect
    assert "S001" in rec.evidence_refs[0]


def test_pattern_candidate_detected_record_carries_member_count_and_qids() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        pattern_candidate_detected_record,
    )

    rec = pattern_candidate_detected_record(
        run_id="r", iteration=2,
        signature_hash="abc12345",
        member_count=11,
        member_cluster_ids=("S001", "S002", "S003"),
        union_qids=("gs_002", "gs_004", "gs_010"),
        root_cause_label="SYNONYM_OR_ENTITY_MATCH_MISSING",
    )
    assert rec.reason_code == "pattern_candidate_detected"
    assert "member_count=11" in rec.expected_effect


def test_provisional_archetype_synthesized_record_carries_archetype_name() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        provisional_archetype_synthesized_record,
    )

    rec = provisional_archetype_synthesized_record(
        run_id="r", iteration=2,
        signature_hash="abc12345",
        archetype_name="brand_dimension_disambiguation_provisional",
        default_priority_step="repair_kit",
        applicable_rca_kinds=("SYNONYM_OR_ENTITY_MATCH_MISSING",),
        target_qids=("gs_002", "gs_004"),
    )
    assert rec.reason_code == "provisional_archetype_synthesized"
    assert "brand_dimension_disambiguation_provisional" in rec.next_action


def test_provisional_archetype_synthesis_declined_record_carries_reason() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        provisional_archetype_synthesis_declined_record,
    )

    rec = provisional_archetype_synthesis_declined_record(
        run_id="r", iteration=2,
        signature_hash="abc12345",
        decline_reason="counterfactuals_did_not_converge",
    )
    assert rec.reason_code == "provisional_archetype_synthesis_declined"
    assert "counterfactuals_did_not_converge" in rec.expected_effect


def test_provisional_archetype_trial_outcome_record_carries_tier_and_lifecycle() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        provisional_archetype_trial_outcome_record,
    )

    rec = provisional_archetype_trial_outcome_record(
        run_id="r", iteration=3,
        signature_hash="abc12345",
        archetype_name="brand_dimension_disambiguation_provisional",
        acceptance_tier="strict_win",
        new_lifecycle_state="confirmed_in_run",
        target_qids=("gs_002",),
    )
    assert rec.reason_code == "provisional_archetype_trial_outcome"
    assert "strict_win" in rec.expected_effect
    assert "confirmed_in_run" in rec.expected_effect


def test_confirmed_in_run_archetype_promoted_record_carries_archetype_name() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        confirmed_in_run_archetype_promoted_record,
    )

    rec = confirmed_in_run_archetype_promoted_record(
        run_id="r", iteration=3,
        signature_hash="abc12345",
        archetype_name="brand_dimension_disambiguation_provisional",
        first_promotion_iteration=3,
    )
    assert rec.reason_code == "confirmed_in_run_archetype_promoted"
    assert "brand_dimension_disambiguation_provisional" in rec.next_action


def test_cross_run_promotion_candidate_record_carries_archetype_name() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        cross_run_promotion_candidate_recorded_record,
    )

    rec = cross_run_promotion_candidate_recorded_record(
        run_id="r", iteration=3,
        signature_hash="abc12345",
        archetype_name="brand_dimension_disambiguation_provisional",
        confirming_iterations=(3,),
        union_qids=("gs_002", "gs_004", "gs_010"),
    )
    assert rec.reason_code == "cross_run_promotion_candidate_recorded"
    assert "brand_dimension_disambiguation_provisional" in rec.next_action
