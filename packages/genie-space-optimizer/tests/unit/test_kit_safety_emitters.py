"""Tests for Section B kit-safety decision-record emitters."""

from __future__ import annotations


def test_kit_safety_summary_built_record_carries_kit_id_and_risk_class() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        kit_safety_summary_built_record,
    )

    rec = kit_safety_summary_built_record(
        run_id="run_1",
        iteration=1,
        ag_id="AG_001",
        kit_id="kit_xyz",
        repair_archetype="plural_top_n_collapse",
        target_qids=("gs_026",),
        risk_class="medium",
        union_passing_dependents_count=4,
    )
    assert rec.reason_code == "kit_safety_summary_built"
    assert rec.target_qids == ("gs_026",)
    assert "medium" in rec.expected_effect
    assert "kit_xyz" in rec.evidence_refs[0]


def test_kit_level_gate_rejected_record_carries_rejection_reason() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        kit_level_gate_rejected_record,
    )

    rec = kit_level_gate_rejected_record(
        run_id="run_1",
        iteration=1,
        ag_id="AG_001",
        kit_id="kit_xyz",
        rejection_reason="union_passing_dependents_exceeds_threshold",
        target_qids=("gs_026",),
        risk_class="high",
    )
    assert rec.reason_code == "kit_level_gate_rejected"
    assert "union_passing_dependents_exceeds_threshold" in rec.next_action
    assert rec.target_qids == ("gs_026",)


def test_repair_kit_no_safe_variant_available_record_carries_kit_attempts() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        repair_kit_no_safe_variant_available_record,
    )

    rec = repair_kit_no_safe_variant_available_record(
        run_id="run_1",
        iteration=1,
        ag_id="AG_001",
        cluster_id="H001",
        repair_archetype="plural_top_n_collapse",
        attempts=2,
        target_qids=("gs_026",),
    )
    assert rec.reason_code == "repair_kit_no_safe_variant_available"
    assert "attempts=2" in rec.next_action


def test_kit_atomicity_violation_record_carries_dropped_member_count() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        kit_atomicity_violation_record,
    )

    rec = kit_atomicity_violation_record(
        run_id="run_1",
        iteration=1,
        ag_id="AG_001",
        kit_id="kit_xyz",
        kept_count=2,
        total_count=5,
        target_qids=("gs_026",),
    )
    assert rec.reason_code == "kit_atomicity_violation"
    assert "kept=2" in rec.expected_effect
    assert "total=5" in rec.expected_effect
