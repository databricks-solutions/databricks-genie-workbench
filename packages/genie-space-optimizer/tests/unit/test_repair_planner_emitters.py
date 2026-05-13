"""Tests for Section A repair-planner decision-record emitters."""

from __future__ import annotations


def test_cluster_archetype_classified_record_carries_archetype_name_and_priority_step() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        cluster_archetype_classified_record,
    )

    rec = cluster_archetype_classified_record(
        run_id="run_1",
        iteration=1,
        cluster_id="H001",
        card_id="card_H001",
        archetype_name="plural_top_n_collapse",
        priority_step="repair_kit",
        target_qids=("gs_026",),
        propagation_root_cause="unknown",
    )
    assert rec.reason_code == "cluster_archetype_classified"
    assert rec.cluster_id == "H001"
    assert "plural_top_n_collapse" in rec.next_action
    assert rec.target_qids == ("gs_026",)


def test_repair_planner_no_archetype_match_record_carries_card_id_and_root_cause() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        repair_planner_no_archetype_match_record,
    )

    rec = repair_planner_no_archetype_match_record(
        run_id="run_1",
        iteration=2,
        cluster_id="H_misc",
        card_id="card_H_misc",
        root_cause="unknown",
        target_qids=("gs_x",),
    )
    assert rec.reason_code == "repair_planner_no_archetype_match"
    assert "card_H_misc" in rec.evidence_refs[0]
    assert rec.target_qids == ("gs_x",)


def test_repair_plan_propagation_guarded_record_carries_propagation_value() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        repair_plan_propagation_guarded_record,
    )

    rec = repair_plan_propagation_guarded_record(
        run_id="run_1",
        iteration=1,
        cluster_id="H001",
        archetype_name="plural_top_n_collapse",
        propagation_root_cause="instruction_insufficient_force",
        guard_action="require_narrow_l6_snippet",
        target_qids=("gs_026",),
    )
    assert rec.reason_code == "repair_plan_propagation_guarded"
    assert "instruction_insufficient_force" in rec.next_action
    assert "require_narrow_l6_snippet" in rec.next_action
