"""Phase 3 — decision-record emitter tests."""

from __future__ import annotations

from genie_space_optimizer.optimization.decision_emitters import (
    iteration_feedback_built_record,
    near_miss_ag_shape_decision_record,
    near_miss_reflection_record,
    soft_evidence_lifted_record,
    soft_signal_trend_report_record,
)


def test_iteration_feedback_built_emitter() -> None:
    rec = iteration_feedback_built_record(
        run_id="r1", iteration=1,
        acceptance_class="diagnostic_hold",
        target_qids=("gs_026",),
        reflection_count=1, tried_shape_count=2,
    )
    assert rec.reason_code == "iteration_feedback_built"
    assert rec.target_qids == ("gs_026",)
    assert "diagnostic_hold" in rec.expected_effect
    assert "reflections=1" in rec.expected_effect


def test_near_miss_reflection_emitter() -> None:
    rec = near_miss_reflection_record(
        run_id="r1", iteration=2, kind="diagnostic_hold",
        target_qids=("gs_026",),
        required_next_iter_change="either",
        prior_repair_archetype="default_time_window_filter",
        prior_target_scope="single_qid",
    )
    assert rec.reason_code == "near_miss_reflection_emitted"
    assert "default_time_window_filter" in rec.expected_effect
    assert "either" in rec.next_action


def test_near_miss_ag_shape_differs_emitter() -> None:
    rec = near_miss_ag_shape_decision_record(
        run_id="r1", iteration=2, ag_id="AG_001",
        differs=True,
        target_qids=("gs_026",),
        candidate_archetype="enforce_explicit_top_n_cardinality",
        candidate_scope="single_qid",
    )
    assert rec.reason_code == "near_miss_ag_shape_differs"
    assert "proceed to strategy" in rec.next_action.lower()


def test_near_miss_ag_shape_repeated_emitter() -> None:
    rec = near_miss_ag_shape_decision_record(
        run_id="r1", iteration=3, ag_id="AG_001",
        differs=False,
        target_qids=("gs_026",),
        candidate_archetype="default_time_window_filter",
        candidate_scope="single_qid",
        matched_prior_archetype="default_time_window_filter",
        matched_prior_scope="single_qid",
    )
    assert rec.reason_code == "near_miss_ag_shape_repeated"
    assert "repeats prior" in rec.next_action.lower()


def test_soft_evidence_lifted_emitter() -> None:
    rec = soft_evidence_lifted_record(
        run_id="r1", iteration=1, kit_count=2, soft_qid_count=11,
    )
    assert rec.reason_code == "soft_evidence_lifted_to_kit"
    assert "kits_with_soft_evidence=2" in rec.expected_effect
    assert "soft_qid_count_total=11" in rec.expected_effect


def test_soft_signal_trend_report_emitter() -> None:
    rec = soft_signal_trend_report_record(
        run_id="r1", iteration=10,
        total_soft_clusters=5, matched_count=2, unmatched_count=3,
        top_unmatched_root_cause="filter_logic_mismatch",
    )
    assert rec.reason_code == "soft_signal_trend_report"
    assert "matched=2" in rec.expected_effect
    assert "unmatched=3" in rec.expected_effect
    assert "filter_logic_mismatch" in rec.expected_effect
