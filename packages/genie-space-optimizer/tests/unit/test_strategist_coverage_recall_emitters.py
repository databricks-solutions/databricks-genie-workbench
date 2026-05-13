"""Tests for Section D strategist-recall decision-record emitters."""

from __future__ import annotations


def test_strategist_coverage_recall_invoked_record_carries_uncovered_cluster_count() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_coverage_recall_invoked_record,
    )

    rec = strategist_coverage_recall_invoked_record(
        run_id="run_1",
        iteration=1,
        uncovered_cluster_ids=("H002", "H003"),
        eligible_cluster_count=2,
    )
    assert rec.reason_code == "strategist_coverage_recall_invoked"
    assert "H002" in rec.next_action
    assert "H003" in rec.next_action


def test_strategist_coverage_recall_result_record_carries_returned_ag_count() -> None:
    from genie_space_optimizer.optimization.decision_emitters import (
        strategist_coverage_recall_result_record,
    )

    rec = strategist_coverage_recall_result_record(
        run_id="run_1",
        iteration=1,
        uncovered_cluster_ids=("H002", "H003"),
        recall_returned_ag_count=1,
        recall_succeeded=True,
    )
    assert rec.reason_code == "strategist_coverage_recall_result"
    assert "ag_count=1" in rec.expected_effect
    assert "succeeded" in rec.expected_effect.lower()
