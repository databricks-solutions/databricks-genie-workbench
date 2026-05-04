"""Cycle 2 Task 4 — per-question lever preference.

Single-question clusters with question-shape root causes (top-N
collapse, count-vs-distinct, etc.) should not get space-wide lever-6
SQL-expression patches. The strategist should prefer per-question
levers (3 example_sql, 5 instructions). Reproducer for run
2afb0be2-88b6-4832-99aa-c7e78fbc90f7 iter 2 AG_COVERAGE_H003 (gs_009
top-N).
"""
from __future__ import annotations


def test_recommended_levers_for_single_question_top_n(monkeypatch) -> None:
    monkeypatch.setenv("GSO_QUESTION_SHAPE_LEVER_PREFERENCE", "1")
    from genie_space_optimizer.optimization.stages.action_groups import (
        recommended_levers_for_cluster,
    )

    cluster = {
        "cluster_id": "H003",
        "question_ids": ["gs_009"],
        "q_count": 1,
        "root_cause": "plural_top_n_collapse",
    }
    levers = recommended_levers_for_cluster(cluster)
    assert 6 not in levers, (
        "lever 6 (SQL Expressions, space-wide) must not be recommended "
        "for a single-question top-N cluster"
    )
    assert 3 in levers or 5 in levers


def test_recommended_levers_unchanged_for_multi_question_cluster(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_QUESTION_SHAPE_LEVER_PREFERENCE", "1")
    from genie_space_optimizer.optimization.stages.action_groups import (
        recommended_levers_for_cluster,
    )

    cluster = {
        "cluster_id": "H_MULTI",
        "question_ids": ["gs_009", "gs_012", "gs_029"],
        "q_count": 3,
        "root_cause": "plural_top_n_collapse",
    }
    levers = recommended_levers_for_cluster(cluster)
    # Multi-question clusters can still get lever 6.
    assert 6 in levers


def test_recommended_levers_unchanged_for_non_question_shape_root_cause(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_QUESTION_SHAPE_LEVER_PREFERENCE", "1")
    from genie_space_optimizer.optimization.stages.action_groups import (
        recommended_levers_for_cluster,
    )

    cluster = {
        "cluster_id": "H001",
        "question_ids": ["gs_024"],
        "q_count": 1,
        "root_cause": "wrong_aggregation",  # NOT a question-shape RCA
    }
    levers = recommended_levers_for_cluster(cluster)
    # wrong_aggregation can be space-wide; preference does not fire.
    assert 6 in levers


def test_flag_off_returns_default_levers(monkeypatch) -> None:
    monkeypatch.setenv("GSO_QUESTION_SHAPE_LEVER_PREFERENCE", "0")
    from genie_space_optimizer.optimization.stages.action_groups import (
        recommended_levers_for_cluster,
    )

    cluster = {
        "cluster_id": "H003",
        "question_ids": ["gs_009"],
        "q_count": 1,
        "root_cause": "plural_top_n_collapse",
    }
    levers = recommended_levers_for_cluster(cluster)
    # With the flag off, today's behavior is preserved (includes 6).
    assert 6 in levers
