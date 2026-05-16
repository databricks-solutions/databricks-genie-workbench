"""Unit tests for `_dispatch_lever_5b_for_cluster` routing matrix.

Covers four cases:
  - flag OFF + SQL-shape cluster → lean path (unchanged).
  - flag OFF + non-SQL-shape cluster → lean path (unchanged).
  - flag ON + SQL-shape cluster → rich path (NEW behaviour).
  - flag ON + non-SQL-shape cluster → lean path (rich path NOT
    invoked).
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock


def _sql_shape_cluster() -> dict:
    return {
        "cluster_id": "C_SQL",
        "root_cause": "plural_top_n_collapse",
        "asi_failure_type": "wrong_aggregation",
        "question_ids": ["q1"],
    }


def _non_sql_shape_cluster() -> dict:
    return {
        "cluster_id": "C_NSQ",
        "root_cause": "ambiguous_question",
        "asi_failure_type": "ambiguity",
        "question_ids": ["q1"],
    }


def test_flag_off_sql_shape_routes_lean(monkeypatch: Any) -> None:
    """Lean path called; rich path NOT called; no decline ledger entry."""
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "0")
    from genie_space_optimizer.optimization import optimizer, synthesis
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()

    lean_mock = MagicMock(return_value={
        "example_question": "Q_lean",
        "example_sql": "SELECT 1",
        "parameters": [],
        "usage_guidance": "G",
    })
    monkeypatch.setattr(synthesis, "synthesize_example_sqls", lean_mock)

    out = optimizer._dispatch_lever_5b_for_cluster(
        cluster=_sql_shape_cluster(),
        metadata_snapshot={"_space_id": "test"},
        w=None,
        benchmark_corpus=None,
    )
    assert lean_mock.call_count == 1
    assert out == [{
        "example_question": "Q_lean",
        "example_sql": "SELECT 1",
        "parameters": [],
        "usage_guidance": "G",
    }]
    assert drain_l5b_rich_path_declines() == []


def test_flag_off_non_sql_shape_routes_lean(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "0")
    from genie_space_optimizer.optimization import optimizer, synthesis

    lean_mock = MagicMock(return_value={
        "example_question": "Q",
        "example_sql": "SELECT 1",
        "parameters": [],
        "usage_guidance": "G",
    })
    monkeypatch.setattr(synthesis, "synthesize_example_sqls", lean_mock)

    out = optimizer._dispatch_lever_5b_for_cluster(
        cluster=_non_sql_shape_cluster(),
        metadata_snapshot={"_space_id": "test"},
        w=None,
        benchmark_corpus=None,
    )
    assert lean_mock.call_count == 1
    assert len(out) == 1


def test_flag_on_sql_shape_routes_rich(monkeypatch: Any) -> None:
    """Rich path called; lean path NOT called; output is the
    normalized rich proposal."""
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization import optimizer, synthesis
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()

    lean_mock = MagicMock(side_effect=AssertionError(
        "lean path MUST NOT be called when flag on + SQL-shape"
    ))
    monkeypatch.setattr(synthesis, "synthesize_example_sqls", lean_mock)

    rich_mock = MagicMock(return_value=ClusterSynthesisResult(
        proposal={
            "example_question": "Q_rich",
            "example_sql": "SELECT 1",
            "parameters": [],
            "usage_guidance": "G_rich",
        },
        attempted_archetypes=("ordered_list_by_metric",),
        skipped_reason=None,
    ))
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.cluster_driven_synthesis."
        "run_cluster_driven_synthesis_for_single_cluster",
        rich_mock,
    )

    out = optimizer._dispatch_lever_5b_for_cluster(
        cluster=_sql_shape_cluster(),
        metadata_snapshot={"_space_id": "test"},
        w=None,
        benchmark_corpus=None,
        benchmarks=[{"qid": "q1", "expected": "..."}],
    )
    assert lean_mock.call_count == 0
    assert rich_mock.call_count == 1
    assert out == [{
        "example_question": "Q_rich",
        "example_sql": "SELECT 1",
        "parameters": [],
        "usage_guidance": "G_rich",
    }]
    # Rich-path benchmarks kwarg threaded through.
    _, kwargs = rich_mock.call_args
    assert kwargs["benchmarks"] == [{"qid": "q1", "expected": "..."}]
    assert drain_l5b_rich_path_declines() == []


def test_flag_on_non_sql_shape_routes_lean(monkeypatch: Any) -> None:
    """Even with the flag on, a cluster whose failure label is NOT
    SQL-shape still uses the lean path (the rich path is reserved for
    structural failures)."""
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization import optimizer, synthesis

    rich_mock = MagicMock(side_effect=AssertionError(
        "rich path MUST NOT be called for non-SQL-shape clusters"
    ))
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.cluster_driven_synthesis."
        "run_cluster_driven_synthesis_for_single_cluster",
        rich_mock,
    )

    lean_mock = MagicMock(return_value={
        "example_question": "Q",
        "example_sql": "SELECT 1",
        "parameters": [],
        "usage_guidance": "G",
    })
    monkeypatch.setattr(synthesis, "synthesize_example_sqls", lean_mock)

    out = optimizer._dispatch_lever_5b_for_cluster(
        cluster=_non_sql_shape_cluster(),
        metadata_snapshot={"_space_id": "test"},
        w=None,
        benchmark_corpus=None,
    )
    assert rich_mock.call_count == 0
    assert lean_mock.call_count == 1
    assert len(out) == 1


def test_flag_on_rich_path_decline_appends_to_ledger(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()

    rich_mock = MagicMock(return_value=ClusterSynthesisResult(
        proposal=None,
        attempted_archetypes=("single_row_top_n", "ordered_list_by_metric"),
        skipped_reason="no_viable_archetype",
    ))
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.cluster_driven_synthesis."
        "run_cluster_driven_synthesis_for_single_cluster",
        rich_mock,
    )

    out = optimizer._dispatch_lever_5b_for_cluster(
        cluster=_sql_shape_cluster(),
        metadata_snapshot={"_space_id": "test"},
        w=None,
        benchmark_corpus=None,
    )
    assert out == []
    declines = drain_l5b_rich_path_declines()
    assert len(declines) == 1
    rec = declines[0]
    assert rec["cluster_id"] == "C_SQL"
    assert rec["attempted_archetypes"] == (
        "single_row_top_n", "ordered_list_by_metric",
    )
    assert rec["skipped_reason"] == "no_viable_archetype"
