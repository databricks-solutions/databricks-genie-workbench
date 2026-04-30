from __future__ import annotations

import json

from genie_space_optimizer.optimization.afs import format_afs
from genie_space_optimizer.optimization.feature_mining import (
    DiffKind,
    compute_diff,
    mine_sql_features,
)
from genie_space_optimizer.optimization.optimizer import (
    _build_context_data,
    _format_cluster_briefs_afs,
    cluster_failures,
)


GT_SQL = """
SELECT store_id, date_key_2, SUM(total_txn_cnt) AS total_txn_cnt
FROM cat.sch.retail_transactions
GROUP BY store_id, date_key_2
""".strip()

GENIE_SQL = """
SELECT store_id, SUM(total_txn_cnt) AS total_txn_cnt
FROM cat.sch.retail_transactions
GROUP BY store_id
""".strip()


def _metadata_snapshot() -> dict:
    return {
        "_data_profile": {},
        "instructions": {
            "text_instructions": [],
            "join_specs": [],
            "sql_snippets": [],
            "example_question_sqls": [],
        },
        "data_sources": {"tables": [], "metric_views": []},
    }


def _row(question_suffix: str) -> dict:
    feature_diff = compute_diff(
        genie=mine_sql_features(GENIE_SQL),
        ground_truth=mine_sql_features(GT_SQL),
    )
    assert feature_diff.primary_kind is DiffKind.MISSING_GROUPBY_COL

    return {
        "question_id": "q011",
        "request": {
            "question": f"Show total transactions by store and date {question_suffix}",
            "expected_sql": GT_SQL,
        },
        "response": {"response": GENIE_SQL, "comparison": {"match": False}},
        "feedback/logical_accuracy/value": "no",
        "feedback/logical_accuracy/rationale": (
            "The generated SQL misses date_key_2 in SELECT/GROUP BY."
        ),
        "feedback/logical_accuracy/metadata": {
            "failure_type": "wrong_grouping",
            "blame_set": ["cat.sch.retail_transactions.date_key_2"],
            "wrong_clause": "GROUP_BY",
            "counterfactual_fix": "Group by date_key_2.",
        },
        # This is the exact row-level handoff Phase 0 protects.
        # harness.py currently stamps the tuple as (generated_sql, expected_sql);
        # cluster_failures must normalize it into AFS' expected dict shape.
        "_sql_pairs_for_ast_diff": (GENIE_SQL, GT_SQL),
        "_feature_diff": feature_diff,
    }


def test_cluster_failures_threads_ast_diff_to_afs_and_rendered_brief() -> None:
    """Row-level SQL pairs must survive cluster formation and reach the prompt text."""
    rows = [_row("A"), _row("B")]

    clusters = cluster_failures(
        {"rows": rows},
        _metadata_snapshot(),
        verbose=False,
    )

    assert clusters, "expected at least one cluster"
    cluster = clusters[0]

    assert cluster.get("_sql_pairs_for_ast_diff"), (
        "cluster_failures must copy row-level SQL pairs onto the cluster"
    )
    assert cluster["_sql_pairs_for_ast_diff"][0]["expected_sql"] == GT_SQL
    assert cluster["_sql_pairs_for_ast_diff"][0]["generated_sql"] == GENIE_SQL

    afs = format_afs(cluster)
    structural_diff = afs["structural_diff"]
    assert "ast_diff" in structural_diff

    ast_diff_text = json.dumps(structural_diff["ast_diff"], sort_keys=True)
    assert "date_key_2" in ast_diff_text
    assert "aggregation_shape_diff" in ast_diff_text or "wrong_columns" in ast_diff_text

    rendered = _format_cluster_briefs_afs([cluster])
    assert "Structural signature:" in rendered
    assert "date_key_2" in rendered
    assert "aggregation_shape_diff" in rendered or "wrong_columns" in rendered


def test_build_context_data_surfaces_typed_failure_features_primary_kind() -> None:
    """The strategist JSON context must expose a compact typed diff token."""
    clusters = cluster_failures(
        {"rows": [_row("A"), _row("B")]},
        _metadata_snapshot(),
        verbose=False,
    )

    context = _build_context_data(
        clusters=clusters,
        soft_signal_clusters=[],
        metadata_snapshot=_metadata_snapshot(),
        reflection_buffer=[],
        priority_ranking=[],
        blame_set=None,
        success_summary="20 of 22 benchmarks pass.",
        reflection_text="",
        persistence_text="",
        proven_patterns_text="",
        suggestions_text="",
    )

    assert context["failure_features"], "expected typed failure_features block"
    first = context["failure_features"][0]
    assert first["cluster_id"] == clusters[0]["cluster_id"]
    assert first["primary_kind"] == "missing_groupby_col"
    assert first["candidate_levers"] == [4, 1]

    feature_text = json.dumps(first, sort_keys=True)
    assert "SELECT" not in feature_text.upper()
    assert "cat.sch.retail_transactions" not in feature_text

    rendered_context = json.dumps(context, sort_keys=True)
    assert "failure_features" in rendered_context
    assert "missing_groupby_col" in rendered_context
