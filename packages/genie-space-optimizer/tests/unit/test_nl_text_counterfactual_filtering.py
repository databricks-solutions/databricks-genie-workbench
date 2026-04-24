"""Regression tests for Phase B3 — NL-text counterfactual filtering.

When a cluster's dominant root cause is SQL-shape, the strategist should
not see counterfactuals sourced from NL_TEXT judges (like
``response_quality``). Those counterfactuals are typically about prose
("don't fabricate numbers") and drag the strategist toward Lever 5
instruction tweaks when the real fix is a Lever 6 snippet.

Per-judge rows are still preserved in ``question_traces`` so forensics
tools see the full picture — only the cluster-level
``asi_counterfactual_fixes`` summary is filtered.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import cluster_failures


def _row_with_counterfactuals(
    question_id: str,
    judges: dict[str, tuple[str, dict]],
) -> dict:
    """Build an eval row where each judge fails with a given rationale AND
    a judge-level ASI metadata dict (including counterfactual_fix)."""
    row: dict = {
        "question_id": question_id,
        "request": {"kwargs": {"question": f"q for {question_id}"}},
        "response": {"response": "SELECT * FROM foo"},
        "inputs/question_id": question_id,
    }
    for judge, (rationale, metadata) in judges.items():
        row[f"feedback/{judge}/value"] = "no"
        row[f"feedback/{judge}/rationale"] = rationale
        row[f"feedback/{judge}/metadata"] = metadata
    return row


def test_nl_text_counterfactual_dropped_when_cluster_is_sql_shape() -> None:
    """Four SQL-shape judges diagnose ``missing_filter``; one NL-text judge
    attaches a prose counterfactual. The cluster must:

    * have dominant root_cause = ``missing_filter``;
    * expose its SQL-shape counterfactuals in ``asi_counterfactual_fixes``;
    * suppress the NL-text counterfactual from the same list.

    The per-judge rows in ``question_traces`` are unchanged.
    """
    sql_shape_cf = {
        "failure_type": "missing_filter",
        "counterfactual_fix": "Add WHERE is_active = true",
    }
    nl_text_cf = {
        "failure_type": "other",
        "counterfactual_fix": (
            "Instruct the assistant to not fabricate numerical values in "
            "the natural language response."
        ),
    }
    row = _row_with_counterfactuals(
        "q1",
        {
            "completeness":         ("missing a filter on is_active", sql_shape_cf),
            "schema_accuracy":      ("missing filter", sql_shape_cf),
            "semantic_equivalence": ("filter missing", sql_shape_cf),
            "logical_accuracy":     ("the required filter is missing", sql_shape_cf),
            "response_quality":     ("summary is misleading", nl_text_cf),
        },
    )

    clusters = cluster_failures({"rows": [row]}, metadata_snapshot={})
    assert len(clusters) == 1
    c = clusters[0]
    assert c["root_cause"] == "missing_filter"

    summary_cfs = c["asi_counterfactual_fixes"]
    # SQL-shape counterfactual is present.
    assert any("is_active" in s for s in summary_cfs)
    # NL-text counterfactual is suppressed from the cluster summary.
    assert not any(
        "fabricate" in s.lower() or "natural language response" in s.lower()
        for s in summary_cfs
    ), summary_cfs

    # Per-judge trace still contains the NL-text counterfactual (forensics).
    failed = c["question_traces"][0]["failed_judges"]
    rq = next(j for j in failed if j["judge"] == "response_quality")
    assert "fabricate" in rq["counterfactual_fix"].lower()


def test_nl_text_counterfactual_kept_when_cluster_is_not_sql_shape() -> None:
    """When the cluster's dominant root cause is NOT SQL-shape, NL-text
    counterfactuals are preserved in the summary — they're the only
    signal we have.
    """
    routing_cf = {
        "failure_type": "asset_routing_error",
        "counterfactual_fix": "Route to the metric view instead of the base table",
    }
    nl_text_cf = {
        "failure_type": "other",
        "counterfactual_fix": "Do not fabricate specific numerical values",
    }
    row = _row_with_counterfactuals(
        "q2",
        {
            "asset_routing":    ("wrong asset picked", routing_cf),
            "response_quality": ("summary is misleading", nl_text_cf),
        },
    )
    clusters = cluster_failures({"rows": [row]}, metadata_snapshot={})
    assert len(clusters) == 1
    c = clusters[0]
    # Not in _SQL_SHAPE_ROOT_CAUSES — no filtering should apply.
    assert c["root_cause"] == "asset_routing_error"
    assert any("fabricate" in s.lower() for s in c["asi_counterfactual_fixes"])
