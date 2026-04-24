"""Regression tests for Phase B2 weighted dominant-root-cause selection.

Without weighting, the previous Counter-based ``most_common(1)`` tie-broke
arbitrarily. The Q004 regression pattern was: four SQL-shape judges voting
for ``missing_filter`` and one NL-text judge's rationale matching a
different pattern — the majority-vote call gave us ``missing_filter``
sometimes and something else other times depending on insertion order.

With B2, each judge's vote is weighted by its :mod:`judge_classes`
weight (SQL-shape=1.0, NL-text=0.1, etc.), so the SQL-shape quartet
reliably dominates. A single NL-text vote cannot override the stack.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import cluster_failures


def _row(question_id: str, judges_and_rationales: dict[str, str]) -> dict:
    """Build a single eval row with the feedback/{judge}/value + /rationale
    columns that ``cluster_failures`` looks for. A ``"no"`` value marks the
    judge as failed."""
    row: dict = {
        "question_id": question_id,
        "request": {"kwargs": {"question": f"q for {question_id}"}},
        "response": {"response": "SELECT * FROM foo"},
        "inputs/question_id": question_id,
    }
    for judge, rationale in judges_and_rationales.items():
        row[f"feedback/{judge}/value"] = "no"
        row[f"feedback/{judge}/rationale"] = rationale
    return row


def test_sql_shape_majority_beats_single_nl_text_vote() -> None:
    """Four SQL-shape judges vote missing_filter, one NL-text judge's
    rationale would resolve differently — weighted voting must pick
    missing_filter with weight ~= 4.0 regardless of dict ordering.
    """
    rows = [
        _row(
            "q1",
            {
                "completeness":         "The query is missing a filter on is_active",
                "schema_accuracy":      "Rows are returned without applying the required filter",
                "semantic_equivalence": "No WHERE clause filter on the year column",
                "logical_accuracy":     "The filter for is_active = true is missing",
                "response_quality":     "The summary misrepresents the aggregation",
            },
        ),
    ]
    clusters = cluster_failures({"rows": rows}, metadata_snapshot={})
    assert len(clusters) == 1, clusters
    assert clusters[0]["root_cause"] == "missing_filter"


def test_sql_shape_weight_shown_in_profile_cleanly() -> None:
    """Smoke test: weighted voting should still pick a single winner even
    when a lone NL-text judge votes against the SQL-shape plurality.
    The cluster assembly path should not crash on a one-judge row.
    """
    rows = [
        _row(
            "q2",
            {
                "response_quality": "The prose summary is misleading about the data",
            },
        ),
    ]
    clusters = cluster_failures({"rows": rows}, metadata_snapshot={})
    # Weight=0.1 for NL_TEXT judge; still the sole vote so it wins.
    assert len(clusters) == 1
    # Should not explode even when the only failing judge is NL_TEXT.


def test_sql_shape_judge_outvotes_nl_text_even_on_single_vote() -> None:
    """One SQL-shape judge (weight=1.0) beats one NL-text judge
    (weight=0.1) even when both cast a single vote for different causes.
    Prevents the NL-text cause from becoming dominant by having a
    slightly different rationale than the SQL-shape cause.
    """
    rows = [
        _row(
            "q3",
            {
                "schema_accuracy":  "Missing a filter on fiscal_year",
                "response_quality": "JOIN appears to be wrong between tables",
            },
        ),
    ]
    clusters = cluster_failures({"rows": rows}, metadata_snapshot={})
    assert len(clusters) == 1
    assert clusters[0]["root_cause"] == "missing_filter"
