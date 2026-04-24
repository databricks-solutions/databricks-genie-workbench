"""Unit tests for the Tier 1.4 unified hard-failure predicate.

``row_is_hard_failure`` is the single predicate shared by the accuracy
gate and clustering. Before Tier 1.4, clustering used the arbiter verdict
alone, so a row with ``rc=yes`` but ``arbiter=ground_truth_correct`` would
be counted correct by the gate but then fed into ``filtered_failure_rows``
by ``_analyze_and_distribute`` — producing phantom hard clusters even when
the accept gate saw 100 percent accuracy.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.evaluation import (
    classify_genie_shape_patterns,
    row_is_hard_failure,
)


# ─────────────────────────────────────────────────────────────────────
# row_is_hard_failure — Tier 1.4
# ─────────────────────────────────────────────────────────────────────


def test_rc_yes_and_arbiter_both_correct_is_not_hard():
    row = {"result_correctness/value": "yes", "arbiter/value": "both_correct"}
    assert row_is_hard_failure(row) is False


def test_rc_yes_and_arbiter_genie_correct_is_not_hard():
    row = {"result_correctness/value": "yes", "arbiter/value": "genie_correct"}
    assert row_is_hard_failure(row) is False


def test_rc_no_and_arbiter_both_correct_is_not_hard():
    """Arbiter override wins: rc=no + arbiter=both_correct stays soft."""
    row = {"result_correctness/value": "no", "arbiter/value": "both_correct"}
    assert row_is_hard_failure(row) is False


def test_rc_no_and_arbiter_genie_correct_is_not_hard():
    row = {"result_correctness/value": "no", "arbiter/value": "genie_correct"}
    assert row_is_hard_failure(row) is False


def test_rc_no_and_arbiter_ground_truth_correct_is_hard():
    row = {"result_correctness/value": "no", "arbiter/value": "ground_truth_correct"}
    assert row_is_hard_failure(row) is True


def test_rc_yes_and_arbiter_ground_truth_correct_is_not_hard():
    """This is the case that Tier 1.4 fixes: rc=yes overrides.

    Before the fix, clustering flagged this as a hard failure and produced
    a ghost cluster even though the accept gate counted the row correct.
    """
    row = {"result_correctness/value": "yes", "arbiter/value": "ground_truth_correct"}
    assert row_is_hard_failure(row) is False


def test_rc_no_and_arbiter_missing_is_hard():
    """Missing arbiter verdict defaults to non-correct — still a hard fail."""
    row = {"result_correctness/value": "no"}
    assert row_is_hard_failure(row) is True


def test_case_insensitive_rc_and_arbiter():
    row = {"result_correctness/value": "NO", "arbiter/value": "BOTH_CORRECT"}
    assert row_is_hard_failure(row) is False


def test_legacy_unsuffixed_keys_work():
    """Rows written without the /value suffix still classify correctly."""
    row = {"result_correctness": "no", "arbiter": "ground_truth_correct"}
    assert row_is_hard_failure(row) is True


# ─────────────────────────────────────────────────────────────────────
# classify_genie_shape_patterns — Tier 2.13 / 2.14
# ─────────────────────────────────────────────────────────────────────


def _row_with_comparison(
    gt_rows: int, genie_rows: int, genie_sql: str, expected_sql: str,
) -> dict:
    """Build the nested shape that ``classify_genie_shape_patterns`` reads."""
    return {
        "request": {"expected_sql": expected_sql},
        "response": {
            "response": genie_sql,
            "comparison": {
                "gt_row_count": gt_rows,
                "genie_row_count": genie_rows,
            },
        },
    }


def test_over_filtered_dimension_detects_spurious_isnull():
    """Genie added ``zone_combination IS NOT NULL`` unprompted — Q14/Q18."""
    genie_sql = (
        "SELECT SUM(amount) FROM fact WHERE zone_combination IS NOT NULL GROUP BY 1"
    )
    expected_sql = "SELECT SUM(amount) FROM fact GROUP BY 1"
    row = _row_with_comparison(5, 3, genie_sql, expected_sql)
    pattern = classify_genie_shape_patterns(row)
    assert pattern is not None
    assert pattern["failure_type"] == "over_filtered_dimension"
    assert pattern["wrong_clause"] == "WHERE"
    assert "zone_combination" in pattern["blame_set"]


def test_over_filtered_dimension_ignored_when_gt_has_isnull_too():
    """If GT has the same IS NOT NULL, Genie didn't add anything spurious."""
    genie_sql = (
        "SELECT SUM(amount) FROM fact WHERE zone_combination IS NOT NULL GROUP BY 1"
    )
    expected_sql = (
        "SELECT SUM(amount) FROM fact WHERE zone_combination IS NOT NULL GROUP BY 1"
    )
    row = _row_with_comparison(5, 3, genie_sql, expected_sql)
    assert classify_genie_shape_patterns(row) is None


def test_wide_vs_long_shape_detects_time_window_pivot():
    """Genie returned 2x rows with time_window column added — Q20."""
    genie_sql = (
        "SELECT region, time_window, SUM(amount) FROM fact GROUP BY ALL"
    )
    expected_sql = "SELECT region, SUM(amount) FROM fact GROUP BY 1"
    row = _row_with_comparison(8, 16, genie_sql, expected_sql)
    pattern = classify_genie_shape_patterns(row)
    assert pattern is not None
    assert pattern["failure_type"] == "wide_vs_long_shape"
    assert pattern["wrong_clause"] == "SELECT"
    assert "time_window" in pattern["blame_set"]


def test_returns_none_on_incomparable_row_counts():
    row = {
        "request": {"expected_sql": "SELECT 1"},
        "response": {"response": "SELECT 1", "comparison": {}},
    }
    assert classify_genie_shape_patterns(row) is None


def test_returns_none_on_matching_row_counts():
    """Equal row counts can't be over-filtered or pivoted — nothing to classify."""
    row = _row_with_comparison(5, 5, "SELECT 1", "SELECT 1")
    assert classify_genie_shape_patterns(row) is None
