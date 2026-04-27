"""Tests for the pure column-confusion analyzer.

The column-confusion analyzer compares an expected vs generated SQL
pair and surfaces typed evidence when the generated SQL substituted
one column for a similar-looking column from the same table. The
existing ``column_disambiguation`` detector requires a shared prefix
of at least 5 characters and only inspects projected/filtered columns,
which misses real-world abbreviation pairs (``is_month_to_date`` vs
``use_mtdate_flag``) and swaps inside ``WHERE``, ``GROUP BY``, and
``MEASURE(...)``.

The analyzer is the building block of the regression-mining loop:
acceptance is unchanged, but failed iterations leave a structured
``ColumnConfusion`` insight that downstream layers can persist and
optionally feed back to the strategist.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import (
    ColumnConfusion,
    detect_column_confusion,
)


# ── Shared-prefix swap (legacy column_disambiguation case) ───────────


def test_detects_shared_prefix_swap_in_where_clause():
    expected = (
        "SELECT full_date FROM dim_date "
        "WHERE is_one_day_prior_year_same_day = 'Y'"
    )
    generated = (
        "SELECT full_date FROM dim_date "
        "WHERE is_month_to_date_prior_year_same_day = 'Y'"
    )

    insights = detect_column_confusion(expected, generated)

    assert any(
        c.intended_column == "is_one_day_prior_year_same_day"
        and c.confused_column == "is_month_to_date_prior_year_same_day"
        and c.sql_clause == "where"
        for c in insights
    ), insights


# ── Abbreviation / subtoken match (the retail flaky case) ────────────


def test_detects_month_to_date_vs_mtdate_abbreviation():
    """is_month_to_date vs use_mtdate_flag — tokens [month, to, date]
    vs token ``mtdate``. Plain shared-prefix matching never catches
    this; the analyzer must accept abbreviation/subtoken evidence."""

    expected = (
        "SELECT full_date FROM dim_date "
        "WHERE is_month_to_date = 'Y'"
    )
    generated = (
        "SELECT full_date FROM dim_date "
        "WHERE use_mtdate_flag = 'Y'"
    )

    insights = detect_column_confusion(expected, generated)

    assert any(
        c.intended_column == "is_month_to_date"
        and c.confused_column == "use_mtdate_flag"
        and c.sql_clause == "where"
        for c in insights
    ), insights


def test_detects_subtoken_overlap_in_select_clause():
    expected = "SELECT order_total_amount FROM fact_orders"
    generated = "SELECT order_amount FROM fact_orders"

    insights = detect_column_confusion(expected, generated)

    assert any(
        c.intended_column == "order_total_amount"
        and c.confused_column == "order_amount"
        and c.sql_clause == "select"
        for c in insights
    ), insights


# ── GROUP BY swap ────────────────────────────────────────────────────


def test_detects_swap_in_group_by_clause():
    expected = (
        "SELECT region_code, COUNT(*) FROM fact_orders "
        "GROUP BY region_code"
    )
    generated = (
        "SELECT region_id, COUNT(*) FROM fact_orders "
        "GROUP BY region_id"
    )

    insights = detect_column_confusion(expected, generated)

    # Insights may surface in either select or group_by; group_by is
    # the more specific signal for "wrong key" so we accept either.
    assert any(
        c.intended_column == "region_code"
        and c.confused_column == "region_id"
        and c.sql_clause in {"group_by", "select"}
        for c in insights
    ), insights


# ── MEASURE(...) swap (metric-view shape) ────────────────────────────


def test_detects_swap_inside_measure_function():
    expected = (
        "SELECT MEASURE(`net_revenue`) FROM mv_sales"
    )
    generated = (
        "SELECT MEASURE(`gross_revenue`) FROM mv_sales"
    )

    insights = detect_column_confusion(expected, generated)

    assert any(
        c.intended_column == "net_revenue"
        and c.confused_column == "gross_revenue"
        and c.sql_clause in {"measure", "select"}
        for c in insights
    ), insights


# ── Negative cases ───────────────────────────────────────────────────


def test_returns_empty_when_columns_match():
    expected = "SELECT full_date FROM dim_date WHERE is_month_to_date = 'Y'"
    generated = "SELECT full_date FROM dim_date WHERE is_month_to_date = 'Y'"

    assert detect_column_confusion(expected, generated) == []


def test_returns_empty_for_unrelated_columns():
    """Columns that share no prefix and no token overlap are not a
    confusion pair — the analyzer must not invent fake evidence."""

    expected = "SELECT customer_id FROM fact_orders"
    generated = "SELECT order_total FROM fact_orders"

    assert detect_column_confusion(expected, generated) == []


def test_returns_empty_when_either_sql_missing():
    assert detect_column_confusion("", "SELECT a FROM t") == []
    assert detect_column_confusion("SELECT a FROM t", "") == []
    assert detect_column_confusion("", "") == []


# ── Structural ──────────────────────────────────────────────────────


def test_insights_carry_confidence_and_rationale():
    expected = (
        "SELECT full_date FROM dim_date "
        "WHERE is_month_to_date = 'Y'"
    )
    generated = (
        "SELECT full_date FROM dim_date "
        "WHERE use_mtdate_flag = 'Y'"
    )

    insights = detect_column_confusion(expected, generated)

    assert insights, "expected at least one ColumnConfusion"
    for ins in insights:
        assert isinstance(ins, ColumnConfusion)
        assert 0.0 < ins.confidence <= 1.0
        assert ins.rationale  # non-empty
        assert ins.intended_column != ins.confused_column


def test_table_is_extracted_when_present():
    expected = (
        "SELECT full_date FROM main.retail.dim_date "
        "WHERE is_month_to_date = 'Y'"
    )
    generated = (
        "SELECT full_date FROM main.retail.dim_date "
        "WHERE use_mtdate_flag = 'Y'"
    )

    insights = detect_column_confusion(expected, generated)

    assert insights, insights
    # We accept either the bare table name or the qualified name; the
    # important contract is that *some* table info is captured.
    assert any(
        ins.table and ("dim_date" in ins.table.lower())
        for ins in insights
    ), insights
