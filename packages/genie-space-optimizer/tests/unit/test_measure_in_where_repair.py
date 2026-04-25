"""Unit tests for the PR 20 ``_repair_measure_in_where`` helper.

The helper rewrites SQL of the form::

    SELECT zone, MEASURE(total_sales) AS sales
    FROM mv_x
    WHERE store_day_count > 0
    GROUP BY zone

into the CTE-first pattern Spark accepts on metric views::

    WITH __mv_base AS (
      SELECT zone,
             MEASURE(total_sales) AS sales,
             MEASURE(store_day_count) AS store_day_count_value
      FROM mv_x
      GROUP BY zone
    )
    SELECT zone, sales
    FROM __mv_base
    WHERE store_day_count_value > 0

It is *conservative*: anything ambiguous (set-ops, subqueries, outer
JOINs, pre-existing CTEs) returns the original SQL unchanged so the LLM
correction loop can take another pass.
"""

from __future__ import annotations

import re

import pytest

from genie_space_optimizer.optimization.evaluation import (
    _classify_sql_validation_error,
    _repair_hint_for_reason,
    _repair_measure_in_where,
)


MV_MEASURES = {
    "mv_x": {"total_sales", "store_day_count"},
}


def _norm(s: str) -> str:
    """Lower-case + collapse whitespace; lets assertions stay tolerant of
    sqlglot's choice of formatting."""
    return re.sub(r"\s+", " ", s.strip().lower())


def test_single_measure_in_where_rewritten_to_cte():
    sql = (
        "SELECT zone_combination, MEASURE(total_sales) AS sales\n"
        "FROM mv_x\n"
        "WHERE store_day_count > 0\n"
        "GROUP BY zone_combination"
    )
    new_sql, lifts = _repair_measure_in_where(sql, MV_MEASURES)
    assert lifts == 1
    norm = _norm(new_sql)
    # CTE wraps the original query.
    assert "with __mv_base as" in norm
    # Inner exposes both the original measure alias and the lifted one.
    assert "measure(total_sales) as sales" in norm
    assert "measure(store_day_count) as store_day_count_value" in norm
    # Outer filters on the materialized alias (and ONLY on it).
    assert "where store_day_count_value > 0" in norm
    assert " store_day_count >" not in norm.split("from __mv_base")[-1]


def test_multi_measure_in_where_materialized_once_each():
    sql = (
        "SELECT zone, MEASURE(total_sales) AS sales\n"
        "FROM mv_x\n"
        "WHERE store_day_count > 0 AND total_sales > 100\n"
        "GROUP BY zone"
    )
    new_sql, lifts = _repair_measure_in_where(sql, MV_MEASURES)
    assert lifts == 2
    norm = _norm(new_sql)
    assert norm.count("measure(store_day_count) as store_day_count_value") == 1
    # ``total_sales`` was already projected as MEASURE(total_sales) AS sales,
    # so the helper still injects MEASURE(total_sales) AS total_sales_value
    # (the existing ``sales`` alias does not satisfy the WHERE rewrite).
    assert "measure(total_sales) as total_sales_value" in norm
    assert "store_day_count_value > 0" in norm
    assert "total_sales_value > 100" in norm


def test_no_measure_in_where_returned_unchanged():
    sql = (
        "SELECT zone, MEASURE(total_sales) AS sales\n"
        "FROM mv_x\n"
        "WHERE zone = 'A'\n"
        "GROUP BY zone"
    )
    new_sql, lifts = _repair_measure_in_where(sql, MV_MEASURES)
    assert lifts == 0
    assert new_sql == sql


def test_existing_with_clause_is_left_alone():
    sql = (
        "WITH base AS (SELECT * FROM other)\n"
        "SELECT zone FROM mv_x WHERE store_day_count > 0"
    )
    new_sql, lifts = _repair_measure_in_where(sql, MV_MEASURES)
    assert lifts == 0
    assert new_sql == sql


def test_outer_join_is_left_alone():
    sql = (
        "SELECT zone FROM mv_x JOIN dim_store USING (store_id) "
        "WHERE store_day_count > 0"
    )
    new_sql, lifts = _repair_measure_in_where(sql, MV_MEASURES)
    assert lifts == 0
    assert new_sql == sql


def test_set_op_root_is_left_alone():
    sql = (
        "SELECT zone FROM mv_x WHERE store_day_count > 0 "
        "UNION ALL SELECT zone FROM mv_x WHERE total_sales > 100"
    )
    new_sql, lifts = _repair_measure_in_where(sql, MV_MEASURES)
    assert lifts == 0
    assert new_sql == sql


def test_subquery_in_where_is_left_alone():
    sql = (
        "SELECT zone FROM mv_x "
        "WHERE store_day_count > 0 AND zone IN (SELECT zone FROM dim_zone)"
    )
    new_sql, lifts = _repair_measure_in_where(sql, MV_MEASURES)
    assert lifts == 0
    assert new_sql == sql


def test_empty_measures_short_circuits():
    sql = "SELECT zone FROM mv_x WHERE store_day_count > 0"
    new_sql, lifts = _repair_measure_in_where(sql, {})
    assert lifts == 0
    assert new_sql == sql


def test_rewrite_is_idempotent():
    sql = (
        "SELECT zone, MEASURE(total_sales) AS sales "
        "FROM mv_x WHERE store_day_count > 0 GROUP BY zone"
    )
    once, lifts1 = _repair_measure_in_where(sql, MV_MEASURES)
    twice, lifts2 = _repair_measure_in_where(once, MV_MEASURES)
    assert lifts1 == 1
    # After the first pass the WHERE references ``store_day_count_value``
    # which is *not* in the measures map (it's a CTE alias), so the
    # helper must not re-wrap into another nested CTE.
    assert lifts2 == 0
    assert twice == once


def test_qualified_measure_in_where_lifted():
    sql = (
        "SELECT zone, MEASURE(total_sales) AS sales "
        "FROM mv_x x WHERE x.store_day_count > 0 GROUP BY zone"
    )
    new_sql, lifts = _repair_measure_in_where(sql, MV_MEASURES)
    assert lifts == 1
    norm = _norm(new_sql)
    assert "store_day_count_value > 0" in norm
    # The qualifier was dropped because the alias now lives on the CTE.
    assert "x.store_day_count_value" not in norm


# ── Classifier + repair-hint integration ─────────────────────────────


@pytest.mark.parametrize(
    "spark_message",
    [
        "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] in WHERE clause",
        "Measure cannot appear in the WHERE clause - "
        "METRIC_VIEW_MISSING_MEASURE_FUNCTION",
        "Spark error: METRIC_VIEW_MISSING_MEASURE_FUNCTION measure used in HAVING clause",
        "METRIC_VIEW_MISSING_MEASURE_FUNCTION encountered in ON clause of join",
    ],
)
def test_classifier_recognises_measure_in_where(spark_message):
    assert _classify_sql_validation_error(spark_message) == "mv_measure_in_where"


def test_classifier_falls_through_to_missing_measure_for_select_only():
    msg = "[METRIC_VIEW_MISSING_MEASURE_FUNCTION] in SELECT projection"
    assert _classify_sql_validation_error(msg) == "mv_missing_measure_function"


def test_repair_hint_present_for_measure_in_where():
    hint = _repair_hint_for_reason("mv_measure_in_where")
    assert hint, "expected a non-empty hint for mv_measure_in_where"
    assert "CTE-first" in hint
    assert "MEASURE(" in hint
