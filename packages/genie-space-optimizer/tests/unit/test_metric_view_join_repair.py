"""Unit tests for PR 26 — synthesis-side metric_view_join pre-check
+ CTE-first repair + LLM prompt anti-pattern hint.

Locks three contracts:

1. ``_check_metric_view_join_pre`` correctly classifies direct
   MV-on-anything joins as ``metric_view_join`` (and only those).
2. ``_repair_metric_view_join`` produces a CTE-first rewrite that
   round-trips through ``sqlglot.parse_one``.
3. ``_build_schema_contexts`` includes the no-direct-JOIN anti-pattern
   reminder when the run has metric views, and omits it otherwise.
"""

from __future__ import annotations

import re

import sqlglot

from genie_space_optimizer.optimization.evaluation import (
    _build_schema_contexts,
    _check_metric_view_join_pre,
    _repair_metric_view_join,
)


MV_MEASURES = {
    "mv_sales": {"total_sales", "avg_revenue"},
    "mv_returns": {"total_returns"},
}


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


# ── _check_metric_view_join_pre ────────────────────────────────────


class TestMetricViewJoinPreCheck:
    def test_direct_mv_on_dim_join_fires(self):
        sql = (
            "SELECT m.region, MEASURE(m.total_sales), d.region_name "
            "FROM cat.sch.mv_sales m "
            "JOIN cat.sch.dim_region d ON m.region = d.region_code"
        )
        assert _check_metric_view_join_pre(sql, {"mv_sales"}) == (
            "metric_view_join"
        )

    def test_direct_mv_on_mv_join_fires(self):
        sql = (
            "SELECT s.region, MEASURE(s.total_sales), MEASURE(r.total_returns) "
            "FROM cat.sch.mv_sales s "
            "JOIN cat.sch.mv_returns r ON s.region = r.region"
        )
        assert _check_metric_view_join_pre(
            sql, {"mv_sales", "mv_returns"},
        ) == "metric_view_join"

    def test_mv_with_cte_does_not_fire(self):
        # The LLM already emitted the documented CTE-first pattern;
        # the pre-check should NOT fire even though there's a JOIN
        # against the MV identifier syntactically.
        sql = (
            "WITH x AS (SELECT 1) "
            "SELECT * FROM cat.sch.mv_sales JOIN x ON 1=1"
        )
        assert _check_metric_view_join_pre(sql, {"mv_sales"}) is None

    def test_non_mv_non_mv_join_does_not_fire(self):
        sql = (
            "SELECT * FROM cat.sch.t1 "
            "JOIN cat.sch.t2 ON t1.id = t2.id"
        )
        assert _check_metric_view_join_pre(sql, {"mv_sales"}) is None

    def test_mv_query_without_join_does_not_fire(self):
        sql = "SELECT region, MEASURE(total_sales) FROM cat.sch.mv_sales"
        assert _check_metric_view_join_pre(sql, {"mv_sales"}) is None

    def test_empty_mv_set_returns_none(self):
        sql = "SELECT * FROM cat.sch.mv_sales JOIN d ON 1=1"
        assert _check_metric_view_join_pre(sql, set()) is None

    def test_unparseable_sql_returns_none(self):
        # sqlglot raises on empty / nonsense input — the pre-check
        # must swallow the error and return None.
        assert _check_metric_view_join_pre("not sql @#$", {"mv_sales"}) is None


# ── _repair_metric_view_join ──────────────────────────────────────


class TestMetricViewJoinRepair:
    def test_mv_dim_join_rewrites_to_cte(self):
        sql = (
            "SELECT m.region, MEASURE(m.total_sales), d.region_name "
            "FROM cat.sch.mv_sales m "
            "JOIN cat.sch.dim_region d ON m.region = d.region_code"
        )
        rewritten, n = _repair_metric_view_join(
            sql, {"mv_sales"}, MV_MEASURES,
        )
        assert n == 1
        assert "with __mv_1 as" in _norm(rewritten)
        # CTE materializes the measure with MEASURE().
        assert "measure(total_sales)" in _norm(rewritten)
        # Outer query references the CTE alias rather than the raw MV.
        assert "from __mv_1" in _norm(rewritten)
        # Round-trips through sqlglot — proves the rewrite emits valid SQL.
        sqlglot.parse_one(rewritten, read="databricks")

    def test_mv_mv_join_wraps_both(self):
        sql = (
            "SELECT s.region, MEASURE(s.total_sales), MEASURE(r.total_returns) "
            "FROM cat.sch.mv_sales s "
            "JOIN cat.sch.mv_returns r ON s.region = r.region"
        )
        rewritten, n = _repair_metric_view_join(
            sql, {"mv_sales", "mv_returns"}, MV_MEASURES,
        )
        assert n == 2
        rewritten_lower = _norm(rewritten)
        assert "with __mv_1" in rewritten_lower
        assert "__mv_2" in rewritten_lower
        sqlglot.parse_one(rewritten, read="databricks")

    def test_mv_with_existing_cte_is_left_alone(self):
        sql = (
            "WITH x AS (SELECT 1) "
            "SELECT * FROM cat.sch.mv_sales m JOIN x ON 1=1"
        )
        rewritten, n = _repair_metric_view_join(
            sql, {"mv_sales"}, MV_MEASURES,
        )
        assert n == 0
        assert rewritten == sql

    def test_no_join_returns_unchanged(self):
        sql = "SELECT region, MEASURE(total_sales) FROM cat.sch.mv_sales"
        rewritten, n = _repair_metric_view_join(
            sql, {"mv_sales"}, MV_MEASURES,
        )
        assert n == 0
        assert rewritten == sql

    def test_unparseable_sql_returns_unchanged(self):
        rewritten, n = _repair_metric_view_join(
            "not sql @#$", {"mv_sales"}, MV_MEASURES,
        )
        assert n == 0
        assert rewritten == "not sql @#$"

    def test_outer_measure_calls_flattened_to_cte_columns(self):
        """When the LLM wrote ``MEASURE(m.measure)`` in the outer
        query, the rewrite should flatten it to ``m.measure`` (the
        CTE has already materialized the measure under that name).
        """
        sql = (
            "SELECT m.region, MEASURE(m.total_sales) AS sales "
            "FROM cat.sch.mv_sales m "
            "JOIN cat.sch.dim_region d ON m.region = d.region_code"
        )
        rewritten, n = _repair_metric_view_join(
            sql, {"mv_sales"}, MV_MEASURES,
        )
        assert n == 1
        rewritten_lower = _norm(rewritten)
        # CTE has MEASURE(total_sales); outer query has m.total_sales,
        # not a duplicate MEASURE() call against the CTE.
        assert "select m.region, m.total_sales as sales" in rewritten_lower


# ── _build_schema_contexts MV anti-pattern hint ────────────────────


class TestSchemaContextsAntiPatternHint:
    def test_hint_present_when_mv_set_non_empty(self):
        config = {
            "_parsed_space": {
                "data_sources": {
                    "metric_views": [
                        {
                            "identifier": "cat.sch.mv_sales",
                            "column_configs": [
                                {
                                    "column_name": "total_sales",
                                    "column_type": "measure",
                                },
                                {
                                    "column_name": "region",
                                    "column_type": "dimension",
                                },
                            ],
                        }
                    ],
                },
            },
        }
        ctx = _build_schema_contexts(config, [], [])
        mv_ctx = ctx["metric_views_context"]
        assert "cat.sch.mv_sales" in mv_ctx
        assert "anti-pattern reminder" in mv_ctx.lower()
        assert "do not join metric views directly" in mv_ctx.lower()
        assert "metric_view_join_not_supported" in mv_ctx.lower()
        assert "with __mv_sales as" in mv_ctx.lower()

    def test_hint_omitted_when_no_metric_views(self):
        config = {"_parsed_space": {"data_sources": {}}}
        ctx = _build_schema_contexts(config, [], [])
        assert ctx["metric_views_context"] == "(none)"
        assert "anti-pattern reminder" not in (
            ctx["metric_views_context"].lower()
        )
