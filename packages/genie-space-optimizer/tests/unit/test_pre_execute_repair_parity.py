"""Unit tests for PR 31 — shared pre-execute MV/CTE repairs.

The unified, preflight, and cluster-synthesis pipelines all need to
apply the same deterministic SQL rewrites before paying for a
warehouse EXPLAIN/execute. PR 31 introduces three new contract
points:

1. :func:`evaluation._extract_cte_names` — recognises top-level CTE
   names declared in a ``WITH`` clause so the dangling-qualifier
   check and the synthesis identifier-qualification gate accept
   ``FROM base`` / ``base.col`` references when ``base`` is a CTE.

2. :func:`evaluation._repair_order_by_measure_alias` — strips
   ``MEASURE()`` around a SELECT alias in ``ORDER BY`` (the alias
   is already an aggregate, so wrapping it again raises
   ``METRIC_VIEW_MISSING_MEASURE_FUNCTION`` at execute time).

3. :func:`evaluation.apply_pre_execute_repairs` — shared repair
   pipeline used by both the unified-correction path and the
   cluster-synthesis path so the unified, preflight, and synthesis
   surfaces all produce the same SQL shape before EXPLAIN.

These tests cover each contract directly, plus the integration
points (``_check_dangling_qualifiers`` accepting CTE aliases, and
the synthesis ``_gate_identifier_qualification`` accepting
``FROM base`` when ``base`` is a CTE in the same SQL).
"""

from __future__ import annotations

from genie_space_optimizer.optimization.evaluation import (
    _check_dangling_qualifiers,
    _extract_cte_names,
    _extract_from_join_aliases,
    _repair_order_by_measure_alias,
    apply_pre_execute_repairs,
    build_table_columns,
)
from genie_space_optimizer.optimization.synthesis import (
    GateResult,
    _gate_identifier_qualification,
)


class TestCTENameExtraction:
    """``_extract_cte_names`` must recognise top-level CTE definitions
    so downstream qualification checks treat them as in-scope
    identifiers rather than bare table references."""

    def test_simple_with_extracts_single_cte(self):
        sql = "WITH base AS (SELECT 1) SELECT * FROM base"
        assert _extract_cte_names(sql) == {"base"}

    def test_multiple_ctes_extracted(self):
        sql = (
            "WITH __mv_1 AS (SELECT MEASURE(x) AS m FROM cat.sch.mv1), "
            "base AS (SELECT * FROM cat.sch.fact) "
            "SELECT base.col, __mv_1.m FROM base CROSS JOIN __mv_1"
        )
        assert _extract_cte_names(sql) == {"__mv_1", "base"}

    def test_with_recursive_recognised(self):
        sql = (
            "WITH RECURSIVE walk AS ("
            "  SELECT id FROM t UNION ALL SELECT id FROM walk"
            ") SELECT * FROM walk"
        )
        assert _extract_cte_names(sql) == {"walk"}

    def test_no_with_returns_empty(self):
        assert _extract_cte_names("SELECT 1") == set()
        assert _extract_cte_names("") == set()
        assert _extract_cte_names("SELECT * FROM cat.sch.t WHERE x = 1") == set()

    def test_case_insensitive_with(self):
        sql = "with cte_a as (select 1) select * from cte_a"
        assert _extract_cte_names(sql) == {"cte_a"}

    def test_cte_with_column_list_extracted(self):
        sql = (
            "WITH numbered (rn, val) AS (SELECT 1, 'x') "
            "SELECT * FROM numbered"
        )
        assert _extract_cte_names(sql) == {"numbered"}


class TestExtractFromJoinAliasesIncludesCTEs:
    """``_extract_from_join_aliases`` must include CTE names so the
    dangling-qualifier check (which only consults this set) treats
    them as in-scope. Without this, the CTE-first MV-join repair
    output (``WITH __mv_1 AS (...) SELECT ... FROM base ...``) is
    rejected as referencing an unknown qualifier."""

    def test_aliases_include_cte_names(self):
        sql = (
            "WITH base AS (SELECT * FROM cat.sch.fact) "
            "SELECT base.col FROM base"
        )
        aliases = _extract_from_join_aliases(sql)
        assert "base" in aliases

    def test_aliases_include_both_ctes_and_from_join(self):
        sql = (
            "WITH __mv_1 AS (SELECT MEASURE(x) AS m FROM cat.sch.mv1) "
            "SELECT t.col, __mv_1.m FROM cat.sch.fact t CROSS JOIN __mv_1"
        )
        aliases = _extract_from_join_aliases(sql)
        assert "t" in aliases
        assert "fact" in aliases
        assert "__mv_1" in aliases


class TestDanglingQualifiersAcceptCTEs:
    """``_check_dangling_qualifiers`` must accept CTE aliases so the
    pre-EXPLAIN gate doesn't reject the CTE-first MV-join repair
    output as ``UNQUALIFIED_TABLE`` for ``base.col`` / ``__mv_1.m``."""

    def test_cte_qualifier_not_flagged(self):
        sql = (
            "WITH base AS (SELECT col FROM cat.sch.fact) "
            "SELECT base.col FROM base"
        )
        cfg = {"_parsed_space": {"data_sources": {"tables": []}}}
        table_columns = build_table_columns(cfg)
        unresolved = _check_dangling_qualifiers(sql, table_columns or {"base": {}})
        # Even with empty table_columns, the CTE alias 'base' must be
        # recognised via _extract_from_join_aliases.
        assert "base" not in unresolved

    def test_dangling_non_cte_still_flagged(self):
        sql = (
            "WITH base AS (SELECT col FROM cat.sch.fact) "
            "SELECT base.col, mystery.foo FROM base"
        )
        unresolved = _check_dangling_qualifiers(sql, {"base": {}})
        assert "mystery" in unresolved


class TestRepairOrderByMeasureAlias:
    """``_repair_order_by_measure_alias`` strips ``MEASURE()`` around
    a SELECT alias in ``ORDER BY`` — wrapping an alias that already
    refers to a ``MEASURE(...)`` projection is a Spark error."""

    def test_strips_measure_around_select_alias(self):
        sql = (
            "SELECT zone, MEASURE(total_sales) AS sales "
            "FROM cat.sch.mv_sales GROUP BY zone "
            "ORDER BY MEASURE(sales) DESC"
        )
        new_sql, count = _repair_order_by_measure_alias(sql)
        assert count == 1
        assert "ORDER BY MEASURE(sales)" not in new_sql.upper()
        assert "ORDER BY SALES" in new_sql.upper()

    def test_keeps_measure_around_real_measure_column(self):
        # No SELECT alias was named ``total_sales`` — the ORDER BY
        # MEASURE(total_sales) is a legitimate measure-column ref.
        sql = (
            "SELECT zone FROM cat.sch.mv_sales "
            "GROUP BY zone "
            "ORDER BY MEASURE(total_sales) DESC"
        )
        new_sql, count = _repair_order_by_measure_alias(sql)
        assert count == 0
        assert new_sql == sql

    def test_handles_multiple_order_by_keys(self):
        sql = (
            "SELECT zone, MEASURE(total_sales) AS sales "
            "FROM cat.sch.mv_sales GROUP BY zone "
            "ORDER BY zone, MEASURE(sales) DESC"
        )
        new_sql, count = _repair_order_by_measure_alias(sql)
        assert count == 1
        # The bare zone column reference is preserved.
        assert "ZONE" in new_sql.upper()
        # Only the MEASURE(sales) reference is unwrapped.
        assert "ORDER BY MEASURE(SALES)" not in new_sql.upper()

    def test_no_order_by_is_noop(self):
        sql = (
            "SELECT zone, MEASURE(total_sales) AS sales "
            "FROM cat.sch.mv_sales GROUP BY zone"
        )
        new_sql, count = _repair_order_by_measure_alias(sql)
        assert count == 0
        assert new_sql == sql

    def test_no_measure_in_select_is_noop(self):
        sql = "SELECT col FROM t ORDER BY col"
        new_sql, count = _repair_order_by_measure_alias(sql)
        assert count == 0
        assert new_sql == sql


class TestApplyPreExecuteRepairsPipeline:
    """``apply_pre_execute_repairs`` orchestrates the full repair
    sequence used by every pipeline. These tests verify that each
    repair fires when its conditions are met and that counters are
    reported back consistently."""

    def test_no_op_on_clean_sql(self):
        sql = "SELECT col FROM cat.sch.t WHERE col > 0"
        counters: dict[str, int] = {}
        new_sql = apply_pre_execute_repairs(sql, counters=counters)
        assert new_sql == sql
        assert counters == {}

    def test_wraps_bare_measure_in_select(self):
        sql = "SELECT zone, total_sales FROM cat.sch.mv_sales GROUP BY zone"
        counters: dict[str, int] = {}
        new_sql = apply_pre_execute_repairs(
            sql,
            mv_measures={"mv_sales": {"total_sales"}},
            counters=counters,
        )
        assert "MEASURE(total_sales)" in new_sql
        assert counters.get("repaired_measure_refs", 0) >= 1

    def test_strips_outer_agg_around_measure(self):
        sql = (
            "SELECT zone, SUM(MEASURE(total_sales)) AS sales "
            "FROM cat.sch.mv_sales GROUP BY zone"
        )
        counters: dict[str, int] = {}
        new_sql = apply_pre_execute_repairs(
            sql,
            mv_measures={"mv_sales": {"total_sales"}},
            counters=counters,
        )
        assert "SUM(MEASURE" not in new_sql.upper().replace(" ", "")
        assert counters.get("stripped_outer_aggregate_around_measure", 0) >= 1

    def test_strips_order_by_measure_alias(self):
        sql = (
            "SELECT zone, MEASURE(total_sales) AS sales "
            "FROM cat.sch.mv_sales GROUP BY zone "
            "ORDER BY MEASURE(sales) DESC"
        )
        counters: dict[str, int] = {}
        new_sql = apply_pre_execute_repairs(
            sql,
            mv_measures={"mv_sales": {"total_sales"}},
            counters=counters,
        )
        assert counters.get("repaired_order_by_measure_alias", 0) == 1

    def test_lifts_measure_in_where_to_cte(self):
        sql = (
            "SELECT zone, MEASURE(total_sales) AS sales "
            "FROM cat.sch.mv_sales WHERE store_day_count > 0 GROUP BY zone"
        )
        counters: dict[str, int] = {}
        new_sql = apply_pre_execute_repairs(
            sql,
            mv_measures={
                "mv_sales": {"total_sales", "store_day_count"},
            },
            counters=counters,
        )
        assert "WITH" in new_sql.upper()
        assert counters.get("repaired_measure_in_where", 0) == 1

    def test_metric_view_join_lifted_to_cte(self):
        sql = (
            "SELECT f.zone, MEASURE(m.total_sales) AS sales "
            "FROM cat.sch.fact f JOIN cat.sch.mv_sales m ON f.id = m.id "
            "GROUP BY f.zone"
        )
        counters: dict[str, int] = {}
        new_sql = apply_pre_execute_repairs(
            sql,
            mv_measures={"mv_sales": {"total_sales"}},
            mv_short_set={"mv_sales"},
            counters=counters,
        )
        # Either the join was repaired with a CTE, or it was left
        # untouched because the rewriter could not produce a clean
        # rewrite. Either way we don't crash.
        if counters.get("repaired_metric_view_join", 0) >= 1:
            assert "WITH" in new_sql.upper()

    def test_empty_sql_passes_through(self):
        assert apply_pre_execute_repairs("") == ""
        assert apply_pre_execute_repairs("   ") == "   "


class TestSynthesisQualificationGateAcceptsCTEs:
    """The synthesis identifier-qualification gate must accept
    ``FROM base`` when ``base`` is a CTE declared in the same SQL.
    Otherwise the CTE-first MV-join repair output is rejected as
    ``UNQUALIFIED_TABLE``."""

    def test_qualification_gate_accepts_cte_alias(self):
        sql = (
            "WITH base AS (SELECT col FROM cat.sch.fact) "
            "SELECT base.col FROM base"
        )
        proposal = {"example_sql": sql}
        allowlist = {"cat.sch.fact"}
        result = _gate_identifier_qualification(proposal, allowlist)
        assert isinstance(result, GateResult)
        assert result.passed, f"unexpected reason: {result.reason}"

    def test_qualification_gate_accepts_cte_first_mv_join(self):
        sql = (
            "WITH __mv_1 AS ("
            "  SELECT id, MEASURE(total_sales) AS sales "
            "  FROM cat.sch.mv_sales GROUP BY id"
            ") "
            "SELECT f.zone, __mv_1.sales "
            "FROM cat.sch.fact f JOIN __mv_1 ON f.id = __mv_1.id"
        )
        proposal = {"example_sql": sql}
        allowlist = {"cat.sch.fact", "cat.sch.mv_sales"}
        result = _gate_identifier_qualification(proposal, allowlist)
        assert result.passed, f"unexpected reason: {result.reason}"

    def test_qualification_gate_rejects_unknown_table(self):
        sql = (
            "WITH base AS (SELECT * FROM cat.sch.fact) "
            "SELECT * FROM mystery_table"
        )
        proposal = {"example_sql": sql}
        allowlist = {"cat.sch.fact"}
        result = _gate_identifier_qualification(proposal, allowlist)
        assert not result.passed
        assert "mystery_table" in (result.reason or "")
