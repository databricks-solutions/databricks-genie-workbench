"""Unit tests for dangling-qualifier detection in SQL repair.

Covers Fix 3b from the *Lever Loop Iteration 3 Fixes* plan: a
deterministic ``_check_dangling_qualifiers`` helper that rejects SQL
where ``<qual>.<col>`` references a qualifier that isn't a FROM/JOIN
table, an explicit alias, or a struct column on any FROM/JOIN table.

The helper short-circuits the EXPLAIN budget — the failing run had 34
candidates fail with ``UNRESOLVED_COLUMN: dim_date`` after the LLM
analogised ``dim_location.region`` (a real struct field) onto
``dim_date.year`` (a separate metric view that must be JOINed). The
fix turns those into a high-signal ``unresolved_qualifier`` rejection
that the strategist sees on the next loop.

Cases under test:
  * Reject: ``SELECT dim_date.year FROM mv_*_store_sales`` with no join.
  * Pass: same SQL with explicit ``JOIN mv_dim_date dim_date ON …``.
  * Pass: ``SELECT dim_location.region`` when ``dim_location`` is a
    struct column on the FROM table.
  * Pass: ``SELECT t.amount`` when ``t`` is the FROM table's short name
    and ``amount`` is a regular (non-struct) column.
  * Pass: catalog.schema.table inside FROM is not flagged (the catalog
    chunk is not a column qualifier).
  * No-op: empty SQL / empty table_columns map.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.evaluation import (
    _check_dangling_qualifiers,
    build_table_columns,
)


# ─────────────────────────────────────────────────────────────────────
# Fixtures: minimal Genie config shapes
# ─────────────────────────────────────────────────────────────────────


def _config(*tables: dict, metric_views: tuple[dict, ...] = ()) -> dict:
    """Wrap a list of tables in the ``_parsed_space`` envelope."""
    return {
        "_parsed_space": {
            "data_sources": {
                "tables": list(tables),
                "metric_views": list(metric_views),
            },
        },
    }


def _mv_sales_with_struct() -> dict:
    return {
        "identifier": "cat.sch.mv_store_sales",
        "name": "mv_store_sales",
        "column_configs": [
            {"column_name": "cy_sales", "data_type": "decimal(18,2)"},
            {
                "column_name": "dim_location",
                "data_type": "struct<region:string,city:string>",
            },
        ],
    }


def _mv_dim_date() -> dict:
    return {
        "identifier": "cat.sch.mv_dim_date",
        "name": "mv_dim_date",
        "column_configs": [
            {"column_name": "year", "data_type": "int"},
            {"column_name": "month", "data_type": "int"},
        ],
    }


# ─────────────────────────────────────────────────────────────────────
# Reject path — the headline regression
# ─────────────────────────────────────────────────────────────────────


def test_dangling_qualifier_rejected_when_table_not_joined() -> None:
    """``dim_date`` is a separate MV; without a JOIN the qualifier
    resolves to nothing.

    This is the exact failure mode from the lever loop run — 34 of 40
    bad candidates carried this exact shape.
    """
    config = _config(metric_views=(
        _mv_sales_with_struct(), _mv_dim_date(),
    ))
    table_columns = build_table_columns(config)

    sql = (
        "SELECT dim_date.year, SUM(MEASURE(cy_sales)) AS sales "
        "FROM cat.sch.mv_store_sales "
        "GROUP BY ALL"
    )
    unresolved = _check_dangling_qualifiers(sql, table_columns)

    assert "dim_date" in unresolved, (
        f"expected dim_date flagged as unresolved; got {unresolved!r}"
    )


def test_dangling_qualifier_rejected_in_where_clause() -> None:
    """Coverage of WHERE-position references (not just SELECT)."""
    config = _config(metric_views=(_mv_sales_with_struct(),))
    table_columns = build_table_columns(config)

    sql = (
        "SELECT MEASURE(cy_sales) FROM cat.sch.mv_store_sales "
        "WHERE dim_date.year = 2026"
    )
    unresolved = _check_dangling_qualifiers(sql, table_columns)
    assert "dim_date" in unresolved


# ─────────────────────────────────────────────────────────────────────
# Pass paths — same shape, but qualifier IS in scope
# ─────────────────────────────────────────────────────────────────────


def test_join_with_alias_resolves_qualifier() -> None:
    """``JOIN mv_dim_date dim_date ON …`` brings the qualifier into
    scope as an explicit alias."""
    config = _config(metric_views=(
        _mv_sales_with_struct(), _mv_dim_date(),
    ))
    table_columns = build_table_columns(config)

    sql = (
        "SELECT dim_date.year, SUM(MEASURE(s.cy_sales)) AS sales "
        "FROM cat.sch.mv_store_sales s "
        "JOIN cat.sch.mv_dim_date dim_date ON s.date_id = dim_date.date_id "
        "GROUP BY ALL"
    )
    assert _check_dangling_qualifiers(sql, table_columns) == []


def test_join_without_alias_uses_short_name() -> None:
    """``JOIN cat.sch.mv_dim_date`` (no alias) resolves via the table's
    short name ``mv_dim_date``."""
    config = _config(metric_views=(
        _mv_sales_with_struct(), _mv_dim_date(),
    ))
    table_columns = build_table_columns(config)

    sql = (
        "SELECT mv_dim_date.year FROM cat.sch.mv_store_sales "
        "JOIN cat.sch.mv_dim_date ON mv_store_sales.date_id = "
        "mv_dim_date.date_id"
    )
    assert _check_dangling_qualifiers(sql, table_columns) == []


def test_struct_column_qualifier_resolves() -> None:
    """``dim_location.region`` is a nested-field reference: the column
    ``dim_location`` is a struct on the FROM table.

    Without struct-column awareness this would be a false positive
    (the qualifier ``dim_location`` isn't a table or alias).
    """
    config = _config(metric_views=(_mv_sales_with_struct(),))
    table_columns = build_table_columns(config)

    sql = (
        "SELECT dim_location.region, SUM(MEASURE(cy_sales)) AS sales "
        "FROM cat.sch.mv_store_sales GROUP BY ALL"
    )
    assert _check_dangling_qualifiers(sql, table_columns) == []


def test_table_short_name_qualifier_resolves() -> None:
    config = _config({
        "identifier": "cat.sch.t_orders",
        "name": "t_orders",
        "column_configs": [
            {"column_name": "amount", "data_type": "decimal(18,2)"},
        ],
    })
    table_columns = build_table_columns(config)

    sql = "SELECT t_orders.amount FROM cat.sch.t_orders"
    assert _check_dangling_qualifiers(sql, table_columns) == []


def test_explicit_alias_resolves() -> None:
    config = _config({
        "identifier": "cat.sch.t_orders",
        "name": "t_orders",
        "column_configs": [
            {"column_name": "amount", "data_type": "decimal(18,2)"},
        ],
    })
    table_columns = build_table_columns(config)

    sql = "SELECT o.amount FROM cat.sch.t_orders AS o"
    assert _check_dangling_qualifiers(sql, table_columns) == []


# ─────────────────────────────────────────────────────────────────────
# False-positive guards
# ─────────────────────────────────────────────────────────────────────


def test_catalog_schema_table_in_from_clause_not_flagged() -> None:
    """``cat.sch.t_orders`` inside FROM is not a column qualifier.

    The helper strips FROM/JOIN clause heads before scanning so the
    catalog component (``cat``) doesn't generate a false positive.
    """
    config = _config({
        "identifier": "cat.sch.t_orders",
        "name": "t_orders",
        "column_configs": [
            {"column_name": "amount", "data_type": "decimal(18,2)"},
        ],
    })
    table_columns = build_table_columns(config)
    sql = "SELECT amount FROM cat.sch.t_orders"
    assert _check_dangling_qualifiers(sql, table_columns) == []


def test_empty_sql_returns_empty_list() -> None:
    config = _config({
        "identifier": "cat.sch.t",
        "name": "t",
        "column_configs": [{"column_name": "x", "data_type": "int"}],
    })
    table_columns = build_table_columns(config)
    assert _check_dangling_qualifiers("", table_columns) == []
    assert _check_dangling_qualifiers("   ", table_columns) == []


def test_empty_table_columns_returns_empty_list() -> None:
    """No table index → can't check qualifiers, return [] (caller
    decides what to do)."""
    sql = "SELECT dim_date.year FROM cat.sch.mv_x"
    assert _check_dangling_qualifiers(sql, {}) == []


def test_no_from_clause_returns_empty_list() -> None:
    """Subquery / DDL fragments with no FROM/JOIN have no aliases to
    resolve; we don't try to flag anything (saves false positives on
    CTE-heads or VALUES literals)."""
    config = _config({
        "identifier": "cat.sch.t",
        "name": "t",
        "column_configs": [{"column_name": "x", "data_type": "int"}],
    })
    table_columns = build_table_columns(config)
    assert _check_dangling_qualifiers(
        "SELECT 1 AS one", table_columns,
    ) == []


# ─────────────────────────────────────────────────────────────────────
# build_table_columns — sanity
# ─────────────────────────────────────────────────────────────────────


class TestBuildTableColumns:
    def test_struct_columns_indexed_separately(self) -> None:
        config = _config(metric_views=(_mv_sales_with_struct(),))
        idx = build_table_columns(config)
        info = idx["mv_store_sales"]
        assert "cy_sales" in info["columns"]
        assert "dim_location" in info["columns"]
        assert "dim_location" in info["struct_columns"]
        assert "cy_sales" not in info["struct_columns"]

    def test_tables_and_metric_views_both_indexed(self) -> None:
        config = _config(
            {
                "identifier": "cat.sch.t_a",
                "name": "t_a",
                "column_configs": [{"column_name": "x", "data_type": "int"}],
            },
            metric_views=(_mv_dim_date(),),
        )
        idx = build_table_columns(config)
        assert "t_a" in idx
        assert "mv_dim_date" in idx

    def test_short_name_is_lowercased(self) -> None:
        config = _config({
            "identifier": "CAT.Sch.T_Caps",
            "name": "T_Caps",
            "column_configs": [
                {"column_name": "X", "data_type": "int"},
            ],
        })
        idx = build_table_columns(config)
        assert "t_caps" in idx
        assert "x" in idx["t_caps"]["columns"]


# ─────────────────────────────────────────────────────────────────────
# Preflight integration (proposal-side hook)
# ─────────────────────────────────────────────────────────────────────


def test_preflight_synthesis_rejects_unresolved_qualifier(monkeypatch) -> None:
    """``synthesize_preflight_candidate`` short-circuits before EXPLAIN
    when the proposal carries a dangling qualifier.

    Confirms Fix 3b's wiring on the synthesis path so the strategist
    sees a structured ``unresolved_qualifier`` reject code on the next
    loop instead of an EXPLAIN failure.
    """
    pytest.importorskip("genie_space_optimizer.optimization.preflight_synthesis")
    from genie_space_optimizer.optimization import preflight_synthesis as ps

    sql = (
        "SELECT dim_date.year FROM cat.sch.mv_store_sales GROUP BY ALL"
    )
    metadata_snapshot = {
        "data_sources": {
            "tables": [],
            "metric_views": [_mv_sales_with_struct(), _mv_dim_date()],
        },
        "instructions": {"join_specs": []},
    }
    proposal = {"example_sql": sql, "example_question": "trend by year"}

    unresolved = ps._check_dangling_qualifiers_on_proposal(
        proposal, metadata_snapshot,
    )
    assert "dim_date" in unresolved
