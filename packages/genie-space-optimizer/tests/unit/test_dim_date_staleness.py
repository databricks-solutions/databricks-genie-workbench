"""Tests for ``check_dim_date_staleness`` (C3, baseline-eval-fix plan)."""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import pytest


def _row(today, max_cy):
    """Mimic a Spark ``Row`` with subscript access."""
    return {"today": today, "max_cy_date": max_cy}


class _FakeSparkDF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _FakeSpark:
    def __init__(self, rows=None, raise_exc=None):
        self._rows = rows or []
        self._raise = raise_exc
        self.last_sql = ""

    def sql(self, text: str):
        self.last_sql = text
        if self._raise is not None:
            raise self._raise
        return _FakeSparkDF(self._rows)


def _import_preflight():
    from genie_space_optimizer.optimization import preflight
    return preflight


def test_fresh_dim_date_reports_ok() -> None:
    preflight = _import_preflight()
    today = date(2026, 4, 23)
    max_cy = date(2026, 4, 22)
    spark = _FakeSpark(rows=[_row(today, max_cy)])
    result = preflight.check_dim_date_staleness(spark, "cat", "sch")
    assert result["status"] == "ok"
    assert result["days_behind"] == 1


def test_stale_dim_date_warns(caplog) -> None:
    preflight = _import_preflight()
    today = date(2026, 4, 23)
    max_cy = today - timedelta(days=120)
    spark = _FakeSpark(rows=[_row(today, max_cy)])
    with caplog.at_level("WARNING"):
        result = preflight.check_dim_date_staleness(spark, "cat", "sch")
    assert result["status"] == "stale"
    assert result["days_behind"] == 120
    assert any("DIM_DATE staleness" in rec.message for rec in caplog.records)


def test_no_current_year_rows_warns() -> None:
    preflight = _import_preflight()
    today = date(2026, 4, 23)
    spark = _FakeSpark(rows=[_row(today, None)])
    result = preflight.check_dim_date_staleness(spark, "cat", "sch")
    assert result["status"] == "stale"
    assert result["max_current_year_date"] is None


def test_missing_table_gracefully_returns_missing() -> None:
    preflight = _import_preflight()
    spark = _FakeSpark(raise_exc=Exception("TABLE_OR_VIEW_NOT_FOUND: dim_date"))
    result = preflight.check_dim_date_staleness(spark, "cat", "sch")
    assert result["status"] == "missing"


def test_other_exception_returns_error() -> None:
    preflight = _import_preflight()
    spark = _FakeSpark(raise_exc=RuntimeError("some transient spark error"))
    result = preflight.check_dim_date_staleness(spark, "cat", "sch")
    assert result["status"] == "error"
    assert "transient" in result["message"]


def test_sql_references_full_qualified_name() -> None:
    preflight = _import_preflight()
    today = date(2026, 4, 23)
    spark = _FakeSpark(rows=[_row(today, today)])
    preflight.check_dim_date_staleness(spark, "my_cat", "my_schema")
    assert "my_cat.my_schema.DIM_DATE" in spark.last_sql


def test_custom_staleness_threshold() -> None:
    """A caller can tighten the freshness window."""
    preflight = _import_preflight()
    today = date(2026, 4, 23)
    max_cy = today - timedelta(days=10)
    spark = _FakeSpark(rows=[_row(today, max_cy)])
    tight = preflight.check_dim_date_staleness(
        spark, "c", "s", staleness_days=5
    )
    assert tight["status"] == "stale"
    assert tight["days_behind"] == 10
