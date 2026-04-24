"""S8 — vacuous / tautological filter rejection in ``validate_sql_snippet``.

Lever 6 occasionally proposes filter snippets whose semantics are
``1 = 1``, ``TRUE``, ``col = col``, or ``x IS NOT NULL OR x IS NULL``.
Pre-S8 they passed validation (EXPLAIN + LIMIT 1 succeed) and silently
deployed, wasting a lever iteration. Two guards now catch them:

- **Syntactic pre-check** — cheap regex match before any warehouse call.
- **Selectivity post-check** — ``COUNT(*) total`` vs
  ``COUNT(*) FILTER (WHERE <filter>)``; reject when ``filtered >= total``.

Both guards gate behind ``GSO_REJECT_VACUOUS_FILTERS`` (default ``on``)
so a single env flip restores the lenient pre-S8 behaviour if a true
positive ever gets miscategorised.
"""

from __future__ import annotations

from typing import Any

import pytest

from genie_space_optimizer.optimization import benchmarks
from genie_space_optimizer.optimization.benchmarks import (
    _is_vacuous_filter_syntactic,
    validate_sql_snippet,
)


# ── Unit: syntactic pre-check ─────────────────────────────────────────


@pytest.mark.parametrize(
    "sql",
    [
        "1 = 1",
        "1=1",
        " 1  =  1 ",
        "TRUE",
        "true",
        "col_a = col_a",
        "status IS NOT NULL OR status IS NULL",
        "status IS NULL OR status IS NOT NULL",
        "(1 = 1)",
    ],
)
def test_syntactic_pre_check_rejects_tautologies(sql: str) -> None:
    assert _is_vacuous_filter_syntactic(sql) is True, sql


@pytest.mark.parametrize(
    "sql",
    [
        "order_total > 1000",
        "status = 'active'",
        "col_a = col_b",  # different columns
        "created_at >= CURRENT_DATE - INTERVAL 30 DAYS",
        "is_current = TRUE",
        "a IS NOT NULL",
        "a IS NOT NULL OR b IS NULL",  # different columns
    ],
)
def test_syntactic_pre_check_accepts_legit_filters(sql: str) -> None:
    assert _is_vacuous_filter_syntactic(sql) is False, sql


# ── Integration: validate_sql_snippet end-to-end ──────────────────────


class FakeSpark:
    """Minimal spark stub wired for ``validate_sql_snippet``.

    The integration path makes three kinds of calls:

    - ``USE CATALOG`` / ``USE SCHEMA`` — bookkeeping; return a no-op.
    - ``SELECT 1 FROM <t> WHERE <filter> LIMIT 1`` — the execute probe.
    - ``SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE <f>) AS filtered
      FROM <t>`` — the selectivity probe.

    ``count_plan`` lets each test control what selectivity looks like.
    """

    class _Result:
        def __init__(self, rows: list[tuple]) -> None:
            self._rows = rows

        def collect(self) -> list[Any]:
            class _Row:
                def __init__(self, values: tuple) -> None:
                    self._values = values

                def __iter__(self):
                    return iter(self._values)

                def asDict(self) -> dict:
                    return {f"c{i}": v for i, v in enumerate(self._values)}

            return [_Row(tuple(r)) for r in self._rows]

    def __init__(self, *, total: int, filtered: int, fail_execute: bool = False) -> None:
        self._total = total
        self._filtered = filtered
        self._fail_execute = fail_execute
        self.calls: list[str] = []

    def sql(self, statement: str) -> "FakeSpark._Result":
        self.calls.append(statement)
        s = statement.strip().upper()
        if s.startswith("USE "):
            return FakeSpark._Result([])
        if s.startswith("SELECT 1 FROM"):
            if self._fail_execute:
                raise RuntimeError("probe blew up")
            return FakeSpark._Result([(1,)])
        if "COUNT(*) FILTER" in statement:
            return FakeSpark._Result([(self._total, self._filtered)])
        return FakeSpark._Result([])


@pytest.fixture
def _patch_helpers(monkeypatch: pytest.MonkeyPatch):
    """Stub out primary-table lookup + FQN resolution + normalize.

    Lets each test focus on the vacuity branches without maintaining a
    real metadata snapshot shape.
    """
    monkeypatch.setattr(
        benchmarks, "_extract_primary_table", lambda *_a, **_k: "cat.sch.orders"
    )
    monkeypatch.setattr(
        benchmarks,
        "_resolve_primary_table_fqn",
        lambda t, **_k: "cat.sch.orders",
    )
    monkeypatch.setattr(
        benchmarks,
        "normalize_sql_snippet",
        lambda sql, *_a, **_k: (sql, []),
    )


@pytest.fixture
def _reject_vacuous_on(monkeypatch: pytest.MonkeyPatch):
    """Default the flag ON; earlier env leakage is reset."""
    monkeypatch.setenv("GSO_REJECT_VACUOUS_FILTERS", "on")
    # ``config`` reads the env at import time; re-import is overkill.
    # Instead rebind the module constant directly for this test.
    import importlib

    from genie_space_optimizer.common import config as _cfg

    importlib.reload(_cfg)


@pytest.fixture
def _reject_vacuous_off(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GSO_REJECT_VACUOUS_FILTERS", "off")
    import importlib

    from genie_space_optimizer.common import config as _cfg

    importlib.reload(_cfg)


def test_syntactic_tautology_rejected_without_warehouse_call(
    _patch_helpers, _reject_vacuous_on
) -> None:
    """``1=1`` must be rejected before any ``spark.sql`` call."""
    spark = FakeSpark(total=100, filtered=100)
    ok, msg, out_sql = validate_sql_snippet(
        "1 = 1", "filter", {}, spark=spark,
        catalog="cat", gold_schema="sch",
    )
    assert ok is False
    assert "tautological" in msg
    assert spark.calls == [], "should short-circuit before any spark call"


def test_col_equals_col_rejected_syntactically(
    _patch_helpers, _reject_vacuous_on
) -> None:
    spark = FakeSpark(total=100, filtered=100)
    ok, msg, _ = validate_sql_snippet(
        "col_a = col_a", "filter", {}, spark=spark,
        catalog="cat", gold_schema="sch",
    )
    assert ok is False
    assert "tautological" in msg


def test_true_rejected_syntactically(
    _patch_helpers, _reject_vacuous_on
) -> None:
    spark = FakeSpark(total=100, filtered=100)
    ok, msg, _ = validate_sql_snippet(
        "TRUE", "filter", {}, spark=spark,
        catalog="cat", gold_schema="sch",
    )
    assert ok is False


def test_selectivity_check_rejects_all_rows(
    _patch_helpers, _reject_vacuous_on
) -> None:
    """Filter passes the syntactic check but still selects 100% of rows."""
    spark = FakeSpark(total=1000, filtered=1000)
    ok, msg, _ = validate_sql_snippet(
        "a IS NOT NULL OR b IS NULL",
        "filter",
        {},
        spark=spark,
        catalog="cat",
        gold_schema="sch",
    )
    assert ok is False
    assert "vacuous" in msg
    assert "1000/1000" in msg


def test_selectivity_check_accepts_restrictive_filter(
    _patch_helpers, _reject_vacuous_on
) -> None:
    spark = FakeSpark(total=1000, filtered=42)
    ok, msg, _ = validate_sql_snippet(
        "order_total > 1000",
        "filter",
        {},
        spark=spark,
        catalog="cat",
        gold_schema="sch",
    )
    assert ok is True, msg


def test_empty_table_skips_selectivity_check(
    _patch_helpers, _reject_vacuous_on
) -> None:
    """``total == 0`` can't prove vacuity. Validator stays lenient."""
    spark = FakeSpark(total=0, filtered=0)
    ok, msg, _ = validate_sql_snippet(
        "order_total > 1000",
        "filter",
        {},
        spark=spark,
        catalog="cat",
        gold_schema="sch",
    )
    assert ok is True, msg


def test_selectivity_probe_failure_defaults_to_accept(
    _patch_helpers, _reject_vacuous_on, monkeypatch
) -> None:
    """If the selectivity probe itself fails, fall back to lenient accept."""
    spark = FakeSpark(total=0, filtered=0)

    original_sql = spark.sql

    def _sql_with_count_fail(statement: str):
        if "COUNT(*) FILTER" in statement:
            raise RuntimeError("warehouse hiccup on count probe")
        return original_sql(statement)

    monkeypatch.setattr(spark, "sql", _sql_with_count_fail)
    ok, msg, _ = validate_sql_snippet(
        "order_total > 1000",
        "filter",
        {},
        spark=spark,
        catalog="cat",
        gold_schema="sch",
    )
    assert ok is True, msg


def test_flag_off_accepts_syntactic_tautologies(
    _patch_helpers, _reject_vacuous_off
) -> None:
    """Kill-switch off → pre-S8 lenient behaviour. Tautologies pass."""
    spark = FakeSpark(total=100, filtered=100)
    ok, _, _ = validate_sql_snippet(
        "1 = 1", "filter", {}, spark=spark,
        catalog="cat", gold_schema="sch",
    )
    assert ok is True


def test_flag_off_accepts_selectivity_tautologies(
    _patch_helpers, _reject_vacuous_off
) -> None:
    spark = FakeSpark(total=1000, filtered=1000)
    ok, _, _ = validate_sql_snippet(
        "a IS NOT NULL OR b IS NULL",
        "filter",
        {},
        spark=spark,
        catalog="cat",
        gold_schema="sch",
    )
    assert ok is True


def test_measure_snippets_skip_filter_guards(
    _patch_helpers, _reject_vacuous_on
) -> None:
    """``snippet_type != 'filter'`` must not be subject to filter guards."""
    spark = FakeSpark(total=100, filtered=100)
    ok, _, _ = validate_sql_snippet(
        "SUM(order_total)",
        "measure",
        {},
        spark=spark,
        catalog="cat",
        gold_schema="sch",
    )
    assert ok is True


def test_expression_snippets_skip_filter_guards(
    _patch_helpers, _reject_vacuous_on
) -> None:
    spark = FakeSpark(total=100, filtered=100)
    ok, _, _ = validate_sql_snippet(
        "MONTH(order_date)",
        "expression",
        {},
        spark=spark,
        catalog="cat",
        gold_schema="sch",
    )
    assert ok is True
