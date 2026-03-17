"""
Generic Delta table read/write utilities.

Used by ``optimization/state.py`` and the backend routes. Every function
takes a ``spark`` session as its first argument.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_MISSING_OBJECT_ERRORS = ("TABLE_OR_VIEW_NOT_FOUND", "SCHEMA_NOT_FOUND")

_REFRESH_SKIP: bool | None = None


def _safe_refresh(spark: "SparkSession", table_name: str) -> None:
    """Best-effort REFRESH TABLE — skipped entirely on serverless/Spark Connect.

    Spark Connect (used by serverless and Databricks Apps) does not support
    REFRESH TABLE. We detect Connect sessions upfront by checking the session
    class module path, avoiding a noisy gRPC round-trip on every call.
    """
    global _REFRESH_SKIP
    if _REFRESH_SKIP is None:
        try:
            _REFRESH_SKIP = "connect" in type(spark).__module__
        except Exception:
            _REFRESH_SKIP = False
    if _REFRESH_SKIP:
        return
    try:
        spark.sql(f"REFRESH TABLE {table_name}")
    except Exception:
        pass


def _native_df(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all numpy dtypes to Python-native object columns.

    Pandas ``toPandas()`` returns numpy scalar types (``numpy.float64``,
    ``numpy.int64``) which Pydantic's C-level serializer cannot handle
    in ``int``-typed model fields.  Converting to ``object`` dtype forces
    native Python ``int`` / ``float`` / ``None`` values.
    """
    if df.empty:
        return df
    return df.astype(object).where(df.notna(), None)


def _fqn(catalog: str, schema: str, table: str) -> str:
    """Build a fully-qualified Delta table name."""
    return f"{catalog}.{schema}.{table}"


def read_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    filters: dict[str, Any] | None = None,
    _max_retries: int = 3,
) -> pd.DataFrame:
    """Read a Delta table into a Pandas DataFrame, optionally filtered.

    ``filters`` is a dict of ``{column: value}`` equality predicates
    combined with ``AND``.

    Issues ``REFRESH TABLE`` before reading and retries on
    ``DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS`` to handle tables that were
    dropped and recreated by an upstream task in the same job.
    """
    fqn = _fqn(catalog, schema, table)
    where_clauses: list[str] = []
    if filters:
        for col, val in filters.items():
            if isinstance(val, str):
                where_clauses.append(f"{col} = '{val}'")
            else:
                where_clauses.append(f"{col} = {val}")

    query = f"SELECT * FROM {fqn}"
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    for attempt in range(_max_retries):
        try:
            _safe_refresh(spark, fqn)
            logger.debug("read_table: %s", query)
            return _native_df(spark.sql(query).toPandas())
        except Exception as exc:
            exc_str = str(exc)
            if "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS" in exc_str and attempt < _max_retries - 1:
                import time as _time
                wait = 5 * (attempt + 1)
                logger.warning(
                    "Delta schema change on attempt %d/%d for %s — retrying in %ds",
                    attempt + 1, _max_retries, fqn, wait,
                )
                _time.sleep(wait)
                continue
            if any(code in exc_str for code in _MISSING_OBJECT_ERRORS):
                logger.debug("Table/schema not found, returning empty DataFrame: %s", exc_str[:120])
                return pd.DataFrame()
            raise
    return pd.DataFrame()


def insert_row(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    row_dict: dict[str, Any],
) -> None:
    """Insert a single row into a Delta table via SQL ``INSERT INTO``.

    Values are auto-quoted based on Python type (str → quoted, else raw).
    """
    fqn = _fqn(catalog, schema, table)
    def _sql_lit(v: Any) -> str:
        if isinstance(v, str):
            return f"'{v.replace(chr(39), chr(39)+chr(39))}'"
        if v is None:
            return "NULL"
        return str(v)

    columns = ", ".join(row_dict.keys())
    values = ", ".join(_sql_lit(v) for v in row_dict.values())
    stmt = f"INSERT INTO {fqn} ({columns}) VALUES ({values})"
    logger.debug("insert_row: %s", stmt)
    spark.sql(stmt)


def update_row(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    key_cols: dict[str, Any],
    update_cols: dict[str, Any],
) -> None:
    """Update a row in a Delta table matching ``key_cols`` with ``update_cols``.

    Both arguments are ``{column: value}`` dicts. Key columns form the
    ``WHERE`` clause; update columns form the ``SET`` clause.
    """
    fqn = _fqn(catalog, schema, table)

    def _fmt(val: Any) -> str:
        if isinstance(val, str):
            escaped = val.replace("'", "''")
            return f"'{escaped}'"
        if val is None:
            return "NULL"
        return str(val)

    set_clause = ", ".join(f"{col} = {_fmt(val)}" for col, val in update_cols.items())
    where_clause = " AND ".join(f"{col} = {_fmt(val)}" for col, val in key_cols.items())

    stmt = f"UPDATE {fqn} SET {set_clause} WHERE {where_clause}"
    logger.debug("update_row: %s", stmt)
    spark.sql(stmt)


def run_query(spark: SparkSession, sql: str, _max_retries: int = 3) -> pd.DataFrame:
    """Execute arbitrary SQL and return results as a Pandas DataFrame.

    Retries on ``DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS`` with a ``REFRESH TABLE``
    for any table referenced in the query.
    """
    for attempt in range(_max_retries):
        try:
            logger.debug("run_query: %s", sql)
            return _native_df(spark.sql(sql).toPandas())
        except Exception as exc:
            exc_str = str(exc)
            if "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS" in exc_str and attempt < _max_retries - 1:
                import re as _re
                import time as _time
                tables = _re.findall(r"FROM\s+([\w.]+)", sql, _re.IGNORECASE)
                for tbl in tables:
                    _safe_refresh(spark, tbl)
                wait = 5 * (attempt + 1)
                logger.warning(
                    "Delta schema change on attempt %d/%d — refreshing and retrying in %ds",
                    attempt + 1, _max_retries, wait,
                )
                _time.sleep(wait)
                continue
            if any(code in exc_str for code in _MISSING_OBJECT_ERRORS):
                logger.debug("Table/schema not found, returning empty DataFrame: %s", exc_str[:120])
                return pd.DataFrame()
            raise
    return pd.DataFrame()
