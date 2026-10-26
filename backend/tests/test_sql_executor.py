"""Tests for ``execute_sql``'s statement lifecycle (backend/sql_executor.py).

The read-only validator has its own suite (test_sql_validation.py). These pin
what happens after validation, against a fake Statement Execution API:

- A statement that outlives ``WAIT_TIMEOUT`` is cancelled (``on_wait_timeout=
  CANCEL``), not left running on the warehouse with nobody reading it.
- Any state but ``SUCCEEDED`` is an error — a cancelled or still-running
  statement is never reported as an empty success.
- A blocked statement never reaches the warehouse.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from databricks.sdk.service.sql import (
    ExecuteStatementRequestOnWaitTimeout,
    StatementState,
)

from backend import sql_executor


def _response(state: str, *, error: str | None = None, rows: list | None = None):
    return SimpleNamespace(
        status=SimpleNamespace(
            state=StatementState(state),
            error=SimpleNamespace(message=error) if error else None,
        ),
        manifest=None,
        result=SimpleNamespace(data_array=rows) if rows is not None else None,
    )


@pytest.fixture()
def run(monkeypatch):
    """Execute ``sql`` against a fake client returning ``response``; returns the
    result and the recorded ``execute_statement`` kwargs."""

    def _run(response, sql: str = "SELECT 1"):
        calls: list[dict] = []

        def execute_statement(**kwargs):
            calls.append(kwargs)
            return response

        client = SimpleNamespace(statement_execution=SimpleNamespace(execute_statement=execute_statement))
        monkeypatch.setattr(sql_executor, "get_workspace_client", lambda: client)
        return sql_executor.execute_sql(sql, warehouse_id="wh-1"), calls

    return _run


def test_statement_is_cancelled_when_it_outlives_the_wait(run):
    _, calls = run(_response("SUCCEEDED", rows=[]))
    assert len(calls) == 1
    assert calls[0]["wait_timeout"] == sql_executor.WAIT_TIMEOUT
    assert calls[0]["on_wait_timeout"] == ExecuteStatementRequestOnWaitTimeout.CANCEL


def test_cancelled_statement_is_an_error_not_an_empty_success(run):
    result, _ = run(_response("CANCELED"))
    assert result["data"] == []
    assert result["error"] == f"Query did not finish within {sql_executor.WAIT_TIMEOUT} and was cancelled"


@pytest.mark.parametrize("state", ["PENDING", "RUNNING", "CLOSED"])
def test_unfinished_or_closed_statement_is_an_error(run, state):
    result, _ = run(_response(state))
    assert result["error"] == f"Query ended in state {state}"


def test_failed_statement_keeps_its_message(run):
    result, _ = run(_response("FAILED", error="TABLE_OR_VIEW_NOT_FOUND"))
    assert result["error"] == "TABLE_OR_VIEW_NOT_FOUND"


def test_succeeded_statement_returns_its_rows(run):
    result, _ = run(_response("SUCCEEDED", rows=[["1", "a"], ["2", "b"]]))
    assert result["error"] is None
    assert result["data"] == [["1", "a"], ["2", "b"]]
    assert result["row_count"] == 2


def test_blocked_statement_never_reaches_the_warehouse(run):
    result, calls = run(_response("SUCCEEDED", rows=[]), sql="DROP TABLE c.s.t")
    assert result["error"]
    assert calls == []
