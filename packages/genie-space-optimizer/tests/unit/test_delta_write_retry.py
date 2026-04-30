from __future__ import annotations

from typing import Any

import pytest

from genie_space_optimizer.common import delta_helpers


class _FakeSpark:
    def __init__(self, failures_before_success: int = 0, error_text: str = "") -> None:
        self.failures_before_success = failures_before_success
        self.error_text = error_text
        self.sql_calls: list[str] = []

    def sql(self, stmt: str) -> object:
        self.sql_calls.append(stmt)
        if self.failures_before_success > 0:
            self.failures_before_success -= 1
            raise RuntimeError(self.error_text)
        return object()


def test_delta_conflict_classifier_matches_partition_hint_error() -> None:
    exc = RuntimeError(
        "[DELTA_CONCURRENT_APPEND.WITH_PARTITION_HINT] Transaction conflict detected. "
        "A concurrent UPDATE added data to table cat.sch.genie_opt_runs."
    )

    assert delta_helpers.is_retryable_delta_write_conflict(exc)


def test_delta_conflict_classifier_rejects_permission_errors() -> None:
    exc = RuntimeError("[PERMISSION_DENIED] User does not have MODIFY on schema")

    assert not delta_helpers.is_retryable_delta_write_conflict(exc)


def test_retry_delta_write_retries_then_returns_value() -> None:
    calls: list[int] = []
    sleeps: list[float] = []

    def operation() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("ConcurrentAppendException: Transaction conflict detected")
        return "ok"

    result = delta_helpers.retry_delta_write(
        operation,
        operation_name="unit-test",
        table_name="cat.sch.tbl",
        attempts=4,
        base_delay_seconds=0.1,
        max_delay_seconds=1.0,
        sleep_func=sleeps.append,
        jitter_func=lambda: 0.0,
    )

    assert result == "ok"
    assert len(calls) == 3
    assert sleeps == [0.1, 0.2]


def test_retry_delta_write_raises_non_retryable_without_sleep() -> None:
    sleeps: list[float] = []

    def operation() -> None:
        raise RuntimeError("[UNRESOLVED_COLUMN] missing field")

    with pytest.raises(RuntimeError, match="UNRESOLVED_COLUMN"):
        delta_helpers.retry_delta_write(
            operation,
            operation_name="unit-test",
            table_name="cat.sch.tbl",
            sleep_func=sleeps.append,
        )

    assert sleeps == []


def test_execute_delta_write_with_retry_retries_spark_sql() -> None:
    spark = _FakeSpark(
        failures_before_success=1,
        error_text="[DELTA_CONCURRENT_APPEND.WITH_PARTITION_HINT] Transaction conflict detected",
    )
    sleeps: list[float] = []

    delta_helpers.execute_delta_write_with_retry(
        spark,
        "UPDATE cat.sch.tbl SET status = 'COMPLETE' WHERE run_id = 'r1'",
        operation_name="update test row",
        table_name="cat.sch.tbl",
        sleep_func=sleeps.append,
        jitter_func=lambda: 0.0,
    )

    assert spark.sql_calls == [
        "UPDATE cat.sch.tbl SET status = 'COMPLETE' WHERE run_id = 'r1'",
        "UPDATE cat.sch.tbl SET status = 'COMPLETE' WHERE run_id = 'r1'",
    ]
    assert sleeps == [0.25]


def test_insert_and_update_row_use_retry_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[dict[str, Any]] = []

    def fake_execute(spark, stmt: str, **kwargs: Any) -> None:
        captured.append({"spark": spark, "stmt": stmt, "kwargs": kwargs})

    monkeypatch.setattr(delta_helpers, "execute_delta_write_with_retry", fake_execute)

    spark = object()
    delta_helpers.insert_row(
        spark, "cat", "sch", "tbl",
        {"run_id": "r1", "status": "IN_PROGRESS", "count": 2},
    )
    delta_helpers.update_row(
        spark, "cat", "sch", "tbl",
        {"run_id": "r1"},
        {"status": "COMPLETE"},
    )

    assert len(captured) == 2
    assert captured[0]["stmt"] == (
        "INSERT INTO cat.sch.tbl (run_id, status, count) "
        "VALUES ('r1', 'IN_PROGRESS', 2)"
    )
    assert captured[0]["kwargs"]["operation_name"] == "insert_row"
    assert captured[0]["kwargs"]["table_name"] == "cat.sch.tbl"
    assert captured[1]["stmt"] == (
        "UPDATE cat.sch.tbl SET status = 'COMPLETE' WHERE run_id = 'r1'"
    )
    assert captured[1]["kwargs"]["operation_name"] == "update_row"
    assert captured[1]["kwargs"]["table_name"] == "cat.sch.tbl"


def test_retry_delta_write_raises_final_conflict_after_attempts() -> None:
    calls: list[int] = []
    sleeps: list[float] = []

    def operation() -> None:
        calls.append(1)
        raise RuntimeError("[DELTA_CONCURRENT_APPEND.WITH_PARTITION_HINT] Transaction conflict detected")

    with pytest.raises(RuntimeError, match="DELTA_CONCURRENT_APPEND"):
        delta_helpers.retry_delta_write(
            operation,
            operation_name="always-conflicts",
            table_name="cat.sch.tbl",
            attempts=3,
            base_delay_seconds=0.1,
            max_delay_seconds=1.0,
            sleep_func=sleeps.append,
            jitter_func=lambda: 0.0,
        )

    assert len(calls) == 3
    assert sleeps == [0.1, 0.2]
