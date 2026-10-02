from __future__ import annotations

import base64
import json
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


def test_insert_and_update_row_preserve_nested_json_bytes_with_base64_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    monkeypatch.setattr(
        delta_helpers,
        "execute_delta_write_with_retry",
        lambda _spark, stmt, **_kwargs: statements.append(stmt),
    )
    payload = json.dumps({
        "serialized_space": json.dumps({"version": 2, "description": "O'Brien"}),
    })
    encoded = base64.b64encode(payload.encode("utf-8")).decode("ascii")

    delta_helpers.insert_row(
        object(), "cat", "sch", "tbl", {"config_snapshot": payload},
        base64_string_columns={"config_snapshot"},
    )
    delta_helpers.update_row(
        object(), "cat", "sch", "tbl", {"run_id": "r1"},
        {"config_snapshot": payload},
        base64_string_columns={"config_snapshot"},
    )

    expression = f"CAST(unbase64('{encoded}') AS STRING)"
    assert expression in statements[0]
    assert f"config_snapshot = {expression}" in statements[1]
    assert payload not in statements[0]
    assert payload not in statements[1]


def test_insert_row_can_transport_string_bytes_without_sql_literal_escaping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    monkeypatch.setattr(
        delta_helpers,
        "execute_delta_write_with_retry",
        lambda _spark, stmt, **_kwargs: statements.append(stmt),
    )
    payload = json.dumps({
        "description": "O'Brien\\nSnowman: ☃",
        "expression": r"CASE WHEN path = 'C:\\tmp' THEN 1 END",
    })
    encoded = base64.b64encode(payload.encode("utf-8")).decode("ascii")

    delta_helpers.insert_row(
        object(),
        "cat",
        "sch",
        "tbl",
        {"artifact_json": payload, "artifact_kind": "wide_schema_inventory"},
        base64_string_columns={"artifact_json"},
    )

    assert f"CAST(unbase64('{encoded}') AS STRING)" in statements[0]
    assert payload not in statements[0]
    assert "'wide_schema_inventory'" in statements[0]


def test_merge_row_builds_a_keyed_upsert_with_insert_only_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(
        delta_helpers,
        "execute_delta_write_with_retry",
        lambda spark, stmt, **kwargs: captured.append({"stmt": stmt, "kwargs": kwargs}),
    )

    delta_helpers.merge_row(
        object(), "cat", "sch", "tbl",
        {"run_id": "r1", "suggestion_id": "s1"},
        {"status": "ATTACHED", "updated_at": "t2"},
        insert_only_cols={"created_at": "t1"},
    )

    assert captured[0]["stmt"] == (
        "MERGE INTO cat.sch.tbl AS t "
        "USING (SELECT 'r1' AS run_id, 's1' AS suggestion_id) AS s "
        "ON t.run_id = s.run_id AND t.suggestion_id = s.suggestion_id "
        "WHEN MATCHED THEN UPDATE SET t.status = 'ATTACHED', t.updated_at = 't2' "
        "WHEN NOT MATCHED THEN INSERT "
        "(run_id, suggestion_id, status, updated_at, created_at) "
        "VALUES (s.run_id, s.suggestion_id, 'ATTACHED', 't2', 't1')"
    )
    assert captured[0]["kwargs"]["operation_name"] == "merge_row"
    assert captured[0]["kwargs"]["table_name"] == "cat.sch.tbl"


def test_merge_row_preserves_json_bytes_with_base64_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    monkeypatch.setattr(
        delta_helpers,
        "execute_delta_write_with_retry",
        lambda _spark, stmt, **_kwargs: statements.append(stmt),
    )
    payload = json.dumps({"note": "O'Brien\\nSnowman: ☃"})
    encoded = base64.b64encode(payload.encode("utf-8")).decode("ascii")

    delta_helpers.merge_row(
        object(), "cat", "sch", "tbl",
        {"probe_id": "p1"},
        {"probe_results_json": payload},
        base64_string_columns={"probe_results_json"},
    )

    assert f"CAST(unbase64('{encoded}') AS STRING)" in statements[0]
    assert payload not in statements[0]


def test_merge_row_requires_keys_and_values() -> None:
    with pytest.raises(ValueError, match="key column"):
        delta_helpers.merge_row(object(), "cat", "sch", "tbl", {}, {"a": 1})
    with pytest.raises(ValueError, match="value column"):
        delta_helpers.merge_row(object(), "cat", "sch", "tbl", {"k": "v"}, {})


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
