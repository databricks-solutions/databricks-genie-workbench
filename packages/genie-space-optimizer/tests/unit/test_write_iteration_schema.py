"""Persistence-contract tests for the active iteration schema."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.state import write_iteration


@pytest.fixture
def mock_spark_iter():
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    return spark


def _insert_sql(spark: MagicMock) -> str:
    for call in spark.sql.call_args_list:
        sql = call.args[0] if call.args else call.kwargs.get("sqlQuery", "")
        if "INSERT INTO" in sql and "genie_opt_iterations" in sql:
            return sql
    raise AssertionError("No genie_opt_iterations INSERT was executed")


def test_write_iteration_persists_active_denominator_columns(mock_spark_iter) -> None:
    write_iteration(
        mock_spark_iter,
        run_id="run-1",
        iteration=0,
        eval_result={
            "overall_accuracy": 85.71,
            "total_questions": 14,
            "evaluated_count": 13,
            "correct_count": 12,
            "excluded_count": 1,
            "scores": {},
            "failures": ["q14"],
            "thresholds_met": False,
        },
        catalog="cat",
        schema="sch",
    )

    sql = _insert_sql(mock_spark_iter)
    assert "evaluated_count, excluded_count" in sql
    assert "quarantined_benchmarks_json" not in sql
    assert "arbiter_actions_json" not in sql
    assert "repeatability_json" not in sql


def test_write_iteration_defaults_missing_denominator_counts(mock_spark_iter) -> None:
    write_iteration(
        mock_spark_iter,
        run_id="run-2",
        iteration=1,
        eval_result={
            "overall_accuracy": 90.0,
            "total_questions": 10,
            "correct_count": 9,
            "scores": {},
            "thresholds_met": True,
        },
        catalog="cat",
        schema="sch",
    )

    sql = _insert_sql(mock_spark_iter)
    assert ", 10, 0, false," in sql


def test_write_iteration_persists_native_eval_metadata(mock_spark_iter) -> None:
    write_iteration(
        mock_spark_iter,
        run_id="run-native",
        iteration=0,
        eval_result={
            "overall_accuracy": 80.0,
            "total_questions": 10,
            "evaluated_count": 10,
            "correct_count": 8,
            "excluded_count": 0,
            "scores": {},
            "thresholds_met": False,
            "num_needs_review": 2,
            "eval_run_id": "er-12345",
            "eval_run_status": "DONE",
        },
        catalog="cat",
        schema="sch",
    )

    sql = _insert_sql(mock_spark_iter)
    assert "num_needs_review, eval_run_id, eval_run_status" in sql
    assert "false, 2, 'er-12345', 'DONE'," in sql


def test_write_iteration_accepts_enrichment_scope(mock_spark_iter) -> None:
    write_iteration(
        mock_spark_iter,
        run_id="run-enrichment",
        iteration=0,
        eval_scope="enrichment",
        eval_result={
            "overall_accuracy": 96.15,
            "total_questions": 26,
            "correct_count": 25,
            "scores": {},
            "thresholds_met": True,
        },
        catalog="cat",
        schema="sch",
    )

    assert "'enrichment'" in _insert_sql(mock_spark_iter)
