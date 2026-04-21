"""Unit tests for ``write_iteration`` schema (Bug #2 persistence contract).

Guards against:
  * Missing evaluated_count / excluded_count / quarantined_benchmarks_json
    columns in the INSERT — would silently drop Bug #2/#3 data.
  * Regression to total_questions-as-denominator semantics.
  * Crashes when old call sites emit eval_results lacking the new keys.

We mock spark.sql and inspect the rendered INSERT statement rather than
requiring a real Spark session — these are pure unit tests that run in CI
without Databricks connectivity.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.state import write_iteration


@pytest.fixture
def mock_spark_iter():
    """Capture the SQL string passed to spark.sql for assertions."""
    spark = MagicMock()
    spark.sql.return_value = MagicMock()
    return spark


def _extract_insert_sql(mock_spark: MagicMock) -> str:
    """Pull the first spark.sql(...) call that looks like an INSERT."""
    for call in mock_spark.sql.call_args_list:
        sql = call.args[0] if call.args else call.kwargs.get("sqlQuery", "")
        if "INSERT INTO" in sql and "genie_opt_iterations" in sql:
            return sql
    raise AssertionError(
        f"No INSERT INTO genie_opt_iterations found. Calls: {mock_spark.sql.call_args_list}"
    )


def test_write_iteration_includes_new_bug2_columns(mock_spark_iter) -> None:
    """Bug #2/#3: write_iteration must persist the new counts + quarantine JSON."""
    eval_result = {
        "overall_accuracy": 85.71,
        "total_questions": 14,
        "evaluated_count": 14,
        "correct_count": 12,
        "excluded_count": 0,
        "scores": {"judge_a": 90.0, "judge_b": 80.0},
        "failures": ["q13", "q14"],
        "remaining_failures": ["q13", "q14"],
        "arbiter_actions": [],
        "thresholds_met": False,
        "rows": [{"question_id": "q1", "result_correctness/value": "yes"}],
        "mlflow_run_id": "run-abc-123",
        "quarantined_benchmarks": [
            {
                "question_id": "q_bad",
                "reason_code": "quarantined",
                "reason_detail": "EXPLAIN failed",
                "question": "broken q",
            }
        ],
    }

    write_iteration(
        mock_spark_iter,
        run_id="run-1",
        iteration=0,
        eval_result=eval_result,
        catalog="cat",
        schema="sch",
        eval_scope="full",
    )

    sql = _extract_insert_sql(mock_spark_iter)
    # All three new columns must be in the col list.
    assert "evaluated_count" in sql
    assert "excluded_count" in sql
    assert "quarantined_benchmarks_json" in sql
    # The quarantine payload must survive serialization into the INSERT.
    assert "q_bad" in sql
    # total_questions is still present for back-compat.
    assert "total_questions" in sql


def test_write_iteration_back_compat_defaults_when_new_fields_missing(
    mock_spark_iter,
) -> None:
    """Back-compat: eval_results from old call sites (e.g. repeatability-only)
    must not crash. evaluated_count falls back to total_questions, excluded
    defaults to 0, quarantined_benchmarks_json is NULL.
    """
    eval_result = {
        "overall_accuracy": 90.0,
        "total_questions": 10,
        "correct_count": 9,
        "scores": {},
        "thresholds_met": True,
    }

    write_iteration(
        mock_spark_iter,
        run_id="run-2",
        iteration=1,
        eval_result=eval_result,
        catalog="cat",
        schema="sch",
    )

    sql = _extract_insert_sql(mock_spark_iter)
    # evaluated_count defaults to total_questions when missing.
    assert ", 10, 0, NULL" in sql or ", 10, 0," in sql


def test_write_iteration_escapes_quotes_in_quarantine_payload(mock_spark_iter) -> None:
    """SQL injection / quote safety: the quarantine JSON may include user-
    controlled strings with single quotes. _opt_json should escape them.
    """
    eval_result = {
        "overall_accuracy": 50.0,
        "total_questions": 2,
        "evaluated_count": 1,
        "correct_count": 0,
        "excluded_count": 1,
        "scores": {},
        "thresholds_met": False,
        "quarantined_benchmarks": [
            {
                "question_id": "q_quote",
                "reason_detail": "Column 'amount' doesn't exist",
                "question": "Show 'weird' data",
            }
        ],
    }

    write_iteration(
        mock_spark_iter,
        run_id="run-3",
        iteration=0,
        eval_result=eval_result,
        catalog="cat",
        schema="sch",
    )

    sql = _extract_insert_sql(mock_spark_iter)
    # Spark SQL literal quoting: every ' inside a '...'-delimited literal
    # must be doubled as ''. Our _esc/_opt_json helpers do this. We verify by
    # extracting the payload literal and asserting it contains only even-length
    # runs of single quotes (i.e. '' pairs, never a lone ').
    import re as _re
    payload_match = _re.search(
        r"(\[\\{.*?\\}\]|\[\{.*?\}\])",
        sql,
        flags=_re.DOTALL,
    )
    assert payload_match, f"No quarantine JSON payload found in SQL: {sql[:500]}"
    payload = payload_match.group(1)
    # A lone (odd-count) single quote would terminate the SQL literal early —
    # classic injection. Every run of quotes in the payload must be even length.
    for run in _re.findall(r"'+", payload):
        assert len(run) % 2 == 0, (
            f"Odd-length quote run in payload (broken escape): {run!r} in {payload!r}"
        )
    # Positive confirmation: the original single quote was preserved as an
    # escaped pair, so the user-supplied text is still recoverable.
    assert "''amount''" in payload or r"\u0027amount\u0027" in payload
