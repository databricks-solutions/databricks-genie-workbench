"""Tests for :func:`_recover_trace_map` parallel strategies (A3).

The recovery path has three strategies. They are tried in order and the
first non-empty result wins; each strategy's hit count is logged as a
best-effort MLflow metric.

We exercise them in isolation by mocking ``mlflow.search_traces`` /
``mlflow.log_metric`` and by constructing fake ``eval_result`` objects
for strategy 3.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from genie_space_optimizer.optimization import evaluation as ev


def _traces_df(rows):
    return pd.DataFrame(rows)


def test_strategy_1_tags_hits_short_circuits_later_strategies():
    traces = _traces_df(
        [
            {"trace_id": "t-1", "tags": {"question_id": "q1"}},
            {"trace_id": "t-2", "tags": {"question_id": "q2"}},
        ]
    )
    with patch.object(
        ev.mlflow, "search_traces", return_value=traces
    ) as search, patch.object(ev.mlflow, "log_metric") as log_metric:
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=3,
            expected_count=2,
        )

    assert recovered == {"q1": "t-1", "q2": "t-2"}
    assert search.call_count == 1
    logged = {call.args[0]: call.args[1] for call in log_metric.call_args_list}
    assert logged.get("trace_map.recovery.tags.hit_count") == 2.0
    assert logged.get("trace_map.recovery.time_window.hit_count") == 0.0
    assert logged.get("trace_map.recovery.eval_results.hit_count") == 0.0


def test_strategy_2_time_window_runs_when_strategy_1_empty():
    empty = _traces_df([])
    tw_traces = _traces_df(
        [{"trace_id": "t-9", "tags": {"question_id": "q-recovered"}}]
    )
    with patch.object(
        ev.mlflow, "search_traces", side_effect=[empty, tw_traces]
    ) as search, patch.object(ev.mlflow, "log_metric") as log_metric:
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=1,
            expected_count=1,
            start_time_ms=1_700_000_000_000,
        )

    assert recovered == {"q-recovered": "t-9"}
    assert search.call_count == 2
    logged = {call.args[0]: call.args[1] for call in log_metric.call_args_list}
    assert logged["trace_map.recovery.tags.hit_count"] == 0.0
    assert logged["trace_map.recovery.time_window.hit_count"] == 1.0
    assert logged["trace_map.recovery.eval_results.hit_count"] == 0.0


def test_strategy_3_eval_results_fallback():
    empty = _traces_df([])
    fake_eval_df = pd.DataFrame(
        [
            {"trace_id": "t-e1", "inputs/question_id": "q-a"},
            {"trace_id": "t-e2", "inputs/question_id": "q-b"},
        ]
    )
    fake_eval_result = SimpleNamespace(tables={"eval_results": fake_eval_df})

    with patch.object(
        ev.mlflow, "search_traces", side_effect=[empty, empty]
    ), patch.object(ev.mlflow, "log_metric") as log_metric:
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=2,
            expected_count=2,
            start_time_ms=1_700_000_000_000,
            eval_result=fake_eval_result,
        )

    assert recovered == {"q-a": "t-e1", "q-b": "t-e2"}
    logged = {call.args[0]: call.args[1] for call in log_metric.call_args_list}
    assert logged["trace_map.recovery.tags.hit_count"] == 0.0
    assert logged["trace_map.recovery.time_window.hit_count"] == 0.0
    assert logged["trace_map.recovery.eval_results.hit_count"] == 2.0


def test_all_strategies_empty_returns_empty_map():
    empty = _traces_df([])
    with patch.object(
        ev.mlflow, "search_traces", side_effect=[empty, empty]
    ), patch.object(ev.mlflow, "log_metric") as log_metric:
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=2,
            expected_count=5,
        )
    assert recovered == {}
    logged = {call.args[0]: call.args[1] for call in log_metric.call_args_list}
    assert logged["trace_map.recovery.tags.hit_count"] == 0.0
    assert logged["trace_map.recovery.time_window.hit_count"] == 0.0
    assert logged["trace_map.recovery.eval_results.hit_count"] == 0.0


def test_search_traces_exception_does_not_propagate():
    with patch.object(
        ev.mlflow, "search_traces", side_effect=RuntimeError("gRPC boom")
    ), patch.object(ev.mlflow, "log_metric"):
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=1,
            expected_count=1,
        )
    assert recovered == {}


def test_log_metric_failure_does_not_poison_recovery():
    traces = _traces_df([{"trace_id": "t-1", "tags": {"question_id": "q1"}}])
    with patch.object(
        ev.mlflow, "search_traces", return_value=traces
    ), patch.object(
        ev.mlflow, "log_metric", side_effect=RuntimeError("no active run")
    ):
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=1,
            expected_count=1,
        )
    assert recovered == {"q1": "t-1"}


def test_missing_experiment_id_short_circuits_strategy_1():
    empty = _traces_df([])
    with patch.object(
        ev.mlflow, "search_traces", return_value=empty
    ) as search, patch.object(ev.mlflow, "log_metric"):
        recovered = ev._recover_trace_map(
            experiment_id="",
            optimization_run_id="opt-1",
            iteration=1,
            expected_count=1,
        )
    assert recovered == {}
    assert search.call_count == 0


def test_eval_results_without_trace_id_column_is_skipped():
    df = pd.DataFrame([{"inputs/question_id": "q-a"}])
    fake = SimpleNamespace(tables={"eval_results": df})
    recovered = ev._recover_trace_map_via_eval_results(fake)
    assert recovered == {}


def test_qid_extraction_from_nested_inputs():
    df = pd.DataFrame(
        [{"trace_id": "t-1", "inputs": {"question_id": "nested-qid"}}]
    )
    fake = SimpleNamespace(tables={"eval_results": df})
    recovered = ev._recover_trace_map_via_eval_results(fake)
    assert recovered == {"nested-qid": "t-1"}
