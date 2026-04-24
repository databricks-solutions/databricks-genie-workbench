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


def test_strategy_1_uses_locations_kwarg_not_experiment_ids():
    """``mlflow.search_traces(experiment_ids=)`` is deprecated.

    Strategy 1 (tags) must call ``search_traces`` with ``locations=``.
    If anyone reintroduces ``experiment_ids=``, this test fails.
    """
    traces = _traces_df([{"trace_id": "t-1", "tags": {"question_id": "q1"}}])
    with patch.object(
        ev.mlflow, "search_traces", return_value=traces
    ) as search, patch.object(ev.mlflow, "log_metric"):
        ev._recover_trace_map(
            experiment_id="exp-42",
            optimization_run_id="opt-1",
            iteration=1,
            expected_count=1,
        )

    assert search.call_count == 1
    kwargs = search.call_args.kwargs
    assert kwargs.get("locations") == ["exp-42"]
    assert "experiment_ids" not in kwargs


def test_strategy_2_uses_locations_kwarg_not_experiment_ids():
    """Strategy 2 (time_window) must also migrate to ``locations=``."""
    empty = _traces_df([])
    tw_traces = _traces_df(
        [{"trace_id": "t-9", "tags": {"question_id": "q-recovered"}}]
    )
    with patch.object(
        ev.mlflow, "search_traces", side_effect=[empty, tw_traces]
    ) as search, patch.object(ev.mlflow, "log_metric"):
        ev._recover_trace_map(
            experiment_id="exp-42",
            optimization_run_id="opt-1",
            iteration=1,
            expected_count=1,
            start_time_ms=1_700_000_000_000,
        )

    assert search.call_count == 2
    kwargs_s2 = search.call_args_list[1].kwargs
    assert kwargs_s2.get("locations") == ["exp-42"]
    assert "experiment_ids" not in kwargs_s2


def test_evaluation_module_has_no_experiment_ids_kwarg():
    """Source-level guard — no mlflow.search_traces call uses experiment_ids=.

    Complements the two call-site assertions above by scanning the module
    source: it catches any future regression where someone adds a third
    ``search_traces`` call with the deprecated kwarg.
    """
    import pathlib

    src = pathlib.Path(ev.__file__).read_text()
    assert "experiment_ids=" not in src, (
        "mlflow.search_traces now takes `locations=`; don't reintroduce "
        "the deprecated `experiment_ids=` kwarg."
    )


# ---------------------------------------------------------------------------
# Union semantics (1d): strategies must cover the residual gap together
# ---------------------------------------------------------------------------

def test_trace_map_recovery_unions_partial_strategy_hits(monkeypatch):
    """Strategy 1 returns partial; strategies 2 and 3 must still run and
    fill in the residual qids.

    Regression target: the observed ``Recovered 14/22`` symptom where
    strategy 1 hit partially and the short-circuit skipped strategies 2
    and 3 even though they could have covered the other 8.
    """
    calls = []

    def fake_tags(experiment_id, optimization_run_id, iteration, expected_count):
        calls.append("tags")
        return {"q1": "t1", "q2": "t2"}

    def fake_time_window(experiment_id, start_time_ms, expected_count):
        calls.append("time_window")
        return {"q3": "t3"}

    def fake_eval_results(eval_result):
        calls.append("eval_results")
        return {"q4": "t4"}

    monkeypatch.setattr(ev, "_recover_trace_map_via_tags", fake_tags)
    monkeypatch.setattr(ev, "_recover_trace_map_via_time_window", fake_time_window)
    monkeypatch.setattr(ev, "_recover_trace_map_via_eval_results", fake_eval_results)

    with patch.object(ev.mlflow, "log_metric"):
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=0,
            expected_count=4,
            start_time_ms=1_700_000_000_000,
            eval_result=object(),
        )

    assert recovered == {"q1": "t1", "q2": "t2", "q3": "t3", "q4": "t4"}
    assert calls == ["tags", "time_window", "eval_results"]


def test_trace_map_recovery_first_writer_wins_on_overlap(monkeypatch):
    """If two strategies return values for the same qid, the earlier
    strategy's value wins — preserves the ordered-preference contract
    (tags most authoritative, eval_results least).
    """

    def fake_tags(*args, **kwargs):
        return {"q1": "from-tags"}

    def fake_time_window(*args, **kwargs):
        return {"q1": "from-window", "q2": "t2"}

    def fake_eval_results(*args, **kwargs):
        return {"q1": "from-eval-results", "q3": "t3"}

    monkeypatch.setattr(ev, "_recover_trace_map_via_tags", fake_tags)
    monkeypatch.setattr(ev, "_recover_trace_map_via_time_window", fake_time_window)
    monkeypatch.setattr(ev, "_recover_trace_map_via_eval_results", fake_eval_results)

    with patch.object(ev.mlflow, "log_metric"):
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=0,
            expected_count=3,
            start_time_ms=1_700_000_000_000,
            eval_result=object(),
        )

    assert recovered["q1"] == "from-tags"
    assert recovered["q2"] == "t2"
    assert recovered["q3"] == "t3"


def test_trace_map_recovery_early_exits_when_fully_covered(monkeypatch):
    """If strategy 1 already covers ``expected_count``, later strategies
    must not be invoked — preserves the happy-path zero-extra-API-call
    cost that the original short-circuit guaranteed.
    """
    calls = []

    def fake_tags(*args, **kwargs):
        calls.append("tags")
        return {"q1": "t1", "q2": "t2"}

    def fake_time_window(*args, **kwargs):
        calls.append("time_window")
        return {"q_extra": "t_extra"}

    def fake_eval_results(*args, **kwargs):
        calls.append("eval_results")
        return {}

    monkeypatch.setattr(ev, "_recover_trace_map_via_tags", fake_tags)
    monkeypatch.setattr(ev, "_recover_trace_map_via_time_window", fake_time_window)
    monkeypatch.setattr(ev, "_recover_trace_map_via_eval_results", fake_eval_results)

    with patch.object(ev.mlflow, "log_metric"):
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=0,
            expected_count=2,
            start_time_ms=1_700_000_000_000,
            eval_result=object(),
        )

    assert recovered == {"q1": "t1", "q2": "t2"}
    assert calls == ["tags"]


def test_trace_map_recovery_metric_reports_new_hits_per_strategy(monkeypatch):
    """Per-strategy hit metric must count NEW qids contributed by that
    strategy, not raw returned size — otherwise a qid already covered by
    an earlier strategy would be double-counted when a later strategy
    also returns it, making ``sum(hits) > distinct recovered``.
    """

    def fake_tags(*args, **kwargs):
        return {"q1": "t1"}

    def fake_time_window(*args, **kwargs):
        # Returns q1 again (already covered — 0 new) + q2 (new).
        return {"q1": "t1-dup", "q2": "t2"}

    def fake_eval_results(*args, **kwargs):
        return {"q3": "t3"}

    monkeypatch.setattr(ev, "_recover_trace_map_via_tags", fake_tags)
    monkeypatch.setattr(ev, "_recover_trace_map_via_time_window", fake_time_window)
    monkeypatch.setattr(ev, "_recover_trace_map_via_eval_results", fake_eval_results)

    with patch.object(ev.mlflow, "log_metric") as log_metric:
        recovered = ev._recover_trace_map(
            experiment_id="exp-1",
            optimization_run_id="opt-1",
            iteration=0,
            expected_count=3,
            start_time_ms=1_700_000_000_000,
            eval_result=object(),
        )

    assert recovered == {"q1": "t1", "q2": "t2", "q3": "t3"}
    logged = {call.args[0]: call.args[1] for call in log_metric.call_args_list}
    assert logged["trace_map.recovery.tags.hit_count"] == 1.0
    assert logged["trace_map.recovery.time_window.hit_count"] == 1.0
    assert logged["trace_map.recovery.eval_results.hit_count"] == 1.0
