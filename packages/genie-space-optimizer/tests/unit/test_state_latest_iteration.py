from __future__ import annotations

import pandas as pd


def test_load_latest_full_iteration_can_exclude_candidate_iteration(monkeypatch) -> None:
    from genie_space_optimizer.optimization import state

    captured_sql: dict[str, str] = {}

    def fake_run_query(_spark, sql: str):
        captured_sql["sql"] = sql
        return pd.DataFrame([{
            "run_id": "run-1",
            "iteration": 0,
            "eval_scope": "full",
            "scores_json": "{}",
            "failures_json": "[]",
            "remaining_failures": "[]",
            "arbiter_actions_json": "[]",
            "repeatability_json": "null",
            "rows_json": '[{"question_id": "q_base"}]',
        }])

    monkeypatch.setattr(state, "run_query", fake_run_query)

    row = state.load_latest_full_iteration(
        object(),
        "run-1",
        "cat",
        "sch",
        before_iteration=1,
    )

    assert row is not None
    assert row["iteration"] == 0
    assert row["rows_json"] == [{"question_id": "q_base"}]
    assert "iteration < 1" in captured_sql["sql"]
    assert "ORDER BY iteration DESC" in captured_sql["sql"]


def test_load_latest_full_iteration_keeps_existing_query_when_no_before_iteration(monkeypatch) -> None:
    from genie_space_optimizer.optimization import state

    captured_sql: dict[str, str] = {}

    def fake_run_query(_spark, sql: str):
        captured_sql["sql"] = sql
        return pd.DataFrame()

    monkeypatch.setattr(state, "run_query", fake_run_query)

    row = state.load_latest_full_iteration(object(), "run-1", "cat", "sch")

    assert row is None
    assert "iteration <" not in captured_sql["sql"]
    assert "rolled_back IS NULL OR rolled_back = false" in captured_sql["sql"]
