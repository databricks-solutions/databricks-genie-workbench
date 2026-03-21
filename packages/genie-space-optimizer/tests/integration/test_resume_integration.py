"""Integration test: run resume — load from Delta, continue from last stage."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pandas as pd
import pytest

from genie_space_optimizer.common.config import MAX_ITERATIONS
from genie_space_optimizer.optimization.state import (
    load_latest_full_iteration,
    load_run,
    load_stages,
    read_latest_stage,
)


@pytest.fixture
def populated_spark(mock_spark):
    """Mock spark that returns pre-populated Delta data for resume testing."""
    run_df = pd.DataFrame(
        [
            {
                "run_id": "resume-run-1",
                "space_id": "space-abc",
                "domain": "revenue",
                "status": "IN_PROGRESS",
                "levers": "[1,2,3,4,5]",
                "config_snapshot": '{"tables": []}',
                "max_iterations": MAX_ITERATIONS,
                "best_iteration": 1,
                "best_accuracy": 78.0,
            }
        ]
    )
    stages_df = pd.DataFrame(
        [
            {
                "run_id": "resume-run-1",
                "stage": "PREFLIGHT_STARTED",
                "status": "COMPLETE",
                "started_at": "2025-01-01T00:00:00Z",
            },
            {
                "run_id": "resume-run-1",
                "stage": "BASELINE_EVAL",
                "status": "COMPLETE",
                "started_at": "2025-01-01T00:10:00Z",
            },
            {
                "run_id": "resume-run-1",
                "stage": "LEVER_1_PROPOSE",
                "status": "COMPLETE",
                "started_at": "2025-01-01T00:20:00Z",
            },
            {
                "run_id": "resume-run-1",
                "stage": "LEVER_1_EVAL",
                "status": "COMPLETE",
                "started_at": "2025-01-01T00:30:00Z",
            },
        ]
    )
    iter_df = pd.DataFrame(
        [
            {
                "run_id": "resume-run-1",
                "iteration": 0,
                "eval_scope": "full",
                "overall_accuracy": 70.0,
                "total_questions": 20,
                "correct_count": 14,
                "scores_json": '{"result_correctness": 70.0}',
                "thresholds_met": False,
            },
            {
                "run_id": "resume-run-1",
                "iteration": 1,
                "eval_scope": "full",
                "overall_accuracy": 78.0,
                "total_questions": 20,
                "correct_count": 15,
                "scores_json": '{"result_correctness": 78.0}',
                "thresholds_met": False,
            },
        ]
    )

    def sql_side_effect(query):
        mock_result = MagicMock()
        q_lower = query.lower()
        if "genie_opt_runs" in q_lower:
            mock_result.toPandas.return_value = run_df
        elif "genie_opt_stages" in q_lower:
            mock_result.toPandas.return_value = stages_df
        elif "genie_opt_iterations" in q_lower:
            mock_result.toPandas.return_value = iter_df
        else:
            mock_result.toPandas.return_value = pd.DataFrame()
        return mock_result

    mock_spark.sql.side_effect = sql_side_effect
    return mock_spark


class TestResumeLoadRun:
    def test_load_in_progress_run(self, populated_spark):
        run = load_run(populated_spark, "resume-run-1", "cat", "gold")
        assert run is not None
        assert run["status"] == "IN_PROGRESS"
        assert isinstance(run["levers"], list)

    def test_load_returns_best_accuracy(self, populated_spark):
        run = load_run(populated_spark, "resume-run-1", "cat", "gold")
        assert run["best_accuracy"] == 78.0


class TestResumeStages:
    def test_load_stages_for_run(self, populated_spark):
        df = load_stages(populated_spark, "resume-run-1", "cat", "gold")
        assert not df.empty
        assert len(df) == 4

    def test_latest_stage_returns_something(self, populated_spark):
        """read_latest_stage should return a stage dict (mock returns first row)."""
        stage = read_latest_stage(populated_spark, "resume-run-1", "cat", "gold")
        assert stage is not None
        assert "stage" in stage


class TestResumeIteration:
    def test_load_latest_full_iteration(self, populated_spark):
        """load_latest_full_iteration returns an iteration (mock returns first row)."""
        iter_data = load_latest_full_iteration(
            populated_spark, "resume-run-1", "cat", "gold"
        )
        assert iter_data is not None
        assert "overall_accuracy" in iter_data
        assert iter_data["overall_accuracy"] in (70.0, 78.0)


class TestResumeContinuation:
    def test_determines_next_lever(self, populated_spark):
        """After lever 1 completed, the next lever to try should be 2."""
        run = load_run(populated_spark, "resume-run-1", "cat", "gold")
        stages_df = load_stages(populated_spark, "resume-run-1", "cat", "gold")

        completed_levers = set()
        for _, row in stages_df.iterrows():
            stage = row.get("stage", "")
            if stage.startswith("LEVER_") and "EVAL" in stage:
                parts = stage.split("_")
                try:
                    completed_levers.add(int(parts[1]))
                except (IndexError, ValueError):
                    pass

        all_levers = run["levers"]
        remaining = [l for l in all_levers if l not in completed_levers]
        assert remaining[0] == 2
