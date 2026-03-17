"""Unit tests for genie_space_optimizer.optimization.state — Delta state machine."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from genie_space_optimizer.optimization.state import (
    load_recent_activity,
    load_run,
    write_stage,
)


class TestWriteStage:
    def test_write_stage_calls_spark_sql(self, mock_spark):
        write_stage(
            mock_spark,
            run_id="run-1",
            stage="PREFLIGHT_STARTED",
            status="STARTED",
            task_key="preflight",
            catalog="cat",
            schema="sch",
        )
        mock_spark.sql.assert_called()
        sql = mock_spark.sql.call_args[0][0]
        assert "INSERT INTO" in sql
        assert "genie_opt_stages" in sql
        assert "run-1" in sql
        assert "PREFLIGHT_STARTED" in sql

    def test_write_stage_with_detail(self, mock_spark):
        write_stage(
            mock_spark,
            run_id="run-1",
            stage="PREFLIGHT_COMPLETE",
            status="COMPLETE",
            detail={"table_count": 5},
            catalog="cat",
            schema="sch",
        )
        sql = mock_spark.sql.call_args[0][0]
        assert "table_count" in sql

    def test_write_stage_with_lever(self, mock_spark):
        write_stage(
            mock_spark,
            run_id="run-1",
            stage="LEVER_2_PROPOSE",
            status="STARTED",
            lever=2,
            iteration=1,
            catalog="cat",
            schema="sch",
        )
        sql = mock_spark.sql.call_args[0][0]
        assert "2" in sql

    def test_write_stage_with_error(self, mock_spark):
        write_stage(
            mock_spark,
            run_id="run-1",
            stage="LEVER_3_EVAL",
            status="FAILED",
            error_message="timeout exceeded",
            catalog="cat",
            schema="sch",
        )
        sql = mock_spark.sql.call_args[0][0]
        assert "timeout exceeded" in sql

    def test_complete_status_computes_duration(self, mock_spark):
        """COMPLETE status should query for the matching STARTED row."""
        started_df = pd.DataFrame({"started_at": ["2025-01-01T00:00:00+00:00"]})
        mock_spark.sql.return_value.toPandas.return_value = started_df

        write_stage(
            mock_spark,
            run_id="run-1",
            stage="PREFLIGHT",
            status="COMPLETE",
            catalog="cat",
            schema="sch",
        )
        assert mock_spark.sql.call_count >= 2


class TestLoadRun:
    def test_returns_none_when_empty(self, mock_spark):
        mock_spark.sql.return_value.toPandas.return_value = pd.DataFrame()
        result = load_run(mock_spark, "nonexistent", "cat", "sch")
        assert result is None

    def test_returns_dict_when_found(self, mock_spark):
        df = pd.DataFrame(
            [
                {
                    "run_id": "run-1",
                    "space_id": "space-abc",
                    "status": "CONVERGED",
                    "levers": "[1,2,3]",
                    "config_snapshot": '{"tables": []}',
                }
            ]
        )
        mock_spark.sql.return_value.toPandas.return_value = df
        result = load_run(mock_spark, "run-1", "cat", "sch")
        assert result is not None
        assert result["run_id"] == "run-1"
        assert isinstance(result["levers"], list)
        assert isinstance(result["config_snapshot"], dict)


class TestLoadRecentActivity:
    def test_returns_dataframe(self, mock_spark):
        mock_spark.sql.return_value.toPandas.return_value = pd.DataFrame(
            [{"run_id": "r1", "space_id": "s1", "status": "CONVERGED"}]
        )
        df = load_recent_activity(mock_spark, "cat", "sch")
        assert not df.empty

    def test_space_id_filter(self, mock_spark):
        mock_spark.sql.return_value.toPandas.return_value = pd.DataFrame()
        load_recent_activity(mock_spark, "cat", "sch", space_id="space-1")
        sql = mock_spark.sql.call_args[0][0]
        assert "space-1" in sql

    def test_no_space_id_filter(self, mock_spark):
        mock_spark.sql.return_value.toPandas.return_value = pd.DataFrame()
        load_recent_activity(mock_spark, "cat", "sch")
        sql = mock_spark.sql.call_args[0][0]
        assert "WHERE" not in sql or "space_id" not in sql
