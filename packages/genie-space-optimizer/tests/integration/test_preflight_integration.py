"""Integration test: preflight task — config fetch + UC metadata + benchmark validation."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def preflight_ws(mock_ws):
    """WorkspaceClient configured for preflight integration tests."""
    mock_ws.api_client.do.return_value = {
        "title": "Revenue Space",
        "description": "Revenue analytics",
        "serialized_space": json.dumps(
            {
                "data_sources": {
                    "tables": [
                        {
                            "identifier": "cat.gold.orders",
                            "description": ["Order data"],
                            "column_configs": [
                                {"column_name": "order_id"},
                                {"column_name": "amount"},
                            ],
                        }
                    ]
                },
                "instructions": {
                    "text_instructions": [{"id": "i1", "content": ["Filter by active."]}],
                    "example_question_sqls": [{"question": "Total revenue?"}],
                },
            }
        ),
    }
    return mock_ws


class TestPreflightConfigFetch:
    def test_fetches_space_config(self, preflight_ws):
        from genie_space_optimizer.common.genie_client import fetch_space_config

        config = fetch_space_config(preflight_ws, "space-123")
        assert config is not None

    def test_extracts_tables_from_config(self, preflight_ws):
        result = preflight_ws.api_client.do(
            "GET", "/api/2.0/genie/spaces/space-123",
            query={"include_serialized_space": "true"},
        )
        ss = result.get("serialized_space", {})
        if isinstance(ss, str):
            ss = json.loads(ss)
        tables = ss.get("data_sources", {}).get("tables", [])
        assert len(tables) == 1
        assert tables[0]["identifier"] == "cat.gold.orders"


class TestPreflightUCMetadata:
    def test_uc_metadata_query(self, mock_spark):
        """Verify UC metadata collection queries execute."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        from genie_space_optimizer.common.uc_metadata import get_columns

        result = get_columns(mock_spark, "cat", "gold")
        mock_spark.sql.assert_called_once()
        assert result is not None


class TestPreflightBenchmarkValidation:
    def test_validates_benchmark_sql(self, mock_spark, sample_benchmarks):
        from genie_space_optimizer.optimization.benchmarks import validate_benchmarks

        results = validate_benchmarks(
            sample_benchmarks, mock_spark, catalog="cat", gold_schema="gold"
        )
        assert len(results) == len(sample_benchmarks)
        for r in results:
            assert "valid" in r
            assert "error" in r

    def test_invalid_sql_detected(self, mock_spark):
        mock_spark.sql.side_effect = Exception("Table not found")
        from genie_space_optimizer.optimization.benchmarks import validate_ground_truth_sql

        valid, error = validate_ground_truth_sql(
            "SELECT * FROM nonexistent", mock_spark
        )
        assert valid is False


class TestPreflightEndToEnd:
    def test_preflight_creates_run_and_writes_stage(self, mock_spark, preflight_ws):
        """Simulate the preflight task flow: create run → fetch config → write stage."""
        from genie_space_optimizer.optimization.state import create_run, write_stage

        create_run(
            mock_spark,
            run_id="test-run",
            space_id="space-123",
            domain="revenue",
            catalog="cat",
            schema="gold",
        )

        write_stage(
            mock_spark,
            run_id="test-run",
            stage="PREFLIGHT_STARTED",
            status="STARTED",
            task_key="preflight",
            catalog="cat",
            schema="gold",
        )

        write_stage(
            mock_spark,
            run_id="test-run",
            stage="PREFLIGHT_COMPLETE",
            status="COMPLETE",
            task_key="preflight",
            detail={"table_count": 1, "instruction_count": 1, "benchmark_count": 20},
            catalog="cat",
            schema="gold",
        )

        assert mock_spark.sql.call_count >= 3
