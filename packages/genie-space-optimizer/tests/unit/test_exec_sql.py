"""Unit tests for the _exec_sql() warehouse-primary / Spark-fallback helper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


@pytest.fixture
def mock_spark():
    spark = MagicMock(name="spark")
    spark.sql.return_value.toPandas.return_value = pd.DataFrame({"col": [1]})
    return spark


@pytest.fixture
def mock_ws():
    return MagicMock(name="workspace_client")


WAREHOUSE_ID = "wh-test-123"
MODULE = "genie_space_optimizer.optimization.evaluation"


class TestExecSqlWarehousePrimary:
    @patch(f"{MODULE}._execute_sql_via_warehouse")
    def test_warehouse_success_returns_dataframe(self, mock_wh, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.evaluation import _exec_sql

        expected = pd.DataFrame({"cnt": ["42"]})
        mock_wh.return_value = expected

        result = _exec_sql("SELECT 1", mock_spark, w=mock_ws, warehouse_id=WAREHOUSE_ID)

        mock_wh.assert_called_once()
        mock_spark.sql.assert_not_called()
        pd.testing.assert_frame_equal(result, expected)

    def test_no_warehouse_falls_back_to_spark(self, mock_spark):
        from genie_space_optimizer.optimization.evaluation import _exec_sql

        result = _exec_sql("SELECT 1", mock_spark)

        mock_spark.sql.assert_called_once_with("SELECT 1")
        assert isinstance(result, pd.DataFrame)

    @patch(f"{MODULE}._execute_sql_via_warehouse", side_effect=RuntimeError("warehouse down"))
    def test_warehouse_failure_falls_back_to_spark(self, mock_wh, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.evaluation import _exec_sql

        result = _exec_sql("SELECT 1", mock_spark, w=mock_ws, warehouse_id=WAREHOUSE_ID)

        mock_wh.assert_called_once()
        mock_spark.sql.assert_called_once_with("SELECT 1")
        assert isinstance(result, pd.DataFrame)

    @patch(f"{MODULE}._execute_sql_via_warehouse", side_effect=RuntimeError("warehouse down"))
    def test_both_fail_raises(self, mock_wh, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.evaluation import _exec_sql

        mock_spark.sql.side_effect = Exception("spark also down")

        with pytest.raises(Exception, match="spark also down"):
            _exec_sql("SELECT 1", mock_spark, w=mock_ws, warehouse_id=WAREHOUSE_ID)

    @patch(f"{MODULE}._execute_sql_via_warehouse")
    def test_returns_pandas_dataframe_type(self, mock_wh, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.evaluation import _exec_sql

        mock_wh.return_value = pd.DataFrame()
        result_wh = _exec_sql("SELECT 1", mock_spark, w=mock_ws, warehouse_id=WAREHOUSE_ID)
        assert isinstance(result_wh, pd.DataFrame)

        result_spark = _exec_sql("SELECT 1", mock_spark)
        assert isinstance(result_spark, pd.DataFrame)

    @patch(f"{MODULE}._execute_sql_via_warehouse")
    def test_catalog_schema_passed_to_warehouse(self, mock_wh, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.evaluation import _exec_sql

        mock_wh.return_value = pd.DataFrame()
        _exec_sql(
            "SELECT 1", mock_spark,
            w=mock_ws, warehouse_id=WAREHOUSE_ID,
            catalog="my_cat", schema="my_schema",
        )

        _, kwargs = mock_wh.call_args
        assert kwargs["catalog"] == "my_cat"
        assert kwargs["schema"] == "my_schema"


class TestExecSqlDataTypeCoercion:
    @patch(f"{MODULE}._execute_sql_via_warehouse")
    def test_warehouse_string_values_usable_as_int(self, mock_wh, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.evaluation import _exec_sql

        mock_wh.return_value = pd.DataFrame({"cnt": ["42"]})
        result = _exec_sql("SELECT COUNT(*)", mock_spark, w=mock_ws, warehouse_id=WAREHOUSE_ID)

        assert int(result.iloc[0]["cnt"]) == 42

    @patch(f"{MODULE}._execute_sql_via_warehouse")
    def test_warehouse_null_values(self, mock_wh, mock_spark, mock_ws):
        from genie_space_optimizer.optimization.evaluation import _exec_sql

        mock_wh.return_value = pd.DataFrame({"col": [None]})
        result = _exec_sql("SELECT NULL", mock_spark, w=mock_ws, warehouse_id=WAREHOUSE_ID)

        assert result.iloc[0]["col"] is None
