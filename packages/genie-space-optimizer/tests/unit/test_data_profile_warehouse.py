"""Unit tests for _collect_data_profile and _compute_join_overlaps warehouse routing."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

MODULE = "genie_space_optimizer.optimization.preflight"
EVAL_MODULE = "genie_space_optimizer.optimization.evaluation"

WAREHOUSE_ID = "wh-test-123"


def _make_uc_columns(table: str, cols: list[tuple[str, str]]) -> list[dict]:
    return [
        {"table_name": table, "column_name": name, "data_type": dtype}
        for name, dtype in cols
    ]


# ---------------------------------------------------------------------------
# _collect_data_profile
# ---------------------------------------------------------------------------


class TestCollectDataProfileWarehouse:
    """Verify _collect_data_profile handles warehouse data types correctly."""

    @patch(f"{EVAL_MODULE}._exec_sql")
    def test_collect_set_json_string_parsed(self, mock_exec):
        """Warehouse returns COLLECT_SET as a JSON string — must be parsed."""
        from genie_space_optimizer.optimization.preflight import _collect_data_profile

        count_df = pd.DataFrame({"cnt": ["5"]})
        stats_df = pd.DataFrame({"_card_status": ["3"]})
        dv_df = pd.DataFrame({"vals": ['["active","closed","pending"]']})
        mock_exec.side_effect = [count_df, stats_df, dv_df]

        uc_cols = _make_uc_columns("cat.schema.orders", [("status", "string")])
        result = _collect_data_profile(
            MagicMock(), ["cat.schema.orders"], uc_cols,
            max_tables=1, sample_size=100, low_cardinality_threshold=10,
            w=MagicMock(), warehouse_id=WAREHOUSE_ID, catalog="cat", schema="schema",
        )

        profile = result["cat.schema.orders"]
        assert profile["columns"]["status"]["distinct_values"] == ["active", "closed", "pending"]

    @patch(f"{EVAL_MODULE}._exec_sql")
    def test_collect_set_native_list_still_works(self, mock_exec):
        """Spark returns COLLECT_SET as a native list — backward compat."""
        from genie_space_optimizer.optimization.preflight import _collect_data_profile

        count_df = pd.DataFrame({"cnt": [5]})
        stats_df = pd.DataFrame({"_card_status": [3]})
        dv_df = pd.DataFrame({"vals": [["active", "closed", "pending"]]})
        mock_exec.side_effect = [count_df, stats_df, dv_df]

        uc_cols = _make_uc_columns("cat.schema.orders", [("status", "string")])
        result = _collect_data_profile(
            MagicMock(), ["cat.schema.orders"], uc_cols,
            max_tables=1, sample_size=100, low_cardinality_threshold=10,
        )

        profile = result["cat.schema.orders"]
        assert profile["columns"]["status"]["distinct_values"] == ["active", "closed", "pending"]

    @patch(f"{EVAL_MODULE}._exec_sql")
    def test_count_returns_integer(self, mock_exec):
        """Warehouse returns count as string — must become int."""
        from genie_space_optimizer.optimization.preflight import _collect_data_profile

        count_df = pd.DataFrame({"cnt": ["1000"]})
        stats_df = pd.DataFrame({
            "_card_amount": ["42"],
            "_min_amount": ["10.5"],
            "_max_amount": ["999.9"],
        })
        mock_exec.side_effect = [count_df, stats_df]

        uc_cols = _make_uc_columns("cat.schema.orders", [("amount", "double")])
        result = _collect_data_profile(
            MagicMock(), ["cat.schema.orders"], uc_cols,
            max_tables=1, sample_size=100, low_cardinality_threshold=10,
            w=MagicMock(), warehouse_id=WAREHOUSE_ID, catalog="cat", schema="schema",
        )

        profile = result["cat.schema.orders"]
        assert profile["row_count"] == 1000
        assert isinstance(profile["row_count"], int)

    @patch(f"{EVAL_MODULE}._exec_sql")
    def test_cardinality_returns_integer(self, mock_exec):
        """Warehouse cardinality string is cast to int."""
        from genie_space_optimizer.optimization.preflight import _collect_data_profile

        count_df = pd.DataFrame({"cnt": ["100"]})
        stats_df = pd.DataFrame({"_card_status": ["7"]})
        dv_df = pd.DataFrame({"vals": ['["a","b","c","d","e","f","g"]']})
        mock_exec.side_effect = [count_df, stats_df, dv_df]

        uc_cols = _make_uc_columns("cat.schema.orders", [("status", "string")])
        result = _collect_data_profile(
            MagicMock(), ["cat.schema.orders"], uc_cols,
            max_tables=1, sample_size=100, low_cardinality_threshold=10,
            w=MagicMock(), warehouse_id=WAREHOUSE_ID, catalog="cat", schema="schema",
        )

        profile = result["cat.schema.orders"]
        assert profile["columns"]["status"]["cardinality"] == 7
        assert isinstance(profile["columns"]["status"]["cardinality"], int)

    @patch(f"{EVAL_MODULE}._exec_sql")
    def test_min_max_stringified(self, mock_exec):
        """Min/max are already stringified, works for both Spark and warehouse."""
        from genie_space_optimizer.optimization.preflight import _collect_data_profile

        count_df = pd.DataFrame({"cnt": ["50"]})
        stats_df = pd.DataFrame({
            "_card_amount": ["42"],
            "_min_amount": ["100"],
            "_max_amount": ["999"],
        })
        mock_exec.side_effect = [count_df, stats_df]

        uc_cols = _make_uc_columns("cat.schema.orders", [("amount", "double")])
        result = _collect_data_profile(
            MagicMock(), ["cat.schema.orders"], uc_cols,
            max_tables=1, sample_size=100, low_cardinality_threshold=10,
            w=MagicMock(), warehouse_id=WAREHOUSE_ID, catalog="cat", schema="schema",
        )

        col_info = result["cat.schema.orders"]["columns"]["amount"]
        assert col_info["min"] == "100"
        assert col_info["max"] == "999"

    @patch(f"{EVAL_MODULE}._exec_sql")
    def test_without_warehouse_id_uses_spark(self, mock_exec):
        """When no warehouse_id, _exec_sql is called without warehouse params."""
        from genie_space_optimizer.optimization.preflight import _collect_data_profile

        count_df = pd.DataFrame({"cnt": [10]})
        stats_df = pd.DataFrame({
            "_card_col1": [5],
            "_min_col1": [0],
            "_max_col1": [100],
        })
        mock_exec.side_effect = [count_df, stats_df]

        uc_cols = _make_uc_columns("cat.schema.t", [("col1", "double")])
        _collect_data_profile(
            MagicMock(), ["cat.schema.t"], uc_cols,
            max_tables=1, sample_size=100, low_cardinality_threshold=10,
        )

        for call in mock_exec.call_args_list:
            assert call.kwargs.get("warehouse_id", "") == ""


# ---------------------------------------------------------------------------
# _compute_join_overlaps
# ---------------------------------------------------------------------------


class TestComputeJoinOverlapsWarehouse:
    """Verify _compute_join_overlaps handles warehouse data types correctly."""

    @patch(f"{EVAL_MODULE}._exec_sql")
    def test_overlap_ratio_with_warehouse(self, mock_exec):
        """Warehouse returns string numerics — int coercion must produce correct ratio."""
        from genie_space_optimizer.optimization.preflight import _compute_join_overlaps

        mock_exec.return_value = pd.DataFrame({
            "left_distinct": ["10"],
            "right_distinct": ["10"],
            "overlap": ["8"],
        })

        fk = [{
            "table_name": "cat.schema.orders",
            "referenced_table": "cat.schema.customers",
            "column_name": "customer_id",
            "referenced_column": "customer_id",
        }]
        result = _compute_join_overlaps(
            MagicMock(), fk,
            w=MagicMock(), warehouse_id=WAREHOUSE_ID, catalog="cat", schema="schema",
        )

        assert len(result) == 1
        assert result[0]["overlap_ratio"] == 0.8

    @patch(f"{EVAL_MODULE}._exec_sql")
    def test_string_division_handled(self, mock_exec):
        """Edge case: '0' left_distinct should not cause ZeroDivisionError."""
        from genie_space_optimizer.optimization.preflight import _compute_join_overlaps

        mock_exec.return_value = pd.DataFrame({
            "left_distinct": ["0"],
            "right_distinct": ["5"],
            "overlap": ["0"],
        })

        fk = [{
            "table_name": "cat.schema.orders",
            "referenced_table": "cat.schema.customers",
            "column_name": "customer_id",
            "referenced_column": "customer_id",
        }]
        result = _compute_join_overlaps(
            MagicMock(), fk,
            w=MagicMock(), warehouse_id=WAREHOUSE_ID,
        )

        assert len(result) == 1
        assert result[0]["overlap_ratio"] == 0.0

    @patch(f"{EVAL_MODULE}._exec_sql")
    def test_without_warehouse_id_uses_spark(self, mock_exec):
        """When no warehouse_id, _exec_sql is called without warehouse params."""
        from genie_space_optimizer.optimization.preflight import _compute_join_overlaps

        mock_exec.return_value = pd.DataFrame({
            "left_distinct": [10],
            "right_distinct": [10],
            "overlap": [8],
        })

        fk = [{
            "table_name": "cat.schema.orders",
            "referenced_table": "cat.schema.customers",
            "column_name": "customer_id",
            "referenced_column": "customer_id",
        }]
        _compute_join_overlaps(MagicMock(), fk)

        for call in mock_exec.call_args_list:
            assert call.kwargs.get("warehouse_id", "") == ""
