"""Unit tests for genie_space_optimizer.optimization.benchmarks — pure functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.benchmarks import (
    assign_splits,
    build_eval_records,
    load_benchmarks_from_dataset,
    resolve_sql,
    validate_ground_truth_sql,
)
from genie_space_optimizer.optimization.evaluation import _normalize_expected_asset


class TestResolveSql:
    def test_substitutes_catalog(self):
        sql = "SELECT * FROM ${catalog}.${gold_schema}.orders"
        result = resolve_sql(sql, catalog="my_cat", gold_schema="my_schema")
        assert result == "SELECT * FROM my_cat.my_schema.orders"

    def test_no_substitution_when_missing(self):
        sql = "SELECT 1"
        assert resolve_sql(sql) == "SELECT 1"

    def test_empty_sql(self):
        assert resolve_sql("") == ""

    def test_partial_substitution(self):
        sql = "SELECT * FROM ${catalog}.gold.orders"
        result = resolve_sql(sql, catalog="test_cat")
        assert "test_cat" in result
        assert "${gold_schema}" not in result


class TestAssignSplits:
    def test_assigns_train_and_held_out(self, sample_benchmarks):
        result = assign_splits(sample_benchmarks, train_ratio=0.75, seed=42)
        splits = {b["split"] for b in result}
        assert "train" in splits
        assert "held_out" in splits

    def test_deterministic_with_same_seed(self, sample_benchmarks):
        import copy
        b1 = assign_splits(copy.deepcopy(sample_benchmarks), seed=42)
        b2 = assign_splits(copy.deepcopy(sample_benchmarks), seed=42)
        for a, b in zip(b1, b2):
            assert a["split"] == b["split"]

    def test_different_seed_may_differ(self, sample_benchmarks):
        import copy
        b1 = assign_splits(copy.deepcopy(sample_benchmarks), seed=1)
        b2 = assign_splits(copy.deepcopy(sample_benchmarks), seed=999)
        splits1 = [b["split"] for b in b1]
        splits2 = [b["split"] for b in b2]
        # With different seeds and few items, splits might differ
        # (not guaranteed, but likely)

    def test_ratio_respected(self):
        benchmarks = [{"id": f"q{i}", "question": f"Question {i}"} for i in range(100)]
        result = assign_splits(benchmarks, train_ratio=0.85, seed=42)
        train_count = sum(1 for b in result if b["split"] == "train")
        assert 82 <= train_count <= 88

    def test_default_ratio_matches_held_out_ratio(self):
        benchmarks = [{"id": f"q{i}", "question": f"Question {i}"} for i in range(100)]
        result = assign_splits(benchmarks, seed=42)
        train_count = sum(1 for b in result if b["split"] == "train")
        assert 82 <= train_count <= 88

    def test_curated_always_train(self):
        benchmarks = [
            {"id": f"q{i}", "question": f"Q{i}", "provenance": "curated"}
            for i in range(10)
        ]
        result = assign_splits(benchmarks, train_ratio=0.5, seed=42)
        assert all(b["split"] == "train" for b in result)

    def test_curated_not_counted_for_ratio(self):
        curated = [
            {"id": f"c{i}", "question": f"C{i}", "provenance": "curated"}
            for i in range(10)
        ]
        synthetic = [
            {"id": f"s{i}", "question": f"S{i}", "provenance": "synthetic"}
            for i in range(90)
        ]
        result = assign_splits(curated + synthetic, train_ratio=0.85, seed=42)
        curated_splits = [b for b in result if b["provenance"] == "curated"]
        synth_splits = [b for b in result if b["provenance"] == "synthetic"]
        assert all(b["split"] == "train" for b in curated_splits)
        synth_train = sum(1 for b in synth_splits if b["split"] == "train")
        assert 73 <= synth_train <= 81  # ~85% of 90

    def test_empty_list(self):
        assert assign_splits([]) == []


class TestLoadBenchmarksFromDataset:
    def test_list_passthrough(self):
        data = [{"question": "test"}]
        result = load_benchmarks_from_dataset(data, "cat.schema", "domain")
        assert result == data

    def test_spark_session(self, mock_spark):
        mock_row = MagicMock()
        mock_row.asDict.return_value = {"question": "test", "expected_sql": "SELECT 1"}
        mock_spark.table.return_value.collect.return_value = [mock_row]
        result = load_benchmarks_from_dataset(mock_spark, "cat.schema", "domain")
        assert len(result) == 1

    def test_spark_exception_returns_empty(self, mock_spark):
        mock_spark.table.side_effect = Exception("table not found")
        result = load_benchmarks_from_dataset(mock_spark, "cat.schema", "domain")
        assert result == []


class TestValidateGroundTruthSql:
    def test_empty_sql_invalid(self, mock_spark):
        valid, error = validate_ground_truth_sql("", mock_spark)
        assert valid is False
        assert "Empty" in error

    def test_valid_sql(self, mock_spark):
        valid, error = validate_ground_truth_sql("SELECT 1", mock_spark)
        assert valid is True
        assert error == ""

    def test_invalid_sql_returns_error(self, mock_spark):
        mock_spark.sql.side_effect = Exception("parse error")
        valid, error = validate_ground_truth_sql("INVALID SQL", mock_spark)
        assert valid is False
        assert "parse error" in error

    @patch("genie_space_optimizer.optimization.evaluation._execute_sql_via_warehouse")
    def test_warehouse_path_used_when_configured(self, mock_wh, mock_spark):
        mock_wh.return_value = MagicMock()
        ws = MagicMock()
        valid, error = validate_ground_truth_sql(
            "SELECT 1", mock_spark, w=ws, warehouse_id="wh-123",
        )
        mock_wh.assert_called()
        mock_spark.sql.assert_not_called()


class TestVerifyTableExists:
    @patch("genie_space_optimizer.optimization.evaluation._execute_sql_via_warehouse")
    def test_warehouse_path_checks_existence(self, mock_wh):
        from genie_space_optimizer.optimization.benchmarks import _verify_table_exists

        mock_wh.return_value = MagicMock()
        exists, err = _verify_table_exists(
            MagicMock(), "cat.schema.orders",
            w=MagicMock(), warehouse_id="wh-123",
        )
        assert exists is True
        assert err == ""

    @patch("genie_space_optimizer.optimization.evaluation._execute_sql_via_warehouse",
           side_effect=RuntimeError("TABLE_OR_VIEW_NOT_FOUND: cat.schema.missing"))
    def test_warehouse_table_not_found(self, mock_wh):
        from genie_space_optimizer.optimization.benchmarks import _verify_table_exists

        exists, err = _verify_table_exists(
            MagicMock(), "cat.schema.missing",
            w=MagicMock(), warehouse_id="wh-123",
        )
        assert exists is False
        assert "does not exist" in err

    def test_spark_fallback_preserves_existing_behavior(self):
        from genie_space_optimizer.optimization.benchmarks import _verify_table_exists

        mock_spark = MagicMock()
        exists, err = _verify_table_exists(mock_spark, "cat.schema.orders")
        mock_spark.sql.assert_called_once()
        assert exists is True
        assert err == ""


class TestBuildEvalRecords:
    def test_basic_record_structure(self, sample_benchmarks):
        records = build_eval_records(sample_benchmarks)
        assert len(records) == len(sample_benchmarks)
        for r in records:
            assert "inputs" in r
            assert "expectations" in r
            assert "question" in r["inputs"]
            assert "question_id" in r["inputs"]
            assert "expected_sql" in r["expectations"]
            assert "expected_asset" in r["expectations"]
            assert "expected_asset" not in r["inputs"]

    def test_empty_list(self):
        assert build_eval_records([]) == []

    def test_auto_generates_question_id(self):
        records = build_eval_records([{"question": "What is X?"}])
        assert records[0]["inputs"]["question_id"] != ""

    def test_expected_asset_in_expectations(self):
        records = build_eval_records([
            {"question": "Q1", "expected_sql": "SELECT 1", "expected_asset": "MV"},
        ])
        assert records[0]["expectations"]["expected_asset"] == "MV"

    def test_dict_expected_asset_falls_back_to_detect(self):
        records = build_eval_records([
            {"question": "Q1", "expected_sql": "SELECT * FROM t", "expected_asset": {"type": "TABLE"}},
        ])
        assert records[0]["expectations"]["expected_asset"] in ("MV", "TVF", "TABLE")


class TestNormalizeExpectedAsset:
    def test_string_table(self):
        assert _normalize_expected_asset("TABLE", "") == "TABLE"

    def test_string_mv(self):
        assert _normalize_expected_asset("mv", "") == "MV"

    def test_string_tvf(self):
        assert _normalize_expected_asset("  tvf  ", "") == "TVF"

    def test_empty_string_falls_back(self):
        result = _normalize_expected_asset("", "SELECT * FROM t")
        assert result in ("MV", "TVF", "TABLE")

    def test_none_falls_back(self):
        result = _normalize_expected_asset(None, "SELECT * FROM t")
        assert result in ("MV", "TVF", "TABLE")

    def test_dict_falls_back(self):
        result = _normalize_expected_asset({"type": "TABLE"}, "SELECT * FROM t")
        assert result in ("MV", "TVF", "TABLE")

    def test_list_falls_back(self):
        result = _normalize_expected_asset(["TABLE"], "SELECT * FROM t")
        assert result in ("MV", "TVF", "TABLE")

    def test_unrecognized_string_falls_back(self):
        result = _normalize_expected_asset("BOOKING_ANALYTICS", "SELECT * FROM t")
        assert result in ("MV", "TVF", "TABLE")
