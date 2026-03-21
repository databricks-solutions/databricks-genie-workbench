"""Unit tests for benchmark deduplication and regeneration flag logic."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_benchmark(question: str, qid: str = "", provenance: str = "synthetic") -> dict:
    return {
        "id": qid or question.replace(" ", "_").lower()[:30],
        "question": question,
        "expected_sql": f"SELECT 1 -- {question}",
        "expected_asset": "TABLE",
        "category": "test",
        "required_tables": [],
        "required_columns": [],
        "source": "test",
        "provenance": provenance,
        "validation_status": "valid",
        "validation_reason_code": "ok",
        "validation_error": None,
        "correction_source": "",
    }


def _make_dataset_row(question: str, qid: str = "") -> dict:
    """Build a row dict as returned by ``spark.sql(...).toPandas().iterrows()``."""
    return {
        "inputs": json.dumps({
            "question_id": qid or question.replace(" ", "_").lower()[:30],
            "question": question,
            "space_id": "sp-1",
            "expected_sql": f"SELECT 1 -- {question}",
            "catalog": "cat",
            "gold_schema": "sch",
        }),
        "expectations": json.dumps({
            "expected_response": f"SELECT 1 -- {question}",
            "expected_asset": "TABLE",
            "category": "test",
            "source": "test",
            "provenance": "synthetic",
            "validation_status": "valid",
            "validation_reason_code": "ok",
            "validation_error": "",
            "correction_source": "",
        }),
    }


# ---------------------------------------------------------------------------
# _load_or_generate_benchmarks — regenerated flag
# ---------------------------------------------------------------------------

class TestLoadOrGenerateRegeneratedFlag:
    """Verify that ``_load_or_generate_benchmarks`` returns the correct
    ``regenerated`` boolean for each decision path."""

    @patch("genie_space_optimizer.optimization.preflight.load_benchmarks_from_dataset")
    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.validate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_reuse_returns_false(
        self, mock_write, mock_validate, mock_extract, mock_load,
    ):
        from genie_space_optimizer.optimization.preflight import (
            _load_or_generate_benchmarks,
        )

        curated = [_make_benchmark("Q1", provenance="curated")]
        mock_extract.return_value = curated

        existing = [_make_benchmark("Q1", qid="q1")] + [
            _make_benchmark(f"Q{i}", qid=f"q{i}") for i in range(2, 25)
        ]
        mock_load.return_value = existing
        mock_validate.return_value = [{"valid": True}] * len(existing)

        w = MagicMock()
        spark = MagicMock()
        benchmarks, regenerated = _load_or_generate_benchmarks(
            w, spark, {}, [], [], [], "dom", "cat", "sch", "cat.sch", "run-1",
        )
        assert regenerated is False
        assert len(benchmarks) >= 5

    @patch("genie_space_optimizer.optimization.preflight.load_benchmarks_from_dataset")
    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.generate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.validate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_topup_returns_false(
        self, mock_write, mock_validate, mock_gen, mock_extract, mock_load,
    ):
        from genie_space_optimizer.optimization.preflight import (
            _load_or_generate_benchmarks,
        )

        curated = [_make_benchmark("Q1", provenance="curated")]
        mock_extract.return_value = curated

        existing = [_make_benchmark("Q1", qid="q1")] + [
            _make_benchmark(f"Q{i}", qid=f"q{i}") for i in range(2, 8)
        ]
        mock_load.return_value = existing
        mock_validate.return_value = [{"valid": True}] * len(existing)

        topped_up = existing + [_make_benchmark(f"Q{i}") for i in range(8, 25)]
        mock_gen.return_value = topped_up

        w = MagicMock()
        spark = MagicMock()
        benchmarks, regenerated = _load_or_generate_benchmarks(
            w, spark, {}, [], [], [], "dom", "cat", "sch", "cat.sch", "run-1",
        )
        assert regenerated is False

    @patch("genie_space_optimizer.optimization.preflight.load_benchmarks_from_dataset")
    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.generate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_generate_returns_true(
        self, mock_write, mock_gen, mock_extract, mock_load,
    ):
        from genie_space_optimizer.optimization.preflight import (
            _load_or_generate_benchmarks,
        )

        mock_extract.return_value = [_make_benchmark("Q1", provenance="curated")]
        mock_load.return_value = []
        mock_gen.return_value = [_make_benchmark(f"Q{i}") for i in range(1, 25)]

        w = MagicMock()
        spark = MagicMock()
        benchmarks, regenerated = _load_or_generate_benchmarks(
            w, spark, {}, [], [], [], "dom", "cat", "sch", "cat.sch", "run-1",
        )
        assert regenerated is True

    @patch("genie_space_optimizer.optimization.preflight.load_benchmarks_from_dataset")
    @patch("genie_space_optimizer.optimization.preflight.extract_genie_space_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.generate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.validate_benchmarks")
    @patch("genie_space_optimizer.optimization.preflight.write_stage")
    def test_regenerate_missing_curated_returns_true(
        self, mock_write, mock_validate, mock_gen, mock_extract, mock_load,
    ):
        from genie_space_optimizer.optimization.preflight import (
            _load_or_generate_benchmarks,
        )

        curated = [
            _make_benchmark("Curated Q1", provenance="curated"),
            _make_benchmark("Curated Q2", provenance="curated"),
        ]
        mock_extract.return_value = curated

        existing = [_make_benchmark(f"Q{i}", qid=f"q{i}") for i in range(1, 25)]
        mock_load.return_value = existing
        mock_validate.return_value = [{"valid": True}] * len(existing)

        mock_gen.return_value = [_make_benchmark(f"Q{i}") for i in range(1, 25)]

        w = MagicMock()
        spark = MagicMock()
        benchmarks, regenerated = _load_or_generate_benchmarks(
            w, spark, {}, [], [], [], "dom", "cat", "sch", "cat.sch", "run-1",
        )
        assert regenerated is True


# ---------------------------------------------------------------------------
# create_evaluation_dataset — write-side dedup
# ---------------------------------------------------------------------------

class TestCreateEvaluationDatasetDedup:
    """Verify that ``create_evaluation_dataset`` deduplicates benchmarks
    by question text before calling ``merge_records``."""

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_duplicate_questions_are_dropped(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import (
            create_evaluation_dataset,
        )

        mock_dataset = MagicMock()
        mock_mlflow.genai.datasets.get_dataset.return_value = mock_dataset

        benchmarks = [
            _make_benchmark("What is total revenue?", qid="dom_gs_001"),
            _make_benchmark("What is total revenue?", qid="dom_007"),
            _make_benchmark("Show top orders", qid="dom_gs_002"),
        ]

        spark = MagicMock()
        create_evaluation_dataset(
            spark, benchmarks, "cat.sch", "dom",
            space_id="sp-1", catalog="cat", gold_schema="sch",
        )

        mock_dataset.merge_records.assert_called_once()
        records = mock_dataset.merge_records.call_args[0][0]
        assert len(records) == 2
        questions = [r["inputs"]["question"] for r in records]
        assert "What is total revenue?" in questions
        assert "Show top orders" in questions

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_no_duplicates_passes_all(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import (
            create_evaluation_dataset,
        )

        mock_dataset = MagicMock()
        mock_mlflow.genai.datasets.get_dataset.return_value = mock_dataset

        benchmarks = [
            _make_benchmark("Q1", qid="id1"),
            _make_benchmark("Q2", qid="id2"),
            _make_benchmark("Q3", qid="id3"),
        ]

        spark = MagicMock()
        create_evaluation_dataset(
            spark, benchmarks, "cat.sch", "dom",
            space_id="sp-1", catalog="cat", gold_schema="sch",
        )

        records = mock_dataset.merge_records.call_args[0][0]
        assert len(records) == 3

    @patch("genie_space_optimizer.optimization.evaluation.mlflow")
    def test_case_insensitive_dedup(self, mock_mlflow):
        from genie_space_optimizer.optimization.evaluation import (
            create_evaluation_dataset,
        )

        mock_dataset = MagicMock()
        mock_mlflow.genai.datasets.get_dataset.return_value = mock_dataset

        benchmarks = [
            _make_benchmark("What Is Revenue?", qid="id1"),
            _make_benchmark("what is revenue?", qid="id2"),
        ]

        spark = MagicMock()
        create_evaluation_dataset(
            spark, benchmarks, "cat.sch", "dom",
            space_id="sp-1", catalog="cat", gold_schema="sch",
        )

        records = mock_dataset.merge_records.call_args[0][0]
        assert len(records) == 1


# ---------------------------------------------------------------------------
# load_benchmarks_from_dataset — read-side dedup
# ---------------------------------------------------------------------------

class TestLoadBenchmarksDedup:
    """Verify that ``load_benchmarks_from_dataset`` deduplicates by question."""

    def _mock_spark_with_rows(self, rows: list[dict]) -> MagicMock:
        df = pd.DataFrame(rows)
        spark = MagicMock()
        spark.sql.return_value.toPandas.return_value = df
        return spark

    def test_duplicates_removed_on_load(self):
        from genie_space_optimizer.optimization.evaluation import (
            load_benchmarks_from_dataset,
        )

        rows = [
            _make_dataset_row("What is revenue?", "dom_gs_001"),
            _make_dataset_row("What is revenue?", "dom_007"),
            _make_dataset_row("Show orders", "dom_008"),
        ]
        spark = self._mock_spark_with_rows(rows)

        with patch("genie_space_optimizer.optimization.evaluation.load_benchmarks_from_dataset.__module__"):
            pass

        result = load_benchmarks_from_dataset(spark, "cat.sch", "dom")
        assert len(result) == 2
        questions = [b["question"] for b in result]
        assert "What is revenue?" in questions
        assert "Show orders" in questions

    def test_no_duplicates_unchanged(self):
        from genie_space_optimizer.optimization.evaluation import (
            load_benchmarks_from_dataset,
        )

        rows = [
            _make_dataset_row("Q1", "id1"),
            _make_dataset_row("Q2", "id2"),
            _make_dataset_row("Q3", "id3"),
        ]
        spark = self._mock_spark_with_rows(rows)
        result = load_benchmarks_from_dataset(spark, "cat.sch", "dom")
        assert len(result) == 3

    def test_case_insensitive_dedup_on_load(self):
        from genie_space_optimizer.optimization.evaluation import (
            load_benchmarks_from_dataset,
        )

        rows = [
            _make_dataset_row("Revenue Query", "id1"),
            _make_dataset_row("revenue query", "id2"),
        ]
        spark = self._mock_spark_with_rows(rows)
        result = load_benchmarks_from_dataset(spark, "cat.sch", "dom")
        assert len(result) == 1
