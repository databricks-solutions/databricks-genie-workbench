"""Integration test: baseline evaluation — predict + score pipeline with known inputs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from genie_space_optimizer.optimization.evaluation import (
    all_thresholds_met,
    filter_benchmarks_by_scope,
    normalize_scores,
)
from genie_space_optimizer.optimization.state import write_iteration, write_stage


class TestBaselineFiltering:
    def test_full_scope_returns_all(self, sample_benchmarks):
        filtered = filter_benchmarks_by_scope(sample_benchmarks, "full")
        assert len(filtered) == len(sample_benchmarks)

    def test_p0_scope(self, sample_benchmarks):
        filtered = filter_benchmarks_by_scope(sample_benchmarks, "p0")
        for b in filtered:
            assert b["priority"] == "P0"

    def test_held_out_scope(self, sample_benchmarks):
        filtered = filter_benchmarks_by_scope(sample_benchmarks, "held_out")
        for b in filtered:
            assert b["split"] == "held_out"

    def test_slice_scope_with_patched_objects(self, sample_benchmarks):
        filtered = filter_benchmarks_by_scope(
            sample_benchmarks, "slice", patched_objects=["customers"]
        )
        for b in filtered:
            assert "customers" in [t.lower() for t in b.get("required_tables", [])]


class TestBaselineScoring:
    def test_normalize_and_threshold_check(self):
        raw_scores = {
            "syntax_validity": 0.99,
            "schema_accuracy": 0.96,
            "logical_accuracy": 0.92,
            "semantic_equivalence": 0.91,
            "completeness": 0.93,
            "response_quality": 0.50,
            "result_correctness": 0.88,
            "asset_routing": 0.97,
        }
        normalized = normalize_scores(raw_scores)
        assert all_thresholds_met(normalized) is True

    def test_below_threshold(self):
        raw_scores = {
            "syntax_validity": 0.70,
            "schema_accuracy": 0.50,
            "logical_accuracy": 0.40,
            "semantic_equivalence": 0.40,
            "completeness": 0.50,
            "result_correctness": 0.30,
            "asset_routing": 0.60,
        }
        normalized = normalize_scores(raw_scores)
        assert all_thresholds_met(normalized) is False


class TestBaselineWriteIteration:
    def test_write_baseline_iteration(self, mock_spark):
        eval_result = {
            "mlflow_run_id": "mlflow-run-001",
            "overall_accuracy": 75.0,
            "total_questions": 20,
            "correct_count": 15,
            "scores": {"result_correctness": 75.0, "syntax_validity": 98.0},
            "failures": ["q1", "q5", "q10", "q15", "q20"],
            "remaining_failures": ["q1", "q5", "q10", "q15", "q20"],
            "thresholds_met": False,
        }
        write_iteration(
            mock_spark,
            run_id="run-1",
            iteration=0,
            eval_result=eval_result,
            catalog="cat",
            schema="gold",
            eval_scope="full",
        )
        sql = mock_spark.sql.call_args[0][0]
        assert "genie_opt_iterations" in sql
        assert "run-1" in sql


class TestBaselineEndToEnd:
    def test_baseline_flow_stage_writes(self, mock_spark):
        """Simulate the baseline task: stage writes + iteration write."""
        write_stage(
            mock_spark,
            run_id="run-1",
            stage="BASELINE_EVAL_STARTED",
            status="STARTED",
            task_key="baseline_eval",
            catalog="cat",
            schema="gold",
        )

        eval_result = {
            "mlflow_run_id": "mlflow-001",
            "overall_accuracy": 82.5,
            "total_questions": 20,
            "correct_count": 16,
            "scores": {"result_correctness": 82.5},
            "failures": ["q3", "q7", "q12", "q18"],
            "thresholds_met": False,
        }

        write_iteration(
            mock_spark,
            run_id="run-1",
            iteration=0,
            eval_result=eval_result,
            catalog="cat",
            schema="gold",
        )

        write_stage(
            mock_spark,
            run_id="run-1",
            stage="BASELINE_EVAL_COMPLETE",
            status="COMPLETE",
            task_key="baseline_eval",
            catalog="cat",
            schema="gold",
        )

        assert mock_spark.sql.call_count >= 3
