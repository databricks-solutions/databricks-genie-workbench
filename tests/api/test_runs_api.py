"""API tests for /api/genie/runs endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestGetRun:
    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_get_run_returns_pipeline(self, mock_get_spark, api_client):
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-1",
                "space_id": "space-abc",
                "domain": "revenue",
                "status": "CONVERGED",
                "started_at": "2025-01-01T00:00:00Z",
                "triggered_by": "test@example.com",
                "best_accuracy": 90.0,
                "convergence_reason": "threshold_met",
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_stages",
            return_value=pd.DataFrame(
                [
                    {"stage": "PREFLIGHT_STARTED", "status": "COMPLETE", "started_at": "2025-01-01T00:00:00Z"},
                    {"stage": "BASELINE_EVAL", "status": "COMPLETE", "started_at": "2025-01-01T00:10:00Z"},
                ]
            ),
        ), patch(
            "genie_space_optimizer.optimization.state.load_iterations",
            return_value=pd.DataFrame(
                [
                    {"iteration": 0, "overall_accuracy": 75.0, "eval_scope": "full"},
                ]
            ),
        ):
            response = api_client.get("/api/genie/runs/run-1")

        assert response.status_code == 200
        data = response.json()
        assert data["runId"] == "run-1"
        assert "steps" in data
        assert isinstance(data["steps"], list)
        assert len(data["steps"]) == 6

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_get_run_not_found(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value=None,
        ):
            response = api_client.get("/api/genie/runs/nonexistent")

        assert response.status_code == 404


class TestStageToStepMapping:
    """Verify the stage-to-step mapping produces exactly 6 steps."""

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_six_steps_always(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-2",
                "space_id": "space-x",
                "domain": "test",
                "status": "IN_PROGRESS",
                "started_at": "2025-01-01T00:00:00Z",
                "triggered_by": "user@test.com",
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_stages",
            return_value=pd.DataFrame(),
        ), patch(
            "genie_space_optimizer.optimization.state.load_iterations",
            return_value=pd.DataFrame(),
        ):
            response = api_client.get("/api/genie/runs/run-2")

        assert response.status_code == 200
        data = response.json()
        assert len(data["steps"]) == 6
        step_names = [s["name"] for s in data["steps"]]
        assert "Preflight" in step_names
        assert "Baseline Evaluation" in step_names
        assert "Proactive Enrichment" in step_names
        assert "Adaptive Optimization" in step_names
        assert "Finalization" in step_names
        assert "Deploy" in step_names


class TestGetComparison:
    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_get_comparison(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-1",
                "space_id": "space-abc",
                "domain": "revenue",
                "config_snapshot": {
                    "instructions": {
                        "text_instructions": [{"id": "i1", "content": ["old"]}],
                        "example_question_sqls": [],
                    },
                    "data_sources": {"tables": []},
                },
            },
        ), patch(
            "genie_space_optimizer.common.genie_client.fetch_space_config",
            return_value={
                "_parsed_space": {
                    "instructions": {
                        "text_instructions": [{"id": "i1", "content": ["new"]}],
                        "example_question_sqls": [],
                    },
                    "data_sources": {"tables": []},
                }
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_iterations",
            return_value=pd.DataFrame(
                [
                    {
                        "iteration": 0,
                        "eval_scope": "full",
                        "overall_accuracy": 70.0,
                        "scores_json": '{"result_correctness": 70.0}',
                    },
                    {
                        "iteration": 3,
                        "eval_scope": "full",
                        "overall_accuracy": 88.0,
                        "scores_json": '{"result_correctness": 88.0}',
                    },
                ]
            ),
        ):
            response = api_client.get("/api/genie/runs/run-1/comparison")

        assert response.status_code == 200
        data = response.json()
        assert "baselineScore" in data
        assert "optimizedScore" in data
        assert "perDimensionScores" in data
        assert "original" in data
        assert "optimized" in data


class TestIterationDetailFlaggedQuestions:
    """Verify iteration-detail includes flag_reason and related fields."""

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_flagged_questions_with_flag_reason(self, mock_get_spark, api_client):
        import json as _json

        mock_get_spark.return_value = MagicMock()
        rows_json = _json.dumps([
            {
                "request_id": "q1",
                "inputs": {"question": "What is revenue?"},
                "result_correctness/value": "no",
            },
        ])

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-flag",
                "space_id": "sp-1",
                "domain": "test",
                "status": "CONVERGED",
                "started_at": "2025-01-01T00:00:00Z",
                "triggered_by": "t@t.com",
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_iterations",
            return_value=pd.DataFrame([
                {
                    "iteration": 0,
                    "eval_scope": "full",
                    "overall_accuracy": 50.0,
                    "total_questions": 1,
                    "correct_count": 0,
                    "rows_json": rows_json,
                },
            ]),
        ), patch(
            "genie_space_optimizer.optimization.state.load_stages",
            return_value=pd.DataFrame(),
        ), patch(
            "genie_space_optimizer.optimization.state.load_patches",
            return_value=pd.DataFrame(),
        ), patch(
            "genie_space_optimizer.optimization.labeling.get_flagged_questions",
            return_value=[
                {
                    "question_id": "q1",
                    "question_text": "What is revenue?",
                    "flag_reason": "PERSISTENT: failed 3/5 evals, 3 consecutive",
                    "iterations_failed": 3,
                    "patches_tried": "example_sql, instruction_edit",
                },
            ],
        ):
            response = api_client.get("/api/genie/runs/run-flag/iteration-detail")

        assert response.status_code == 200
        data = response.json()
        flagged = data.get("flaggedQuestions", [])
        assert len(flagged) >= 1
        fq = flagged[0]
        assert fq["question_id"] == "q1"
        assert "PERSISTENT" in fq.get("flag_reason", "")
        assert fq.get("iterations_failed") == 3

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_no_flagged_questions(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-noflag",
                "space_id": "sp-1",
                "domain": "test",
                "status": "CONVERGED",
                "started_at": "2025-01-01T00:00:00Z",
                "triggered_by": "t@t.com",
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_iterations",
            return_value=pd.DataFrame([
                {"iteration": 0, "eval_scope": "full", "overall_accuracy": 100.0, "total_questions": 1, "correct_count": 1},
            ]),
        ), patch(
            "genie_space_optimizer.optimization.state.load_stages",
            return_value=pd.DataFrame(),
        ), patch(
            "genie_space_optimizer.optimization.state.load_patches",
            return_value=pd.DataFrame(),
        ), patch(
            "genie_space_optimizer.optimization.labeling.get_flagged_questions",
            return_value=[],
        ):
            response = api_client.get("/api/genie/runs/run-noflag/iteration-detail")

        assert response.status_code == 200
        data = response.json()
        assert data.get("flaggedQuestions", []) == []


class TestIterationDetailNanHandling:
    """Regression: NaN values in pandas DataFrames must not crash the endpoint."""

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_nan_numeric_columns_do_not_crash(self, mock_get_spark, api_client):
        """Spark NULL → pandas NaN in iteration/total_questions/correct_count."""
        import json as _json
        import numpy as np

        mock_get_spark.return_value = MagicMock()

        rows_json = _json.dumps([
            {
                "request_id": "q1",
                "inputs": {"question": "What is revenue?"},
                "result_correctness/value": "yes",
            },
        ])

        iters_df = pd.DataFrame([
            {
                "iteration": 0,
                "eval_scope": "full",
                "overall_accuracy": 75.0,
                "total_questions": np.nan,
                "correct_count": np.nan,
                "rows_json": rows_json,
                "scores_json": '{"result_correctness": 75.0}',
            },
            {
                "iteration": 1,
                "eval_scope": "full",
                "overall_accuracy": 85.0,
                "total_questions": 10.0,
                "correct_count": np.nan,
                "rows_json": rows_json,
                "scores_json": '{"result_correctness": 85.0}',
            },
        ])

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-nan",
                "space_id": "sp-1",
                "domain": "test",
                "status": "CONVERGED",
                "started_at": "2025-01-01T00:00:00Z",
                "triggered_by": "t@t.com",
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_iterations",
            return_value=iters_df,
        ), patch(
            "genie_space_optimizer.optimization.state.load_stages",
            return_value=pd.DataFrame(),
        ), patch(
            "genie_space_optimizer.optimization.state.load_patches",
            return_value=pd.DataFrame(),
        ), patch(
            "genie_space_optimizer.optimization.labeling.get_flagged_questions",
            return_value=[],
        ):
            response = api_client.get("/api/genie/runs/run-nan/iteration-detail")

        assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
        data = response.json()
        assert len(data["iterations"]) == 2
        assert data["iterations"][0]["totalQuestions"] == 0
        assert data["iterations"][0]["correctCount"] == 0
        assert data["iterations"][1]["totalQuestions"] == 10
        assert data["iterations"][1]["correctCount"] == 0


class TestPendingReviewsFlagReason:
    """Verify pending-reviews passes through flag_reason patterns."""

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_persistence_reason_passed_through(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.labeling.get_flagged_questions",
            return_value=[
                {
                    "question_id": "q1",
                    "question_text": "Revenue last month?",
                    "flag_reason": "PERSISTENT: failed 3/5 evals, 3 consecutive",
                },
            ],
        ), patch(
            "genie_space_optimizer.optimization.state.get_queued_patches",
            return_value=[],
        ), patch(
            "genie_space_optimizer.optimization.state.run_query",
            return_value=pd.DataFrame(),
        ):
            response = api_client.get("/api/genie/pending-reviews/sp-1")

        assert response.status_code == 200
        data = response.json()
        assert data["flaggedQuestions"] == 1
        assert len(data["items"]) >= 1
        assert "PERSISTENT" in data["items"][0]["reason"]

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_tvf_removal_reason_passed_through(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.labeling.get_flagged_questions",
            return_value=[
                {
                    "question_id": "q2",
                    "question_text": "Amenity impact?",
                    "flag_reason": "Low-confidence TVF removal recommended: get_amenity_imp",
                },
            ],
        ), patch(
            "genie_space_optimizer.optimization.state.get_queued_patches",
            return_value=[],
        ), patch(
            "genie_space_optimizer.optimization.state.run_query",
            return_value=pd.DataFrame(),
        ):
            response = api_client.get("/api/genie/pending-reviews/sp-2")

        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) >= 1
        assert "TVF removal" in data["items"][0]["reason"]
