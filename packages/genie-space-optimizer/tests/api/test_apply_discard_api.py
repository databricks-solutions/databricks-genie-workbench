"""API tests for /api/genie/runs/{run_id}/apply and /discard endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestApplyOptimization:
    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_apply_converged_run(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-1",
                "space_id": "space-abc",
                "status": "CONVERGED",
            },
        ), patch(
            "genie_space_optimizer.optimization.state.update_run_status",
        ) as mock_update:
            response = api_client.post("/api/genie/runs/run-1/apply")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "applied"
        assert data["runId"] == "run-1"
        mock_update.assert_called_once()

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_apply_non_terminal_run_rejected(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-1",
                "status": "IN_PROGRESS",
            },
        ):
            response = api_client.post("/api/genie/runs/run-1/apply")

        assert response.status_code == 400

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_apply_run_not_found(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value=None,
        ):
            response = api_client.post("/api/genie/runs/nonexistent/apply")

        assert response.status_code == 404

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_apply_stalled_run(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={"run_id": "run-1", "status": "STALLED"},
        ), patch(
            "genie_space_optimizer.optimization.state.update_run_status",
        ):
            response = api_client.post("/api/genie/runs/run-1/apply")

        assert response.status_code == 200


class TestDiscardOptimization:
    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_discard_converged_run(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-1",
                "space_id": "space-abc",
                "status": "CONVERGED",
                "config_snapshot": '{"instructions": {}}',
            },
        ), patch(
            "genie_space_optimizer.optimization.state.update_run_status",
        ), patch(
            "genie_space_optimizer.optimization.applier.rollback",
        ):
            response = api_client.post("/api/genie/runs/run-1/discard")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "discarded"

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_discard_already_applied_rejected(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-1",
                "status": "APPLIED",
            },
        ):
            response = api_client.post("/api/genie/runs/run-1/discard")

        assert response.status_code == 409

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_discard_already_discarded_rejected(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value={
                "run_id": "run-1",
                "status": "DISCARDED",
            },
        ):
            response = api_client.post("/api/genie/runs/run-1/discard")

        assert response.status_code == 409

    @patch("genie_space_optimizer.backend.routes.runs.get_spark")
    def test_discard_run_not_found(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_run",
            return_value=None,
        ):
            response = api_client.post("/api/genie/runs/nonexistent/discard")

        assert response.status_code == 404
