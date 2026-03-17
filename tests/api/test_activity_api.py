"""API tests for /api/genie/activity endpoint."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestGetActivity:
    @patch("genie_space_optimizer.backend.routes.activity.get_spark")
    def test_activity_returns_list(self, mock_get_spark, api_client):
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        with patch(
            "genie_space_optimizer.optimization.state.load_recent_activity",
            return_value=pd.DataFrame(
                [
                    {
                        "run_id": "run-1",
                        "space_id": "space-abc",
                        "domain": "revenue",
                        "status": "CONVERGED",
                        "triggered_by": "test@example.com",
                        "best_accuracy": 90.0,
                        "started_at": "2025-01-01T00:00:00Z",
                    },
                    {
                        "run_id": "run-2",
                        "space_id": "space-def",
                        "domain": "cost",
                        "status": "IN_PROGRESS",
                        "triggered_by": "test@example.com",
                        "best_accuracy": None,
                        "started_at": "2025-01-02T00:00:00Z",
                    },
                ]
            ),
        ), patch(
            "genie_space_optimizer.backend.routes.activity._check_access_for_spaces",
            return_value={"space-abc": "CAN_EDIT", "space-def": "CAN_VIEW"},
        ), patch(
            "genie_space_optimizer.backend.routes.activity._load_baseline_scores",
            return_value={},
        ):
            response = api_client.get("/api/genie/activity")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["runId"] == "run-1"
        assert data[0]["status"] == "CONVERGED"
        assert data[1]["baselineScore"] is None

    @patch("genie_space_optimizer.backend.routes.activity.get_spark")
    def test_activity_empty(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_recent_activity",
            return_value=pd.DataFrame(),
        ):
            response = api_client.get("/api/genie/activity")

        assert response.status_code == 200
        assert response.json() == []

    @patch("genie_space_optimizer.backend.routes.activity.get_spark")
    def test_activity_with_space_id_filter(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_recent_activity",
            return_value=pd.DataFrame(
                [
                    {
                        "run_id": "run-1",
                        "space_id": "space-abc",
                        "domain": "revenue",
                        "status": "CONVERGED",
                        "triggered_by": "test@example.com",
                        "best_accuracy": 90.0,
                        "started_at": "2025-01-01T00:00:00Z",
                    }
                ]
            ),
        ) as mock_activity:
            response = api_client.get("/api/genie/activity?space_id=space-abc")

        assert response.status_code == 200
        mock_activity.assert_called_once()
        call_kwargs = mock_activity.call_args
        assert call_kwargs[1].get("space_id") == "space-abc" or "space-abc" in str(call_kwargs)

    @patch("genie_space_optimizer.backend.routes.activity.get_spark")
    def test_activity_with_limit(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.optimization.state.load_recent_activity",
            return_value=pd.DataFrame(),
        ) as mock_activity:
            response = api_client.get("/api/genie/activity?limit=5")

        assert response.status_code == 200
