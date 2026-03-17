"""API tests for /api/genie/spaces endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestListSpaces:
    @patch("genie_space_optimizer.backend.routes.spaces.get_spark")
    @patch("genie_space_optimizer.backend.routes.spaces.list_spaces")
    def test_list_spaces_returns_list(
        self, mock_list_spaces_fn, mock_get_spark, api_client
    ):
        mock_list_spaces_fn.return_value = [
            {"id": "space-1", "title": "Revenue"},
            {"id": "space-2", "title": "Cost"},
        ]
        mock_spark = MagicMock()
        mock_spark.sql.return_value.toPandas.return_value = pd.DataFrame()
        mock_get_spark.return_value = mock_spark

        with patch(
            "genie_space_optimizer.common.genie_client.list_spaces",
            return_value=[
                {"id": "space-1", "title": "Revenue"},
                {"id": "space-2", "title": "Cost"},
            ],
        ), patch(
            "genie_space_optimizer.optimization.state.load_recent_activity",
            return_value=pd.DataFrame(),
        ):
            response = api_client.get("/api/genie/spaces")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "spaces" in data
        assert len(data["spaces"]) == 2
        assert "totalCount" in data

    @patch("genie_space_optimizer.backend.routes.spaces.get_spark")
    def test_list_spaces_empty(self, mock_get_spark, api_client):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.toPandas.return_value = pd.DataFrame()
        mock_get_spark.return_value = mock_spark

        with patch(
            "genie_space_optimizer.common.genie_client.list_spaces",
            return_value=[],
        ), patch(
            "genie_space_optimizer.optimization.state.load_recent_activity",
            return_value=pd.DataFrame(),
        ):
            response = api_client.get("/api/genie/spaces")

        assert response.status_code == 200
        data = response.json()
        assert data["spaces"] == []
        assert data["totalCount"] == 0


class TestGetSpaceDetail:
    @patch("genie_space_optimizer.backend.routes.spaces.get_spark")
    def test_get_space_detail(self, mock_get_spark, api_client):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.toPandas.return_value = pd.DataFrame()
        mock_get_spark.return_value = mock_spark

        with patch(
            "genie_space_optimizer.common.genie_client.fetch_space_config",
            return_value={
                "title": "Revenue Space",
                "description": "Revenue analytics",
                "_parsed_space": {
                    "data_sources": {
                        "tables": [
                            {
                                "identifier": "catalog.schema.orders",
                                "description": ["Orders"],
                                "column_configs": [
                                    {"column_name": "order_id"},
                                ],
                            }
                        ]
                    },
                    "instructions": {
                        "text_instructions": [{"id": "i1", "content": ["Filter."]}],
                        "example_question_sqls": [{"question": "Total?"}],
                    },
                },
            },
        ), patch(
            "genie_space_optimizer.optimization.state.load_runs_for_space",
            return_value=pd.DataFrame(),
        ):
            response = api_client.get("/api/genie/spaces/space-123")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "space-123"
        assert "tables" in data
        assert "sampleQuestions" in data

    @patch("genie_space_optimizer.backend.routes.spaces.get_spark")
    def test_get_space_detail_not_found(self, mock_get_spark, api_client):
        mock_get_spark.return_value = MagicMock()

        with patch(
            "genie_space_optimizer.common.genie_client.fetch_space_config",
            side_effect=Exception("Not found"),
        ):
            response = api_client.get("/api/genie/spaces/nonexistent")

        assert response.status_code == 404


class TestStartOptimization:
    @patch("genie_space_optimizer.backend.routes.trigger.get_spark")
    def test_start_optimization(self, mock_get_spark, api_client, mock_workspace_client):
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        with patch(
            "genie_space_optimizer.backend.routes.spaces.do_start_optimization",
        ) as mock_do:
            from genie_space_optimizer.backend.models import OptimizeResponse
            mock_do.return_value = OptimizeResponse(
                runId="run-123", jobRunId="job-456", jobUrl=None,
            )
            response = api_client.post(
                "/api/genie/trigger",
                json={"space_id": "space-123"},
            )

        assert response.status_code == 200
        data = response.json()
        assert "runId" in data
        assert "jobRunId" in data
