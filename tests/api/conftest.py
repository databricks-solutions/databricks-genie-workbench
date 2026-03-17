"""Shared API test fixtures with dependency overrides for FastAPI TestClient."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from genie_space_optimizer.backend.core._config import AppConfig


@pytest.fixture
def mock_config():
    """AppConfig with test values."""
    return AppConfig(
        app_name="genie-space-optimizer",
        catalog="test_catalog",
        schema="test_gold",
        warehouse_id="wh-test-123",
    )


@pytest.fixture
def mock_workspace_client():
    """Mock WorkspaceClient for API tests."""
    ws = MagicMock()
    ws.api_client.do.return_value = {
        "title": "Test Space",
        "description": "A test space",
        "serialized_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "test_catalog.test_gold.orders",
                        "description": ["Orders table"],
                        "column_configs": [
                            {"column_name": "order_id", "description": ["PK"]},
                        ],
                    }
                ]
            },
            "instructions": {
                "text_instructions": [{"id": "i1", "content": ["Filter active."]}],
                "example_question_sqls": [{"question": "Total revenue?"}],
            },
        },
        "create_time": "2025-01-01T00:00:00Z",
        "update_time": "2025-06-01T00:00:00Z",
    }
    ws.jobs.list.return_value = [MagicMock(job_id=12345)]
    run_mock = MagicMock()
    run_mock.run_id = 67890
    ws.jobs.run_now.return_value = run_mock
    return ws


@pytest.fixture
def api_client(mock_config, mock_workspace_client):
    """TestClient with dependency overrides for WorkspaceClient and AppConfig."""
    from genie_space_optimizer.backend.app import app
    from genie_space_optimizer.backend.core._defaults import (
        _ConfigDependency,
        _WorkspaceClientDependency,
        _get_user_ws,
    )

    app.dependency_overrides[_ConfigDependency.__call__] = lambda: mock_config
    app.dependency_overrides[_WorkspaceClientDependency.__call__] = lambda: mock_workspace_client
    app.dependency_overrides[_get_user_ws] = lambda: mock_workspace_client

    client = TestClient(app, raise_server_exceptions=False)
    yield client

    app.dependency_overrides.clear()
