"""Tests for Genie serialized_space fetch semantics."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.common.genie_client import (
    MissingSerializedSpaceError,
    fetch_space_config,
)


def _workspace_with_response(response: dict) -> MagicMock:
    w = MagicMock()
    w.api_client.do.return_value = response
    return w


@pytest.mark.parametrize(
    "response",
    [
        {"space_id": "space-1", "title": "No export"},
        {"space_id": "space-1", "serialized_space": None},
        {"space_id": "space-1", "serialized_space": ""},
        {"space_id": "space-1", "serialized_space": "   "},
        {"space_id": "space-1", "serialized_space": {}},
        {"space_id": "space-1", "serialized_space": "{}"},
    ],
)
def test_fetch_space_config_raises_on_missing_or_empty_serialized_space(
    response: dict,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(
        logging.ERROR,
        logger="genie_space_optimizer.common.genie_client",
    ):
        with pytest.raises(MissingSerializedSpaceError):
            fetch_space_config(_workspace_with_response(response), "space-1")

    assert any("serialized_space" in record.getMessage() for record in caplog.records)


def test_fetch_space_config_allows_present_but_genuinely_empty_data_sources() -> None:
    serialized = {
        "version": 2,
        "data_sources": {"tables": [], "metric_views": [], "functions": []},
        "config": {"sample_questions": []},
        "instructions": {"text_instructions": []},
    }
    config = fetch_space_config(
        _workspace_with_response(
            {
                "space_id": "space-1",
                "title": "Empty but exported",
                "serialized_space": json.dumps(serialized),
            }
        ),
        "space-1",
    )

    assert config["_parsed_space"] == serialized
    assert config["_tables"] == []
    assert config["_metric_views"] == []
    assert config["_functions"] == []
