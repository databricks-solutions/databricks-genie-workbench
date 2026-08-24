from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.common.genie_client import (
    MissingSerializedSpaceError,
    fetch_space_config,
)


def _client_with_response(response: dict) -> MagicMock:
    client = MagicMock()
    client.api_client.do.return_value = response
    return client


@pytest.mark.parametrize(
    "response",
    [
        {"title": "No export"},
        {"title": "None export", "serialized_space": None},
        {"title": "Empty export", "serialized_space": {}},
        {"title": "String empty export", "serialized_space": "{}"},
    ],
)
def test_fetch_space_config_rejects_missing_or_empty_serialized_space(
    response: dict,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _client_with_response(response)

    with caplog.at_level(logging.ERROR), pytest.raises(MissingSerializedSpaceError):
        fetch_space_config(client, "space-1")

    assert client.api_client.do.call_args.kwargs["query"] == {
        "include_serialized_space": "true"
    }
    assert "serialized_space" in caplog.text


def test_fetch_space_config_allows_genuine_empty_space_with_export() -> None:
    client = _client_with_response(
        {
            "title": "Empty but exported",
            "serialized_space": {
                "version": 2,
                "data_sources": {"tables": [], "metric_views": [], "functions": []},
                "instructions": {"text_instructions": []},
            },
        }
    )

    config = fetch_space_config(client, "space-1")

    assert config["_parsed_space"]["version"] == 2
    assert config["_tables"] == []
    assert config["_metric_views"] == []
    assert config["_functions"] == []


def test_fetch_space_config_drops_id_only_text_instruction_placeholder(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = _client_with_response(
        {
            "title": "Agent with empty instruction placeholder",
            "serialized_space": {
                "version": 2,
                "data_sources": {
                    "tables": [{"identifier": "cat.sch.orders"}],
                    "metric_views": [],
                },
                "instructions": {
                    "text_instructions": [{"id": "a" * 32}],
                },
            },
        }
    )

    with caplog.at_level(logging.WARNING):
        config = fetch_space_config(client, "space-1")

    assert config["_parsed_space"]["instructions"]["text_instructions"] == []
    assert config["_instructions"] == []
    assert "empty text-instruction placeholder" in caplog.text
