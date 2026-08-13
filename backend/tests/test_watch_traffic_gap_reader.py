from __future__ import annotations

import json

import pytest

from backend.watch.services.traffic_gap_reader import (
    IncompleteTrafficRead,
    read_traffic_gap_analysis,
)


SPACE_ID = "a" * 32


class _ApiClient:
    def __init__(self, responses: dict[tuple[str, str | None], dict]):
        self.responses = responses
        self.calls: list[tuple[str, dict]] = []

    def do(self, *, method: str, path: str, query: dict):
        assert method == "GET"
        self.calls.append((path, query))
        token = query.get("page_token")
        return self.responses[(path, token)]


class _Client:
    def __init__(self, responses: dict[tuple[str, str | None], dict]):
        self.api_client = _ApiClient(responses)

        class Config:
            host = "https://workspace.example"

        self.config = Config()


def _responses() -> dict[tuple[str, str | None], dict]:
    space_path = f"/api/2.0/genie/spaces/{SPACE_ID}"
    conversations_path = f"{space_path}/conversations"
    messages_1_path = f"{conversations_path}/conv-1/messages"
    messages_2_path = f"{conversations_path}/conv-2/messages"
    return {
        (space_path, None): {
            "serialized_space": json.dumps(
                {"benchmarks": {"questions": [{"question": ["Covered question"]}]}}
            )
        },
        (conversations_path, None): {
            "conversations": [{"id": "conv-1", "user": {"id": "u1"}}],
            "next_page_token": "next",
        },
        (conversations_path, "next"): {
            "conversations": [{"id": "conv-2", "user": {"id": "u2"}}]
        },
        (messages_1_path, None): {
            "messages": [
                {
                    "content": "Missing question",
                    "status": "FAILED",
                    "created_timestamp": 1_754_003_200_000,
                }
            ]
        },
        (messages_2_path, None): {
            "messages": [
                {
                    "content": "Covered question",
                    "status": "FAILED",
                    "feedback": {"rating": "NEGATIVE"},
                    "created_timestamp": 1_754_003_300_000,
                }
            ]
        },
    }


def test_reads_all_pages_with_manager_scope_and_returns_no_raw_text() -> None:
    client = _Client(_responses())

    result = read_traffic_gap_analysis(client=client, space_id=SPACE_ID)

    assert result.scanned_message_count == 2
    assert result.covered_family_count == 1
    assert len(result.candidates) == 1
    assert result.candidates[0].signals == ["failed"]
    assert result.candidates[0].conversation_urls == [
        f"https://workspace.example/genie/rooms/{SPACE_ID}/chats/conv-1"
    ]
    serialized = result.model_dump_json()
    assert "Missing question" not in serialized
    assert "Covered question" not in serialized
    assert "u1" not in serialized
    assert "u2" not in serialized

    conversation_call = next(
        query for path, query in client.api_client.calls if path.endswith("/conversations")
    )
    assert conversation_call["include_all"] == "true"
    assert any(query.get("page_token") == "next" for _, query in client.api_client.calls)


def test_repeated_page_token_fails_closed_without_partial_results() -> None:
    responses = _responses()
    conversations_path = f"/api/2.0/genie/spaces/{SPACE_ID}/conversations"
    responses[(conversations_path, "next")]["next_page_token"] = "next"
    client = _Client(responses)

    with pytest.raises(IncompleteTrafficRead, match="repeated page token"):
        read_traffic_gap_analysis(client=client, space_id=SPACE_ID)


def test_missing_serialized_space_fails_closed() -> None:
    responses = _responses()
    space_path = f"/api/2.0/genie/spaces/{SPACE_ID}"
    responses[(space_path, None)] = {}

    with pytest.raises(IncompleteTrafficRead, match="serialized_space"):
        read_traffic_gap_analysis(client=_Client(responses), space_id=SPACE_ID)


def test_omitted_empty_collection_is_a_complete_empty_page() -> None:
    responses = _responses()
    conversations_path = f"/api/2.0/genie/spaces/{SPACE_ID}/conversations"
    responses[(conversations_path, None)] = {}

    result = read_traffic_gap_analysis(client=_Client(responses), space_id=SPACE_ID)

    assert result.scanned_message_count == 0
    assert result.candidates == []


def test_omitted_collection_with_next_token_fails_closed() -> None:
    responses = _responses()
    conversations_path = f"/api/2.0/genie/spaces/{SPACE_ID}/conversations"
    responses[(conversations_path, None)] = {"next_page_token": "next"}

    with pytest.raises(IncompleteTrafficRead, match="missing page data"):
        read_traffic_gap_analysis(client=_Client(responses), space_id=SPACE_ID)
