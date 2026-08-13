"""Transient, user-authorized reads for benchmark candidate-gap analysis."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from backend.watch.models import TrafficGapAnalysis
from backend.watch.services.traffic_gaps import (
    TrafficMessage,
    analyze_traffic_gaps,
)

_MAX_CONVERSATIONS = 1_000
_MAX_MESSAGES = 10_000
_MAX_PAGES = 500


class IncompleteTrafficRead(RuntimeError):
    """Raised when the complete manager-visible traffic set is unavailable."""


def _get_space(client: Any, space_id: str) -> dict[str, Any]:
    response = client.api_client.do(
        method="GET",
        path=f"/api/2.0/genie/spaces/{space_id}",
        query={"include_serialized_space": "true"},
    )
    if not isinstance(response, dict) or not response.get("serialized_space"):
        raise IncompleteTrafficRead("Genie did not return serialized_space")
    return response


def _paginate(
    client: Any,
    *,
    path: str,
    item_key: str,
    query: dict[str, Any] | None = None,
    max_items: int,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page_token: str | None = None
    seen_tokens: set[str] = set()
    page_count = 0
    while True:
        if page_token:
            if page_token in seen_tokens:
                raise IncompleteTrafficRead(f"{item_key}: repeated page token")
            seen_tokens.add(page_token)
        page_count += 1
        if page_count > _MAX_PAGES:
            raise IncompleteTrafficRead(f"{item_key}: page limit exceeded")

        params = {"page_size": 50, **(query or {})}
        if page_token:
            params["page_token"] = page_token
        response = client.api_client.do(method="GET", path=path, query=params)
        if not isinstance(response, dict):
            raise IncompleteTrafficRead(f"{item_key}: invalid page response")
        page_items = response.get(item_key)
        if page_items is None and not response.get("next_page_token"):
            page_items = []
        if not isinstance(page_items, list):
            raise IncompleteTrafficRead(f"{item_key}: missing page data")
        if any(not isinstance(item, dict) for item in page_items):
            raise IncompleteTrafficRead(f"{item_key}: invalid item data")
        items.extend(page_items)
        if len(items) > max_items:
            raise IncompleteTrafficRead(f"{item_key}: safety limit exceeded")
        page_token = response.get("next_page_token")
        if not page_token:
            return items


def _parse_serialized_space(response: dict[str, Any]) -> dict[str, Any]:
    raw = response.get("serialized_space")
    if isinstance(raw, dict):
        parsed = raw
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise IncompleteTrafficRead("serialized_space is not valid JSON") from exc
    else:
        raise IncompleteTrafficRead("serialized_space has an invalid shape")
    if not isinstance(parsed, dict):
        raise IncompleteTrafficRead("serialized_space has an invalid shape")
    return parsed


def _benchmark_questions(config: dict[str, Any]) -> list[str]:
    benchmarks = config.get("benchmarks") or {}
    if not isinstance(benchmarks, dict):
        raise IncompleteTrafficRead("benchmarks has an invalid shape")
    questions = benchmarks.get("questions") or []
    if not isinstance(questions, list):
        raise IncompleteTrafficRead("benchmark questions have an invalid shape")
    out: list[str] = []
    for entry in questions:
        if not isinstance(entry, dict):
            raise IncompleteTrafficRead("benchmark question has an invalid shape")
        value = entry.get("question")
        if isinstance(value, str) and value.strip():
            out.append(value)
        elif isinstance(value, list):
            if any(not isinstance(item, str) for item in value):
                raise IncompleteTrafficRead("benchmark question has an invalid shape")
            out.extend(item for item in value if item.strip())
    return out


def _timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _user_key(message: dict[str, Any], conversation: dict[str, Any]) -> str:
    for payload in (message.get("user"), conversation.get("user")):
        if not isinstance(payload, dict):
            continue
        value = payload.get("id") or payload.get("user_id") or payload.get("user_name")
        if value is not None:
            return str(value)
    value = message.get("user_id") or conversation.get("user_id")
    return str(value) if value is not None else ""


def _feedback_rating(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        rating = value.get("rating") or value.get("feedback_rating")
        return str(rating) if rating is not None else None
    return None


def _conversation_url(host: str, space_id: str, conversation_id: str) -> str:
    return (
        f"{host.rstrip('/')}/genie/rooms/{quote(space_id, safe='')}"
        f"/chats/{quote(conversation_id, safe='')}"
    )


def read_traffic_gap_analysis(*, client: Any, space_id: str) -> TrafficGapAnalysis:
    """Read the full manager-visible corpus and analyze it entirely in memory.

    Calling the conversation endpoint with ``include_all=true`` is both the
    manager-permission enforcement point and the source of complete space-wide
    traffic. Any page/shape/cap failure aborts the whole analysis.
    """
    space_path = f"/api/2.0/genie/spaces/{space_id}"
    config = _parse_serialized_space(_get_space(client, space_id))
    conversations_path = f"{space_path}/conversations"
    conversations = _paginate(
        client,
        path=conversations_path,
        item_key="conversations",
        query={"include_all": "true"},
        max_items=_MAX_CONVERSATIONS,
    )

    messages: list[TrafficMessage] = []
    for conversation in conversations:
        conversation_id = conversation.get("id") or conversation.get("conversation_id")
        if not conversation_id:
            raise IncompleteTrafficRead("conversation is missing its id")
        conversation_id = str(conversation_id)
        raw_messages = _paginate(
            client,
            path=f"{conversations_path}/{conversation_id}/messages",
            item_key="messages",
            max_items=_MAX_MESSAGES - len(messages),
        )
        for message in raw_messages:
            content = message.get("content")
            if not isinstance(content, str) or not content.strip():
                continue
            messages.append(
                TrafficMessage(
                    content=content,
                    conversation_id=conversation_id,
                    user_key=_user_key(message, conversation),
                    status=str(message.get("status") or ""),
                    feedback=_feedback_rating(message.get("feedback")),
                    created_at=_timestamp(
                        message.get("created_timestamp") or message.get("created_at")
                    ),
                )
            )
            if len(messages) > _MAX_MESSAGES:
                raise IncompleteTrafficRead("messages: safety limit exceeded")

    host = str(getattr(getattr(client, "config", None), "host", "") or "")
    if not host:
        raise IncompleteTrafficRead("workspace host is unavailable")
    return analyze_traffic_gaps(
        messages=messages,
        benchmark_questions=_benchmark_questions(config),
        conversation_url=lambda conversation_id: _conversation_url(
            host, space_id, conversation_id
        ),
    )
