"""Tests for CreateAgent idempotency guards (backend/services/create_agent.py)."""

import asyncio
from types import SimpleNamespace

import pytest

from backend.services.create_agent import CreateGenieAgent


def _make_session(space_id=None, space_url=None):
    """Build a minimal mock session with the fields CreateAgent checks."""
    return SimpleNamespace(
        space_id=space_id,
        space_url=space_url,
        space_config={"data_sources": {"tables": []}},
    )


class TestCreateSpaceIdempotency:
    """_create_space_with_repair must not call the API if space already exists (#67)."""

    @pytest.mark.asyncio
    async def test_returns_early_when_space_exists(self):
        agent = CreateGenieAgent.__new__(CreateGenieAgent)  # skip __init__
        session = _make_session(space_id="abc123", space_url="https://example.com/space/abc123")

        events = []
        async for event in agent._create_space_with_repair(session, {}, "Test Space"):
            events.append(event)

        # Should yield tool_result + created, NOT actually call the API
        assert any(e["event"] == "tool_result" for e in events)
        result_data = next(e for e in events if e["event"] == "tool_result")["data"]["result"]
        assert result_data["success"] is True
        assert result_data["space_id"] == "abc123"
        assert result_data["already_existed"] is True

        assert any(e["event"] == "created" for e in events)
        created_data = next(e for e in events if e["event"] == "created")["data"]
        assert created_data["space_id"] == "abc123"

    @pytest.mark.asyncio
    async def test_no_early_return_when_no_space(self):
        """When space_id is not set, the guard should NOT fire (normal flow proceeds)."""
        agent = CreateGenieAgent.__new__(CreateGenieAgent)
        session = _make_session(space_id=None)

        # We can't run the full flow without mocking the API, but we can verify
        # the guard doesn't yield early-return events by checking the first event
        events = []
        try:
            async for event in agent._create_space_with_repair(session, {}, "Test Space"):
                events.append(event)
                break  # stop after first event to avoid API call
        except Exception:
            pass  # expected — we didn't mock handle_tool_call

        # First event should be tool_call (not tool_result with already_existed)
        assert events, "Expected at least one event before API call"
        assert events[0]["event"] == "tool_call"
