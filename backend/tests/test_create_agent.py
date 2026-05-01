"""Tests for CreateAgent idempotency guards (backend/services/create_agent.py)."""

import asyncio
from types import SimpleNamespace

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

    def test_returns_early_when_space_exists(self):
        async def run():
            agent = CreateGenieAgent.__new__(CreateGenieAgent)  # skip __init__
            session = _make_session(space_id="abc123", space_url="https://example.com/space/abc123")

            events = []
            async for event in agent._create_space_with_repair(session, {}, "Test Space"):
                events.append(event)
            return events

        events = asyncio.run(run())

        # Should yield tool_result + created, NOT actually call the API
        assert any(e["event"] == "tool_result" for e in events)
        result_data = next(e for e in events if e["event"] == "tool_result")["data"]["result"]
        assert result_data["success"] is True
        assert result_data["space_id"] == "abc123"
        assert result_data["already_existed"] is True

        assert any(e["event"] == "created" for e in events)
        created_data = next(e for e in events if e["event"] == "created")["data"]
        assert created_data["space_id"] == "abc123"

    def test_no_early_return_when_no_space(self):
        """When space_id is not set, the guard should NOT fire (normal flow proceeds)."""
        async def run():
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
            return events

        events = asyncio.run(run())

        # First event should be tool_call (not tool_result with already_existed)
        assert events, "Expected at least one event before API call"
        assert events[0]["event"] == "tool_call"

    def test_repair_status_is_not_emitted_as_final_tool_result(self, monkeypatch):
        async def run():
            agent = CreateGenieAgent.__new__(CreateGenieAgent)
            agent._repair_config = lambda config, err: {"data_sources": {"tables": [{"identifier": "c.s.t"}]}}
            session = _make_session(space_id=None)

            events = []
            async for event in agent._create_space_with_repair(session, {"bad": "config"}, "Test Space"):
                events.append(event)
            return events

        calls = []

        def fake_handle_tool_call(name, arguments, session_config=None):
            calls.append((name, session_config))
            if len(calls) == 1:
                return {"success": False, "error": "Invalid export proto: Duplicate column config"}
            return {"success": True, "space_id": "space123", "space_url": "https://example.com/space123", "display_name": "Test Space"}

        monkeypatch.setattr("backend.services.create_agent_tools.handle_tool_call", fake_handle_tool_call)

        events = asyncio.run(run())

        tool_results = [e["data"]["result"] for e in events if e["event"] == "tool_result"]
        assert len(tool_results) == 1
        assert not any(result.get("repairing") for result in tool_results)
        assert tool_results[0]["success"] is True
        assert any(e["event"] == "created" for e in events)
        assert len(calls) == 2

    def test_repair_retry_failure_emits_error_event(self, monkeypatch):
        async def run():
            agent = CreateGenieAgent.__new__(CreateGenieAgent)
            agent._repair_config = lambda config, err: {"data_sources": {"tables": [{"identifier": "c.s.t"}]}}
            session = _make_session(space_id=None)

            events = []
            async for event in agent._create_space_with_repair(session, {"bad": "config"}, "Test Space"):
                events.append(event)
            return events

        def fake_handle_tool_call(name, arguments, session_config=None):
            return {"success": False, "error": "Invalid export proto: still duplicated"}

        monkeypatch.setattr("backend.services.create_agent_tools.handle_tool_call", fake_handle_tool_call)

        events = asyncio.run(run())

        final_result = next(e["data"]["result"] for e in events if e["event"] == "tool_result")
        assert final_result["success"] is False
        assert final_result["error"] == "Invalid export proto: still duplicated"
        assert any(
            e["event"] == "error" and e["data"]["message"] == "Invalid export proto: still duplicated"
            for e in events
        )
