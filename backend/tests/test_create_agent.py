"""Tests for CreateAgent idempotency guards (backend/services/create_agent.py)."""

import asyncio
import json
from types import SimpleNamespace

from backend.services.create_agent import CreateGenieAgent


def _make_session(space_id=None, space_url=None, history=None, space_config=None):
    """Build a minimal mock session with the fields CreateAgent checks."""
    return SimpleNamespace(
        space_id=space_id,
        space_url=space_url,
        space_config=space_config if space_config is not None else {"data_sources": {"tables": []}},
        llm_model=None,
        history=history or [],
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
            agent._repair_config = lambda config, err, model=None: {"data_sources": {"tables": [{"identifier": "c.s.t"}]}}
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
            agent._repair_config = lambda config, err, model=None: {"data_sources": {"tables": [{"identifier": "c.s.t"}]}}
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


class TestCreateSpaceDescription:
    """_create_space_with_repair threads the description into create_space args."""

    def test_repair_retry_uses_repaired_config_and_preserves_description(self, monkeypatch):
        original = {"data_sources": {"tables": [{"identifier": "c.s.original"}]}}
        repaired = {"data_sources": {"tables": [{"identifier": "c.s.repaired"}]}}
        calls = []

        def fake_create_space(display_name, description="", config=None):
            calls.append((display_name, description, config))
            if config != repaired:
                return {"success": False, "error": "Invalid configuration"}
            return {"success": True, "space_id": "s1", "space_url": "https://example.com/s1"}

        # Keep the real dispatcher: it injects config into the arguments dict.
        monkeypatch.setattr("backend.services.create_agent_tools._create_space", fake_create_space)
        agent = CreateGenieAgent.__new__(CreateGenieAgent)
        agent._repair_config = lambda config, err, model=None: repaired
        session = _make_session(space_config=original)

        async def run():
            return [event async for event in agent._create_space_with_repair(
                session, original, "Test Space", "Answers revenue questions."
            )]

        events = asyncio.run(run())

        assert calls == [
            ("Test Space", "Answers revenue questions.", original),
            ("Test Space", "Answers revenue questions.", repaired),
        ]
        assert session.space_id == "s1"
        assert session.space_config == repaired
        assert any(event["event"] == "created" for event in events)
        tool_call = next(event for event in events if event["event"] == "tool_call")
        assert tool_call["data"]["args"] == {
            "display_name": "Test Space", "description": "Answers revenue questions."
        }

    def test_description_passed_to_create_space(self, monkeypatch):
        async def run():
            agent = CreateGenieAgent.__new__(CreateGenieAgent)
            session = _make_session(space_id=None)

            events = []
            async for event in agent._create_space_with_repair(
                session, {}, "Test Space", "Answers revenue questions."
            ):
                events.append(event)
            return events

        calls = []

        def fake_handle_tool_call(name, arguments, session_config=None):
            calls.append(arguments)
            return {"success": True, "space_id": "s1", "space_url": "https://example.com/s1", "display_name": "Test Space"}

        monkeypatch.setattr("backend.services.create_agent_tools.handle_tool_call", fake_handle_tool_call)

        events = asyncio.run(run())

        assert calls[0]["description"] == "Answers revenue questions."
        tool_call = next(e for e in events if e["event"] == "tool_call")
        assert tool_call["data"]["args"]["description"] == "Answers revenue questions."

    def test_description_omitted_when_empty(self, monkeypatch):
        async def run():
            agent = CreateGenieAgent.__new__(CreateGenieAgent)
            session = _make_session(space_id=None)

            events = []
            async for event in agent._create_space_with_repair(session, {}, "Test Space"):
                events.append(event)
            return events

        calls = []

        def fake_handle_tool_call(name, arguments, session_config=None):
            calls.append(arguments)
            return {"success": True, "space_id": "s1", "space_url": "https://example.com/s1", "display_name": "Test Space"}

        monkeypatch.setattr("backend.services.create_agent_tools.handle_tool_call", fake_handle_tool_call)

        asyncio.run(run())

        assert "description" not in calls[0]


class TestExistingSpaceDescriptionUpdate:
    """Existing-space fast approvals propagate description metadata."""

    def test_fast_create_updates_description(self, monkeypatch):
        generated_config = {"data_sources": {"tables": [{"identifier": "c.s.t"}]}}
        calls = []

        def fake_handle_tool_call(name, arguments, session_config=None):
            calls.append((name, arguments.copy(), session_config))
            if name == "generate_config":
                return {"config": generated_config}
            if name == "update_space":
                return {"success": True, "space_id": "s1", "url": "https://example.com/s1"}
            raise AssertionError(f"Unexpected tool call: {name}")

        monkeypatch.setattr("backend.services.create_agent_tools.handle_tool_call", fake_handle_tool_call)
        agent = CreateGenieAgent.__new__(CreateGenieAgent)
        session = _make_session(space_id="s1", space_url="https://example.com/s1")
        selections = {
            "edited_plan": {"tables": [{"identifier": "c.s.t"}]},
            "display_name": "Revenue Agent",
            "description": "Answers revenue questions.",
        }

        async def run():
            return [event async for event in agent._fast_create(session, selections)]

        events = asyncio.run(run())

        update_call = next(call for call in calls if call[0] == "update_space")
        assert update_call[1] == {
            "space_id": "s1",
            "display_name": "Revenue Agent",
            "description": "Answers revenue questions.",
        }
        assert update_call[2] == generated_config
        assert any(event["event"] == "updated" for event in events)

    def test_fast_create_update_omits_derived_description(self, monkeypatch):
        """Updating an existing space must NOT write a description the user didn't ask to
        change — a plan suggestion / PURPOSE fallback would clobber existing wording."""
        generated_config = {
            "data_sources": {"tables": [{"identifier": "c.s.t"}]},
            "instructions": {"text_instructions": [{"content": ["## PURPOSE\n- Derived purpose.\n"]}]},
        }
        calls = []

        def fake_handle_tool_call(name, arguments, session_config=None):
            calls.append((name, arguments.copy(), session_config))
            if name == "generate_config":
                return {"config": generated_config}
            if name == "update_space":
                return {"success": True, "space_id": "s1", "url": "https://example.com/s1"}
            raise AssertionError(f"Unexpected tool call: {name}")

        monkeypatch.setattr("backend.services.create_agent_tools.handle_tool_call", fake_handle_tool_call)
        agent = CreateGenieAgent.__new__(CreateGenieAgent)
        session = _make_session(
            space_id="s1",
            space_url="https://example.com/s1",
            history=[{"role": "tool", "content": json.dumps({"suggested_description": "Plan suggestion"})}],
        )
        # No description in selections — the user only tweaked the plan.
        selections = {"edited_plan": {"tables": [{"identifier": "c.s.t"}]}, "display_name": "Revenue Agent"}

        async def run():
            return [event async for event in agent._fast_create(session, selections)]

        asyncio.run(run())

        update_call = next(call for call in calls if call[0] == "update_space")
        assert "description" not in update_call[1]


class TestDeriveDescription:
    """_derive_description fallback ordering: selections > tool_args > history > PURPOSE > ""."""

    def test_selections_win(self):
        session = _make_session()
        desc = CreateGenieAgent._derive_description(
            {"description": "from selections"}, {"description": "from args"}, session
        )
        assert desc == "from selections"

    def test_tool_args_second(self):
        session = _make_session()
        desc = CreateGenieAgent._derive_description(None, {"description": "from args"}, session)
        assert desc == "from args"

    def test_history_selections(self):
        session = _make_session(history=[
            {"role": "user", "content": 'go ahead [User selections: {"description": "from history"}]'},
        ])
        desc = CreateGenieAgent._derive_description(None, None, session)
        assert desc == "from history"

    def test_history_plan_suggestion(self):
        session = _make_session(history=[
            {"role": "tool", "content": json.dumps({"suggested_description": "from plan"})},
        ])
        desc = CreateGenieAgent._derive_description(None, None, session)
        assert desc == "from plan"

    def test_chat_approval_preserves_structured_edit_over_regenerated_plan(self):
        session = _make_session(history=[
            {"role": "tool", "content": json.dumps({"suggested_description": "Original suggestion"})},
            {"role": "user", "content": 'The agent description should be: Edited description [User selections: {"description": "Edited description"}]'},
            {"role": "tool", "content": json.dumps({"suggested_description": "New suggestion"})},
            {"role": "user", "content": "go ahead and create it"},
        ])
        assert CreateGenieAgent._derive_description(None, None, session) == "Edited description"

    def test_purpose_fallback_from_config(self):
        config = {
            "instructions": {
                "text_instructions": [{
                    "id": "abc",
                    "content": [
                        "## PURPOSE\n- Answer revenue questions for the US retail team.\n- Audience: merchandising managers.\n",
                        "## CONSTRAINTS\n- Never show PII columns.\n",
                    ],
                }]
            }
        }
        session = _make_session()
        desc = CreateGenieAgent._derive_description(None, None, session, config)
        assert desc == "Answer revenue questions for the US retail team. Audience: merchandising managers."

    def test_purpose_fallback_from_joined_string(self):
        """The plan editor sends text_instructions as one joined string — sections must still split."""
        config = {
            "instructions": {
                "text_instructions": [{
                    "id": "abc",
                    "content": ["## PURPOSE\n- Answers sales questions.\n\n## CONSTRAINTS\n- No PII.\n"],
                }]
            }
        }
        session = _make_session()
        desc = CreateGenieAgent._derive_description(None, None, session, config)
        assert desc == "Answers sales questions."

    def test_purpose_fallback_uses_session_config(self):
        config = {
            "instructions": {
                "text_instructions": [{
                    "id": "abc",
                    "content": ["## Purpose\n- Session config purpose.\n"],
                }]
            }
        }
        session = _make_session(space_config=config)
        desc = CreateGenieAgent._derive_description(None, None, session, None)
        assert desc == "Session config purpose."

    def test_empty_when_nothing_available(self):
        session = _make_session(space_config={"data_sources": {}})
        assert CreateGenieAgent._derive_description(None, None, session) == ""

    def test_non_str_explicit_is_coerced_not_crashed(self):
        """LLM tool args can carry a non-str description — slice must not raise."""
        session = _make_session()
        # int would raise TypeError on subscript without str() coercion
        assert CreateGenieAgent._derive_description(None, {"description": 12345}, session) == "12345"
        # dict mis-slices without coercion; coerced form is a string, no crash
        desc = CreateGenieAgent._derive_description(None, {"description": {"a": 1}}, session)
        assert isinstance(desc, str) and desc


class TestExplicitDescription:
    """_explicit_description (update path) returns explicit intent only — never a derived fallback."""

    def test_selections_win(self):
        session = _make_session()
        assert CreateGenieAgent._explicit_description(
            {"description": "from selections"}, {"description": "from args"}, session
        ) == "from selections"

    def test_tool_args_second(self):
        session = _make_session()
        assert CreateGenieAgent._explicit_description(None, {"description": "from args"}, session) == "from args"

    def test_history_user_selection(self):
        session = _make_session(history=[
            {"role": "user", "content": 'go ahead [User selections: {"description": "from history"}]'},
        ])
        assert CreateGenieAgent._explicit_description(None, None, session) == "from history"

    def test_ignores_plan_suggestion_fallback(self):
        """A plan-time suggestion is a derived default — the update path must not send it."""
        session = _make_session(history=[
            {"role": "tool", "content": json.dumps({"suggested_description": "from plan"})},
        ])
        assert CreateGenieAgent._explicit_description(None, None, session) == ""

    def test_ignores_purpose_fallback(self):
        """The ## PURPOSE distillation is a derived default — the update path must not send it."""
        config = {"instructions": {"text_instructions": [{"content": ["## PURPOSE\n- Answers sales.\n"]}]}}
        session = _make_session(space_config=config)
        assert CreateGenieAgent._explicit_description(None, None, session) == ""

    def test_clamps_to_max(self):
        from backend.services.create_agent import MAX_DESCRIPTION_CHARS
        session = _make_session()
        long = "x" * (MAX_DESCRIPTION_CHARS + 50)
        assert len(CreateGenieAgent._explicit_description({"description": long}, None, session)) == MAX_DESCRIPTION_CHARS

    def test_non_str_explicit_is_coerced_not_crashed(self):
        """LLM tool args can carry a non-str description — slice must not raise."""
        session = _make_session()
        assert CreateGenieAgent._explicit_description(None, {"description": 12345}, session) == "12345"


class TestPurposeFromConfig:
    """_purpose_from_config must not crash on malformed config shapes."""

    def test_non_dict_instructions_returns_empty(self):
        # instructions is a string (not a dict) — must not raise AttributeError.
        assert CreateGenieAgent._purpose_from_config({"instructions": "just a string"}) == ""

    def test_list_instructions_returns_empty(self):
        assert CreateGenieAgent._purpose_from_config({"instructions": ["a", "b"]}) == ""

    def test_string_content_not_char_iterated(self):
        # content as a string must be treated as one chunk, not iterated char-by-char.
        config = {"instructions": {"text_instructions": [{"content": "## PURPOSE\n- Answers sales questions.\n"}]}}
        assert CreateGenieAgent._purpose_from_config(config) == "Answers sales questions."

    def test_none_config_returns_empty(self):
        assert CreateGenieAgent._purpose_from_config(None) == ""
