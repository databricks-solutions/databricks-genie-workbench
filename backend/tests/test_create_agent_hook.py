"""Offline tests for the AGENTIC Create flow's initial-VC-capture hook.

These verify that a freshly created Genie space fires the fail-soft
``vc_capture`` callback at BOTH creation code paths in ``create_agent.py``
(the ``_create_space_with_repair`` success branch used by the fast/auto-chain
paths, and the main tool-result loop), that the idempotent-guard branch does
NOT capture (the space already existed), and that omitting ``vc_capture``
(default ``None``) never breaks the stream.

No network: ``handle_tool_call`` is patched to return a success dict. The
capture callback runs on a thread via ``run_in_executor`` + ``run_in_context``,
but ``_maybe_capture`` awaits that executor before the generator advances, so
asserting after draining is deterministic.
"""

import asyncio
from unittest.mock import Mock, patch

from backend.services import create_agent as create_agent_module
from backend.services.create_agent import CreateGenieAgent
from backend.services.create_agent_session import AgentSession

# Success payload the create tool returns (no network).
SUCCESS = {
    "success": True,
    "space_id": "sid-1",
    "space_url": "http://x",
    "display_name": "D",
}


def _drain(agen) -> list[dict]:
    """Fully consume an async generator and return its yielded events."""
    async def run():
        return [event async for event in agen]

    return asyncio.run(run())


# ── Test 1: fast path (_create_space_with_repair success branch) captures ──────

def test_create_space_with_repair_fires_capture():
    agent = CreateGenieAgent()
    session = AgentSession(session_id="t1")  # no space_id → real create
    capture = Mock()

    with patch(
        "backend.services.create_agent_tools.handle_tool_call",
        return_value=dict(SUCCESS),
    ):
        events = _drain(
            agent._create_space_with_repair(session, {}, "D", vc_capture=capture)
        )

    capture.assert_called_once_with("sid-1")
    assert session.space_id == "sid-1"
    created = [e for e in events if e["event"] == "created"]
    assert len(created) == 1
    assert created[0]["data"]["space_id"] == "sid-1"


# ── Test 2: idempotent-guard branch does NOT capture ───────────────────────────

def test_idempotent_guard_does_not_capture():
    agent = CreateGenieAgent()
    session = AgentSession(session_id="t2")
    session.space_id = "existing"  # already created → guard branch
    capture = Mock()

    with patch("backend.services.create_agent_tools.handle_tool_call") as htc:
        events = _drain(
            agent._create_space_with_repair(session, {}, "D", vc_capture=capture)
        )
        # Guard short-circuits before any create call.
        htc.assert_not_called()

    capture.assert_not_called()
    # The guard still emits a "created" event (for direct callers), but no capture.
    assert any(e["event"] == "created" for e in events)


# ── Test 3: main tool-result loop captures + _maybe_capture edge cases ──────────

def test_main_loop_fires_capture():
    agent = CreateGenieAgent()
    agent._build_messages = lambda session: []

    async def fake_stream(messages, tools=None, model=None, space_id=None):
        yield {
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call-1",
                        "function": {"name": "create_space", "arguments": "{}"},
                    }]
                }
            }]
        }

    agent._async_stream_llm = fake_stream
    session = AgentSession(session_id="main-loop")  # no space_id → real create
    capture = Mock()

    # The main loop uses the module-level ``handle_tool_call`` binding.
    with patch.object(create_agent_module, "handle_tool_call", return_value=dict(SUCCESS)):
        async def run():
            return [
                event
                async for event in agent.chat(session, "create it", vc_capture=capture)
            ]

        events = asyncio.run(run())

    capture.assert_called_once_with("sid-1")
    assert session.space_id == "sid-1"
    assert any(e["event"] == "created" for e in events)


def test_maybe_capture_fires_with_space_id():
    agent = CreateGenieAgent()
    session = AgentSession(session_id="e-fire")
    session.space_id = "sid-9"
    capture = Mock()

    asyncio.run(agent._maybe_capture(session, capture))

    capture.assert_called_once_with("sid-9")


def test_maybe_capture_noop_without_callback():
    agent = CreateGenieAgent()
    session = AgentSession(session_id="e-nocb")
    session.space_id = "sid-x"

    # Must not raise even though space_id is set — vc_capture is None.
    asyncio.run(agent._maybe_capture(session, None))


def test_maybe_capture_noop_without_space_id():
    agent = CreateGenieAgent()
    session = AgentSession(session_id="e-nosid")  # space_id defaults to None
    capture = Mock()

    asyncio.run(agent._maybe_capture(session, capture))

    capture.assert_not_called()


def test_maybe_capture_swallows_callback_exception():
    agent = CreateGenieAgent()
    session = AgentSession(session_id="e-boom")
    session.space_id = "sid-boom"
    capture = Mock(side_effect=RuntimeError("boom"))

    # _maybe_capture must never break the stream, even if the callback throws.
    asyncio.run(agent._maybe_capture(session, capture))

    capture.assert_called_once_with("sid-boom")


# ── Test 4: default (no vc_capture) is safe ────────────────────────────────────

def test_default_none_is_safe():
    agent = CreateGenieAgent()
    session = AgentSession(session_id="t4")

    with patch(
        "backend.services.create_agent_tools.handle_tool_call",
        return_value=dict(SUCCESS),
    ):
        events = _drain(agent._create_space_with_repair(session, {}, "D"))

    assert session.space_id == "sid-1"
    assert any(e["event"] == "created" for e in events)
