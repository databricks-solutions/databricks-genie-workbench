"""Streaming response-shape tests for the Create Agent."""

import asyncio

from backend.services import create_agent as create_agent_module
from backend.services.create_agent import CreateGenieAgent
from backend.services.create_agent_session import AgentSession


def _collect_events(agent: CreateGenieAgent, session: AgentSession, message: str):
    async def run():
        return [event async for event in agent.chat(session, message)]

    return asyncio.run(run())


def test_structured_content_blocks_stream_as_plain_text():
    agent = CreateGenieAgent()
    agent._build_messages = lambda session: []

    async def fake_stream(messages, tools=None, model=None):
        yield {
            "choices": [{
                "delta": {
                    "content": [
                        {"type": "metadata", "signature": "ignored"},
                        {"type": "text", "text": "Great — "},
                    ]
                }
            }]
        }
        yield {"choices": [{"delta": {"content": "let's continue."}}]}

    agent._async_stream_llm = fake_stream
    session = AgentSession(session_id="structured-text")

    events = _collect_events(agent, session, "yes")

    deltas = [event["data"]["content"] for event in events if event["event"] == "message_delta"]
    final_message = next(event for event in events if event["event"] == "message")
    assert deltas == ["Great — ", "let's continue."]
    assert final_message["data"]["content"] == "Great — let's continue."
    assert not any(event["event"] == "error" for event in events)
    assert session.history[-1] == {
        "role": "assistant",
        "content": "Great — let's continue.",
    }


def test_structured_content_blocks_do_not_break_tool_calling(monkeypatch):
    agent = CreateGenieAgent()
    agent._build_messages = lambda session: []

    async def fake_stream(messages, tools=None, model=None):
        yield {
            "choices": [{
                "delta": {
                    "content": [{"type": "text", "text": "I'll look now."}]
                }
            }]
        }
        yield {
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "id": "call-1",
                        "function": {
                            "name": "discover_catalogs",
                            "arguments": "{}",
                        },
                    }]
                }
            }]
        }

    agent._async_stream_llm = fake_stream
    monkeypatch.setattr(
        create_agent_module,
        "handle_tool_call",
        lambda name, arguments, session_config=None: {"catalogs": []},
    )
    session = AgentSession(session_id="structured-tool")

    events = _collect_events(agent, session, "browse catalogs")

    assert any(event["event"] == "tool_call" for event in events)
    assert any(event["event"] == "tool_result" for event in events)
    assert not any(event["event"] == "error" for event in events)
    done = next(event for event in events if event["event"] == "done")
    assert done["data"]["needs_continuation"] is True
    assistant_tool_message = next(
        message for message in session.history
        if message["role"] == "assistant" and message.get("tool_calls")
    )
    assert assistant_tool_message["content"] == "I'll look now."
