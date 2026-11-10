"""Structural invariants of CreateGenieAgent._build_messages, plus AgentSession
regression checks.

Ported from the retired root scripts tests/test_compaction_400.py and
tests/test_inspection_loop.py, which no runner collected. Every other backend
test that reaches _build_messages replaces it with a lambda, so this is the only
place its real output is checked.
"""

import asyncio
import dataclasses
import json

import pytest

from backend.prompts_create import detect_step
from backend.services.create_agent import CreateGenieAgent
from backend.services.create_agent_session import AgentSession, _sessions, get_session


def build_nyctaxi_autopilot_session() -> AgentSession:
    """Auto-pilot session on one nyctaxi table, with describe_table,
    assess_data_quality and profile_table_usage all completed."""
    session = AgentSession(session_id="test-nyctaxi")
    tc_counter = [0]

    def tc_id():
        tc_counter[0] += 1
        return f"call_{tc_counter[0]}"

    session.history.append({
        "role": "user",
        "content": "autopilot make a space on nyctaxi\n\n[User selections: {\"auto_pilot\": true}]",
    })

    # discover_catalogs
    cid = tc_id()
    session.history.append({
        "role": "assistant", "content": "Let me explore the catalogs.",
        "tool_calls": [{"id": cid, "type": "function", "function": {"name": "discover_catalogs", "arguments": "{}"}}],
    })
    session.history.append({"role": "tool", "tool_call_id": cid, "content": json.dumps({"catalogs": [{"name": "samples"}]})})

    # discover_schemas
    cid = tc_id()
    session.history.append({
        "role": "assistant", "content": "Found the samples catalog.",
        "tool_calls": [{"id": cid, "type": "function", "function": {"name": "discover_schemas", "arguments": json.dumps({"catalog": "samples"})}}],
    })
    session.history.append({"role": "tool", "tool_call_id": cid, "content": json.dumps({"schemas": [{"name": "nyctaxi"}]})})

    # discover_tables
    cid = tc_id()
    session.history.append({
        "role": "assistant", "content": "Found the nyctaxi schema.",
        "tool_calls": [{"id": cid, "type": "function", "function": {"name": "discover_tables", "arguments": json.dumps({"catalog": "samples", "schema": "nyctaxi"})}}],
    })
    session.history.append({"role": "tool", "tool_call_id": cid, "content": json.dumps({"tables": [{"name": "trips"}]})})

    # describe_table (trips)
    cid = tc_id()
    session.history.append({
        "role": "assistant", "content": "Inspecting the trips table.",
        "tool_calls": [{"id": cid, "type": "function", "function": {"name": "describe_table", "arguments": json.dumps({"catalog": "samples", "schema": "nyctaxi", "table": "trips"})}}],
    })
    session.history.append({"role": "tool", "tool_call_id": cid, "content": json.dumps({
        "table_name": "samples.nyctaxi.trips",
        "columns": [{"name": f"col_{j}", "type": "STRING"} for j in range(6)],
        "row_count": 100000,
    })})

    # assess_data_quality + profile_table_usage (same round)
    qa_id = tc_id()
    usage_id = tc_id()
    session.history.append({
        "role": "assistant", "content": "Running quality checks and usage profiling.",
        "tool_calls": [
            {"id": qa_id, "type": "function", "function": {"name": "assess_data_quality", "arguments": json.dumps({"tables": ["samples.nyctaxi.trips"]})}},
            {"id": usage_id, "type": "function", "function": {"name": "profile_table_usage", "arguments": json.dumps({"tables": ["samples.nyctaxi.trips"]})}},
        ],
    })
    session.history.append({"role": "tool", "tool_call_id": qa_id, "content": json.dumps({
        "tables": {"trips": {"quality_score": 95, "issues": []}}, "overall_assessment": "Good",
    })})
    session.history.append({"role": "tool", "tool_call_id": usage_id, "content": json.dumps({
        "tables": {"samples.nyctaxi.trips": {"recent_queries": [{"query": "SELECT *", "executed_by": "user1"}], "lineage": {"downstream": ["gold_analytics"]}}},
    })})

    return session


def validate_messages(messages: list[dict]) -> list[str]:
    """Return Claude API message-structure violations (empty list == valid)."""
    errors = []

    if not messages:
        errors.append("Empty messages array")
        return errors

    non_system = [m for m in messages if m["role"] != "system"]
    if not non_system:
        errors.append("No non-system messages")
        return errors

    if non_system[0]["role"] != "user":
        errors.append(f"First non-system message should be 'user', got '{non_system[0]['role']}'")

    # Consecutive assistant messages (no user/tool in between)
    prev_role = None
    prev_idx = -1
    for i, msg in enumerate(messages):
        role = msg["role"]
        if role == "system":
            continue
        if role == "assistant" and prev_role == "assistant":
            errors.append(f"Consecutive assistant messages at {prev_idx} and {i}")
        prev_role = role
        prev_idx = i

    # The Claude endpoint requires the last message to be from the user
    last = non_system[-1]
    if last["role"] != "user":
        errors.append(f"Last message should be 'user', got '{last['role']}'")

    # Orphaned tool results
    tc_ids_defined = set()
    for msg in messages:
        for tc in msg.get("tool_calls", []):
            tc_ids_defined.add(tc["id"])

    for msg in messages:
        if msg["role"] == "tool":
            tcid = msg.get("tool_call_id")
            if tcid and tcid not in tc_ids_defined:
                errors.append(f"Orphaned tool result: {tcid}")

    # Every tool_call has a result
    tc_ids_with_results = {msg.get("tool_call_id") for msg in messages if msg["role"] == "tool"}
    for msg in messages:
        for tc in msg.get("tool_calls", []):
            if tc["id"] not in tc_ids_with_results:
                errors.append(f"Tool call without result: {tc['id']} ({tc['function']['name']})")

    return errors


def find_tool_calls_in_messages(messages: list[dict], tool_name: str) -> int:
    """Count how many times a tool appears in the message list."""
    count = 0
    for msg in messages:
        for tc in msg.get("tool_calls", []):
            if tc.get("function", {}).get("name") == tool_name:
                count += 1
    return count


@pytest.fixture
def messages() -> list[dict]:
    return CreateGenieAgent()._build_messages(build_nyctaxi_autopilot_session())


# --- _build_messages ---

def test_fixture_session_is_past_inspection():
    """Guards the fixture: it must represent the post-inspection state."""
    assert detect_step(build_nyctaxi_autopilot_session()) == "profiling"


def test_build_messages_structure_is_valid(messages):
    assert validate_messages(messages) == []


@pytest.mark.parametrize("tool_name", ["describe_table", "assess_data_quality", "profile_table_usage"])
def test_build_messages_preserves_inspection_tool_calls(messages, tool_name):
    """Completed inspection calls stay visible, so the LLM doesn't re-run them."""
    assert find_tool_calls_in_messages(messages, tool_name) >= 1


# --- AgentSession regressions (fixes diagnosed by test_inspection_loop.py) ---

def test_continuation_count_is_a_dataclass_field():
    """It used to be an ad-hoc `_continuation_count` attribute outside the dataclass."""
    names = {f.name for f in dataclasses.fields(AgentSession)}
    assert "continuation_count" in names
    assert AgentSession(session_id="x").continuation_count == 0


def test_session_lock_is_per_session_and_excluded_from_eq_and_repr():
    lock_field = {f.name: f for f in dataclasses.fields(AgentSession)}["_lock"]
    assert lock_field.compare is False
    assert lock_field.repr is False

    a = AgentSession(session_id="x")
    b = AgentSession(session_id="y")
    assert isinstance(a._lock, asyncio.Lock)
    assert a._lock is not b._lock
    assert "_lock" not in repr(a)


def test_get_session_shares_one_object_and_lock(monkeypatch):
    """Concurrent requests get the same session object, so they must serialize on
    its lock (the create router takes `async with session._lock`)."""
    session = AgentSession(session_id="test-shared-lock")
    monkeypatch.setitem(_sessions, session.session_id, session)

    s1 = get_session(session.session_id)
    s2 = get_session(session.session_id)
    assert s1 is s2
    assert s1._lock is s2._lock
