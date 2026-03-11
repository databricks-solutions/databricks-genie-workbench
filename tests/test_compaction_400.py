"""Test: verify message building after inspection → plan step transition.

Simulates the user's exact scenario: auto-pilot, 1 table (nyctaxi trips),
full inspection with describe_table + assess_data_quality + profile_table_usage.
Verifies:
  1. detect_step returns "plan" after inspection completes
  2. Message list preserves ALL tool calls (no compaction erasure)
  3. Messages end with a user message (Claude endpoint requirement)
  4. No consecutive assistant messages
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.services.create_agent_session import AgentSession
from backend.services.create_agent import CreateGenieAgent
from backend.prompts_create import detect_step


def build_nyctaxi_autopilot_session() -> AgentSession:
    """Build a session matching the user's scenario: 1 nyctaxi table, auto-pilot,
    describe_table + assess_data_quality + profile_table_usage all completed."""
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
    """Check for Claude API message structure violations."""
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

    # Check for consecutive assistant messages (no user/tool in between)
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

    # Check last message is user
    last = non_system[-1]
    if last["role"] != "user":
        errors.append(f"Last message should be 'user', got '{last['role']}'")

    # Check for orphaned tool results
    tc_ids_defined = set()
    for msg in messages:
        for tc in msg.get("tool_calls", []):
            tc_ids_defined.add(tc["id"])

    for msg in messages:
        if msg["role"] == "tool":
            tcid = msg.get("tool_call_id")
            if tcid and tcid not in tc_ids_defined:
                errors.append(f"Orphaned tool result: {tcid}")

    # Check all tool_calls have results
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


def main():
    agent = CreateGenieAgent()
    session = build_nyctaxi_autopilot_session()

    step = detect_step(session)
    print(f"Step detection: {step}")
    assert step == "plan", f"Expected 'plan', got '{step}'"
    print("  ✓ detect_step correctly returns 'plan' after inspection")

    messages = agent._build_messages(session)

    print(f"\nMessage structure ({len(messages)} messages):")
    for i, m in enumerate(messages):
        role = m["role"]
        has_tc = bool(m.get("tool_calls"))
        tc_id = m.get("tool_call_id", "")
        content_len = len(str(m.get("content", "") or ""))
        if role == "system":
            print(f"  [{i}] {role}: {content_len} chars")
        elif has_tc:
            tc_names = [tc["function"]["name"] for tc in m["tool_calls"]]
            print(f"  [{i}] {role}: tool_calls=[{', '.join(tc_names)}], content={content_len}")
        elif role == "tool":
            name = m.get("name", "?")
            print(f"  [{i}] {role}: {name} (tc_id={tc_id}), {content_len} chars")
        else:
            preview = str(m.get("content", ""))[:60]
            print(f"  [{i}] {role}: {content_len} chars — {preview!r}")

    # Verify inspection tool calls are PRESERVED (not compacted away)
    desc_count = find_tool_calls_in_messages(messages, "describe_table")
    assess_count = find_tool_calls_in_messages(messages, "assess_data_quality")
    usage_count = find_tool_calls_in_messages(messages, "profile_table_usage")
    print(f"\nInspection tool calls preserved in messages:")
    print(f"  describe_table:      {desc_count}")
    print(f"  assess_data_quality: {assess_count}")
    print(f"  profile_table_usage: {usage_count}")
    assert desc_count >= 1, "describe_table should be in messages!"
    assert assess_count >= 1, "assess_data_quality should be in messages!"
    assert usage_count >= 1, "profile_table_usage should be in messages!"
    print("  ✓ All inspection tool calls preserved — LLM can see they already ran")

    errors = validate_messages(messages)
    print(f"\nMessage structure validation:")
    if errors:
        print(f"  FOUND {len(errors)} ERRORS:")
        for e in errors:
            print(f"    ✗ {e}")
    else:
        print("  ✓ Valid message structure (user-last, no consecutive assistants)")

    total_chars = sum(len(json.dumps(m)) for m in messages)
    print(f"\nTotal payload: ~{total_chars:,} chars (~{total_chars // 4:,} tokens)")

    all_passed = step == "plan" and desc_count >= 1 and assess_count >= 1 and usage_count >= 1 and not errors
    print(f"\n{'=' * 50}")
    print(f"{'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")
    return 0 if all_passed else 1


if __name__ == "__main__":
    exit(main())
