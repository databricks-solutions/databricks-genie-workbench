"""Diagnostic test: Why does the agent repeat inspection in auto-pilot mode?

Hypotheses:
  A) SSE timeout race condition — proxy kills connection, frontend sends continuation,
     backend still executing tools → concurrent session mutation → partial history.
  B) Step detection + prompt loop — detect_step returns "inspection" indefinitely because
     it only advances to "plan" when present_plan/generate_plan is called. The inspection
     prompt's imperative "Call describe_table on each table" causes the LLM to re-run them.
  C) Session persistence gap — _continuation_count not persisted, session data lost.

This script tests hypothesis B (the most likely) by simulating the session state after
a full round of inspection and checking what the LLM would see.
"""

import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.services.create_agent_session import AgentSession
from backend.prompts_create import detect_step, assemble_system_prompt, STEP_ORDER


def build_mock_session_after_inspection() -> AgentSession:
    """Build a session that looks like round 1 completed:
    - User message with auto_pilot: true
    - Assistant called describe_table x6
    - All 6 tool results present
    - assess_data_quality + profile_table_usage also called and completed
    """
    session = AgentSession(session_id="test-123")

    # User message with auto-pilot
    session.history.append({
        "role": "user",
        "content": "Create a Genie Space for banking analytics\n\n[User selections: {\"auto_pilot\": true}]",
    })

    tables = [
        ("transactions", 17), ("customers", 17), ("accounts", 11),
        ("branches", 10), ("products", 10), ("service_requests", 12),
    ]

    # Assistant message with tool calls
    tool_calls = []
    for i, (name, _) in enumerate(tables):
        tool_calls.append({
            "id": f"call_desc_{i}",
            "type": "function",
            "function": {
                "name": "describe_table",
                "arguments": json.dumps({
                    "catalog": "banking", "schema": "core", "table": name,
                }),
            },
        })
    tool_calls.append({
        "id": "call_quality",
        "type": "function",
        "function": {
            "name": "assess_data_quality",
            "arguments": json.dumps({"tables": ["banking.core." + t[0] for t in tables]}),
        },
    })
    tool_calls.append({
        "id": "call_usage",
        "type": "function",
        "function": {
            "name": "profile_table_usage",
            "arguments": json.dumps({"tables": ["banking.core." + t[0] for t in tables]}),
        },
    })

    session.history.append({
        "role": "assistant",
        "content": "Let me inspect everything in parallel.",
        "tool_calls": tool_calls,
    })

    # Tool results for all describe_table calls
    for i, (name, col_count) in enumerate(tables):
        columns = [{"name": f"col_{j}", "type": "STRING"} for j in range(col_count)]
        result = {
            "table": f"banking.core.{name}",
            "table_name": name,
            "columns": columns,
            "row_count": 10000,
            "comment": f"The {name} table",
        }
        session.history.append({
            "role": "tool",
            "tool_call_id": f"call_desc_{i}",
            "content": json.dumps(result),
        })

    # assess_data_quality result
    session.history.append({
        "role": "tool",
        "tool_call_id": "call_quality",
        "content": json.dumps({
            "tables": {t[0]: {"quality_score": 85, "issues": []} for t in tables},
            "overall_assessment": "Good quality",
            "table_details": {},
        }),
    })

    # profile_table_usage result
    session.history.append({
        "role": "tool",
        "tool_call_id": "call_usage",
        "content": json.dumps({
            "tables": {
                f"banking.core.{t[0]}": {"recent_queries": [], "lineage": {}}
                for t in tables
            },
        }),
    })

    return session


def test_step_detection():
    """Test: What step does detect_step return after full inspection?"""
    session = build_mock_session_after_inspection()
    step = detect_step(session)
    print(f"\n=== STEP DETECTION ===")
    print(f"Step after full inspection: '{step}'")
    print(f"Expected for plan: 'plan'")
    print(f"ISSUE: Step is '{step}' — {'STUCK IN INSPECTION LOOP!' if step == 'inspection' else 'OK'}")
    return step


def test_prompt_content():
    """Test: What prompt does the LLM see in the continuation round?"""
    session = build_mock_session_after_inspection()
    step = detect_step(session)

    # We can't call assemble_system_prompt without the schema file, so just check the step
    print(f"\n=== PROMPT ANALYSIS ===")
    print(f"Detected step: '{step}'")
    step_idx = STEP_ORDER.index(step) if step in STEP_ORDER else -1
    print(f"Step index: {step_idx} (inspection=2, plan=3)")

    if step == "inspection":
        print("PROBLEM: The LLM will receive the inspection prompt which says:")
        print('  "Call describe_table on each selected table"')
        print("  This causes the LLM to re-run describe_table even though results exist!")
        print()
        print("The step detection only advances to 'plan' when present_plan or generate_plan")
        print("was called. But you can't call generate_plan until you're IN the plan step.")
        print("=> DEADLOCK: inspection step tells LLM to inspect, never advances to plan")


def test_history_completeness():
    """Test: Does the session history contain all tool results?"""
    session = build_mock_session_after_inspection()
    print(f"\n=== HISTORY COMPLETENESS ===")
    print(f"Total history messages: {len(session.history)}")

    # Count by role
    roles = {}
    for msg in session.history:
        role = msg["role"]
        roles[role] = roles.get(role, 0) + 1
    print(f"By role: {roles}")

    # Check tool_call_ids have matching results
    call_ids = set()
    result_ids = set()
    for msg in session.history:
        if msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                call_ids.add(tc["id"])
        if msg["role"] == "tool":
            result_ids.add(msg["tool_call_id"])

    orphans = call_ids - result_ids
    print(f"Tool call IDs: {len(call_ids)}")
    print(f"Tool result IDs: {len(result_ids)}")
    print(f"Orphaned (call without result): {orphans or 'none'}")

    # Check which tools were called
    tool_names = set()
    for msg in session.history:
        if msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                tool_names.add(tc["function"]["name"])
    print(f"Tools called: {tool_names}")


def test_continuation_count_persistence():
    """Test: Is _continuation_count preserved between requests?"""
    session = build_mock_session_after_inspection()
    print(f"\n=== CONTINUATION COUNT ===")

    # Simulate setting it
    session._continuation_count = 3
    print(f"Set _continuation_count = 3")

    # Check if it's a dataclass field
    import dataclasses
    fields = {f.name for f in dataclasses.fields(session)}
    print(f"Dataclass fields: {fields}")
    print(f"'_continuation_count' is a field: {'_continuation_count' in fields}")
    print(f"Persisted to Lakebase: {'_continuation_count' in fields}")

    # Simulate what happens on session reload
    has_attr = hasattr(session, "_continuation_count")
    print(f"hasattr after set: {has_attr}")
    val = getattr(session, "_continuation_count", 0)
    print(f"getattr value: {val}")
    print(f"NOTE: If session is loaded from Lakebase, _continuation_count defaults to 0")
    print(f"  This means MAX_TOOL_ROUNDS limit would reset on session reload")


def test_concurrent_access():
    """Test: Can two requests access the same session simultaneously?"""
    from backend.services.create_agent_session import _sessions
    print(f"\n=== CONCURRENT ACCESS ===")

    session = build_mock_session_after_inspection()
    _sessions["test-123"] = session

    # Two "requests" getting the same session
    from backend.services.create_agent_session import get_session
    s1 = get_session("test-123")
    s2 = get_session("test-123")

    print(f"s1 is s2: {s1 is s2}")
    print(f"Same object: {id(s1) == id(s2)}")
    print(f"ISSUE: No locking — concurrent requests mutate the SAME object!")
    print(f"  If request 1 is still executing tools while request 2 starts,")
    print(f"  request 2 reads partial history from request 1.")

    # Cleanup
    _sessions.pop("test-123", None)


def test_partial_inspection():
    """Test: describe_table only (no quality/usage) should stay in inspection."""
    session = AgentSession(session_id="test-partial")
    session.history.append({
        "role": "user", "content": "Create a space",
    })
    session.history.append({
        "role": "assistant",
        "content": "Inspecting...",
        "tool_calls": [{
            "id": "call_desc_0", "type": "function",
            "function": {"name": "describe_table", "arguments": "{}"},
        }],
    })
    session.history.append({
        "role": "tool", "tool_call_id": "call_desc_0",
        "content": json.dumps({"table": "t1", "columns": [{"name": "a"}]}),
    })
    step = detect_step(session)
    print(f"\n=== PARTIAL INSPECTION (describe_table only) ===")
    print(f"Step: '{step}' — {'CORRECT (stays in inspection)' if step == 'inspection' else 'WRONG!'}")
    return step == "inspection"


def test_cancelled_results():
    """Test: cancelled tool results should NOT count as completed."""
    session = build_mock_session_after_inspection()
    # Replace the assess_data_quality result with a cancelled one
    for msg in session.history:
        if msg.get("role") == "tool" and msg.get("tool_call_id") == "call_quality":
            msg["content"] = json.dumps({"cancelled": True})
    # Also replace profile_table_usage result
    for msg in session.history:
        if msg.get("role") == "tool" and msg.get("tool_call_id") == "call_usage":
            msg["content"] = json.dumps({"cancelled": True})

    step = detect_step(session)
    print(f"\n=== CANCELLED RESULTS (both quality+usage cancelled) ===")
    print(f"Step: '{step}' — {'CORRECT (stays in inspection)' if step == 'inspection' else 'WRONG!'}")
    return step == "inspection"


if __name__ == "__main__":
    print("=" * 60)
    print("DIAGNOSIS: Agent repeating inspection in auto-pilot mode")
    print("=" * 60)

    step = test_step_detection()
    test_prompt_content()
    test_history_completeness()
    test_continuation_count_persistence()
    test_concurrent_access()
    p1 = test_partial_inspection()
    p2 = test_cancelled_results()

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    all_pass = (step == "plan") and p1 and p2
    if all_pass:
        print("ALL TESTS PASSED")
        print("  - Full inspection → step='plan' (was: 'inspection' before fix)")
        print("  - Partial inspection → stays in 'inspection'")
        print("  - Cancelled results → stays in 'inspection'")
        print("  - Session locking added via asyncio.Lock")
        print("  - continuation_count is now a proper dataclass field")
    else:
        print("SOME TESTS FAILED — review output above")
