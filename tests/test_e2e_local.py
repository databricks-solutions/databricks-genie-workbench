"""End-to-end local tests for the Genie Workbench create agent.

Tests the SSE chat endpoint through multiple conversation scenarios.
"""
import json
import sys
import time
import requests

BASE = "http://localhost:8000"
RESULTS = {}


def chat(session_id, message, selections=None, timeout=600):
    body = {"message": message}
    if session_id:
        body["session_id"] = session_id
    if selections:
        body["selections"] = selections

    events = []
    resp = requests.post(
        f"{BASE}/api/create/agent/chat", json=body, stream=True, timeout=timeout,
    )
    current_event = None
    for line in resp.iter_lines(decode_unicode=True):
        if not line:
            continue
        if line.startswith("event: "):
            current_event = line[7:]
        elif line.startswith("data: "):
            try:
                data = json.loads(line[6:])
            except json.JSONDecodeError:
                data = {"raw": line[6:]}
            events.append((current_event, data))
            if current_event == "session":
                session_id = data.get("session_id", session_id)
    resp.close()
    return session_id, events


def get_event(events, event_type):
    return [d for e, d in events if e == event_type]


def get_plan(events):
    for e, d in events:
        if e == "tool_result" and d.get("tool") == "present_plan":
            result = d.get("result", {})
            sections = result.get("sections", {})
            total = sum(len(v) for v in sections.values() if isinstance(v, list))
            return sections, total
    return None, 0


def summarize(events):
    counts = {}
    for evt, _ in events:
        counts[evt] = counts.get(evt, 0) + 1
    print(f"  Events: {dict(sorted(counts.items()))}")

    for evt, data in events:
        if evt == "tool_call":
            args_keys = list(data.get("args", {}).keys())[:5]
            print(f"  Tool: {data['tool']}(keys={args_keys})")
        elif evt == "error":
            print(f"  ERROR: {data.get('message', '')[:300]}")
        elif evt == "created":
            print(f"  CREATED: space_id={data.get('space_id')} url={data.get('url')}")

    sections, total = get_plan(events)
    if sections:
        for k, v in sections.items():
            n = len(v) if isinstance(v, list) else 0
            print(f"  Plan[{k}]: {n} items")
        print(f"  Plan total: {total} items")

    errors = get_event(events, "error")
    return len(errors) == 0


# ─────────────────────────────────────────────────────────────────────
# TEST 1: Happy path — full autonomous creation from samples.tpch
# ─────────────────────────────────────────────────────────────────────
def test_happy_path():
    print("\n" + "=" * 70)
    print("TEST 1: Happy path — samples.tpch full creation")
    print("=" * 70)
    sid = None

    # Turn 1: initial request with pre-selected tables
    print("\n--- Turn 1: Initial request with table selections ---")
    sid, events = chat(sid, (
        "Create a Genie space called 'E2E Test - TPCH Analytics'. "
        "Use samples.tpch with tables: orders, lineitem, customer, nation, supplier. "
        "Purpose: supply chain analytics. Key metrics: revenue, order count, avg delivery days. "
        "Go autonomous — inspect, plan, create."
    ), selections={
        "catalog_selection": "samples",
        "schema_selection": "tpch",
        "table_selection": [
            "samples.tpch.orders", "samples.tpch.lineitem",
            "samples.tpch.customer", "samples.tpch.nation",
            "samples.tpch.supplier",
        ],
    })
    ok = summarize(events)

    # Turn 2: continue
    print("\n--- Turn 2: Continue ---")
    sid, events2 = chat(sid, "Continue. Inspect all tables and build the plan.")
    ok2 = summarize(events2)

    # Check plan
    sections, total = get_plan(events2)
    if total == 0:
        # Plan might be in a later turn
        print("\n--- Turn 2b: Plan not ready, asking again ---")
        sid, events2b = chat(sid, "Please present the plan now.")
        ok2 = summarize(events2b)
        sections, total = get_plan(events2b)

    plan_ok = total > 0
    print(f"\n  PLAN CHECK: {'PASS' if plan_ok else 'FAIL'} ({total} items)")

    # Turn 3: approve and create
    print("\n--- Turn 3: Approve and create ---")
    sid, events3 = chat(sid, "Looks great. Approve and create the space.")
    ok3 = summarize(events3)

    created = any(e == "created" for e, _ in events3)
    if not created:
        print("\n--- Turn 3b: Space not created yet, asking again ---")
        sid, events3b = chat(sid, "Please create the space now.", selections={"action": "create"})
        summarize(events3b)
        created = any(e == "created" for e, _ in events3b)

    print(f"\n  RESULT: plan_items={total} created={created}")
    return plan_ok and created, sid


# ─────────────────────────────────────────────────────────────────────
# TEST 2: Edge case — vague initial request (no table names)
# ─────────────────────────────────────────────────────────────────────
def test_vague_request():
    print("\n" + "=" * 70)
    print("TEST 2: Edge case — vague request, no tables specified")
    print("=" * 70)
    sid = None

    print("\n--- Turn 1: Vague request ---")
    sid, events = chat(sid, "I want to build something for sales analytics")
    ok = summarize(events)

    # Should ask for catalog/schema selection
    messages = get_event(events, "message")
    has_ui = any(m.get("ui_elements") for m in messages)
    print(f"  Has UI elements (catalog picker): {has_ui}")
    return True, sid  # Just checking it doesn't crash


# ─────────────────────────────────────────────────────────────────────
# TEST 3: Edge case — single table, minimal config
# ─────────────────────────────────────────────────────────────────────
def test_single_table():
    print("\n" + "=" * 70)
    print("TEST 3: Edge case — single table, minimal request")
    print("=" * 70)
    sid = None

    print("\n--- Turn 1: Single table request ---")
    sid, events = chat(sid, (
        "Create a Genie space called 'E2E Test - Nations Only'. "
        "Just use samples.tpch.nation. Keep it simple."
    ), selections={
        "catalog_selection": "samples",
        "schema_selection": "tpch",
        "table_selection": ["samples.tpch.nation"],
    })
    ok = summarize(events)

    print("\n--- Turn 2: Continue to plan ---")
    sid, events2 = chat(sid, "Continue. Build the plan and create the space.")
    ok2 = summarize(events2)

    sections, total = get_plan(events2)
    if total == 0:
        print("\n--- Turn 2b: Plan not ready ---")
        sid, events2b = chat(sid, "Present the plan please.")
        summarize(events2b)
        sections, total = get_plan(events2b)

    plan_ok = total > 0
    print(f"\n  PLAN CHECK: {'PASS' if plan_ok else 'FAIL'} ({total} items)")
    return plan_ok, sid


# ─────────────────────────────────────────────────────────────────────
# RUN ALL TESTS
# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start = time.time()
    results = {}

    # Test 1: Happy path
    try:
        ok, sid = test_happy_path()
        results["happy_path"] = "PASS" if ok else "FAIL"
    except Exception as e:
        print(f"  EXCEPTION: {e}")
        results["happy_path"] = f"ERROR: {e}"

    # Test 2: Vague request
    try:
        ok, sid = test_vague_request()
        results["vague_request"] = "PASS" if ok else "FAIL"
    except Exception as e:
        print(f"  EXCEPTION: {e}")
        results["vague_request"] = f"ERROR: {e}"

    # Test 3: Single table
    try:
        ok, sid = test_single_table()
        results["single_table"] = "PASS" if ok else "FAIL"
    except Exception as e:
        print(f"  EXCEPTION: {e}")
        results["single_table"] = f"ERROR: {e}"

    elapsed = time.time() - start
    print("\n" + "=" * 70)
    print(f"ALL TESTS COMPLETE ({elapsed:.0f}s)")
    print("=" * 70)
    for name, status in results.items():
        print(f"  {name}: {status}")

    if any("FAIL" in str(v) or "ERROR" in str(v) for v in results.values()):
        sys.exit(1)
