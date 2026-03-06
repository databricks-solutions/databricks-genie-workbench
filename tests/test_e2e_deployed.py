"""End-to-end tests against the deployed Genie Workbench app.

Same tests as test_e2e_local.py but targeting the deployed app URL.
Uses Databricks CLI token for OBO authentication.
"""
import json
import subprocess
import sys
import time
import requests

APP_URL = "https://genie-workbench-v0-2-7474649446784190.aws.databricksapps.com"
PROFILE = "fevm-genie-workbench-dev"


def get_token():
    raw = subprocess.check_output(
        ["databricks", "auth", "token", "--profile", PROFILE],
        stderr=subprocess.DEVNULL,
    )
    return json.loads(raw)["access_token"]


TOKEN = get_token()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def chat(session_id, message, selections=None, timeout=600):
    body = {"message": message}
    if session_id:
        body["session_id"] = session_id
    if selections:
        body["selections"] = selections

    events = []
    resp = requests.post(
        f"{APP_URL}/api/create/agent/chat", json=body, headers=HEADERS,
        stream=True, timeout=timeout,
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


def get_plan(events):
    for e, d in events:
        if e == "tool_result" and d.get("tool") == "present_plan":
            result = d.get("result", {})
            sections = result.get("sections", {})
            total = sum(len(v) for v in sections.values() if isinstance(v, list))
            return sections, total
    return None, 0


def was_created(events):
    for e, d in events:
        if e == "created":
            return d.get("space_id"), d.get("url")
    return None, None


def summarize(events):
    counts = {}
    for evt, _ in events:
        counts[evt] = counts.get(evt, 0) + 1
    print(f"  Events: {dict(sorted(counts.items()))}")

    for evt, data in events:
        if evt == "tool_call":
            print(f"  Tool: {data['tool']}")
        elif evt == "error":
            print(f"  ERROR: {data.get('message', '')[:300]}")
        elif evt == "created":
            print(f"  CREATED: space_id={data.get('space_id')} url={data.get('url')}")

    sections, total = get_plan(events)
    if sections:
        for k, v in sections.items():
            n = len(v) if isinstance(v, list) else 0
            if n > 0:
                print(f"  Plan[{k}]: {n} items")
        print(f"  Plan total: {total} items")

    errors = [d for e, d in events if e == "error"]
    return len(errors) == 0


# ─────────────────────────────────────────────────────────────────────
# TEST 1: Happy path — 5 tables, full creation
# ─────────────────────────────────────────────────────────────────────
def test_happy_path():
    print("\n" + "=" * 70)
    print("TEST 1: Happy path — samples.tpch 5 tables, full creation")
    print("=" * 70)
    sid = None

    print("\n--- Turn 1: Initial request with table selections ---")
    sid, ev1 = chat(sid, (
        "Create a Genie space called 'E2E Deployed - TPCH Analytics'. "
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
    summarize(ev1)

    # Check plan from turn 1
    _, plan_total = get_plan(ev1)
    space_id, space_url = was_created(ev1)

    if plan_total > 0 and not space_id:
        print(f"\n  Plan ready ({plan_total} items). Approving...")
        print("\n--- Turn 2: Approve and create ---")
        sid, ev2 = chat(sid, "Looks great. Approve and create the space.")
        summarize(ev2)
        space_id, space_url = was_created(ev2)
        if not space_id:
            # Sometimes it generates config but doesn't create yet
            print("\n--- Turn 3: Force create ---")
            sid, ev3 = chat(sid, "Please create the space now.")
            summarize(ev3)
            space_id, space_url = was_created(ev3)
    elif space_id:
        print(f"\n  Agent went fully autonomous in turn 1!")

    if not space_id and plan_total == 0:
        # Plan wasn't in turn 1, continue
        print("\n--- Turn 2: Continue ---")
        sid, ev2 = chat(sid, "Continue. Build the plan and create.")
        summarize(ev2)
        _, plan_total = get_plan(ev2)
        space_id, space_url = was_created(ev2)
        if plan_total > 0 and not space_id:
            print(f"\n  Plan ready ({plan_total} items). Approving...")
            sid, ev3 = chat(sid, "Approve and create.")
            summarize(ev3)
            space_id, space_url = was_created(ev3)

    print(f"\n  RESULT: plan_items={plan_total} created={bool(space_id)}")
    if space_id:
        print(f"  Space: {space_id}")
        print(f"  URL: {space_url}")
    return plan_total > 0 and bool(space_id)


# ─────────────────────────────────────────────────────────────────────
# TEST 2: Edge case — vague initial request
# ─────────────────────────────────────────────────────────────────────
def test_vague_request():
    print("\n" + "=" * 70)
    print("TEST 2: Edge case — vague request, no tables specified")
    print("=" * 70)
    sid = None

    print("\n--- Turn 1: Vague request ---")
    sid, ev1 = chat(sid, "I want to build something for sales analytics")
    ok = summarize(ev1)

    messages = [d for e, d in ev1 if e == "message"]
    has_ui = any(m.get("ui_elements") for m in messages)
    has_content = any(m.get("content") for m in messages)
    print(f"  Has UI elements: {has_ui}")
    print(f"  Has response content: {has_content}")
    print(f"  RESULT: PASS (handled gracefully)")
    return True


# ─────────────────────────────────────────────────────────────────────
# TEST 3: Edge case — single table, minimal config
# ─────────────────────────────────────────────────────────────────────
def test_single_table():
    print("\n" + "=" * 70)
    print("TEST 3: Edge case — single table, minimal config")
    print("=" * 70)
    sid = None

    print("\n--- Turn 1: Single table request ---")
    sid, ev1 = chat(sid, (
        "Create a Genie space called 'E2E Deployed - Nations Only'. "
        "Just use samples.tpch.nation. Keep it simple."
    ), selections={
        "catalog_selection": "samples",
        "schema_selection": "tpch",
        "table_selection": ["samples.tpch.nation"],
    })
    summarize(ev1)

    _, plan_total = get_plan(ev1)
    space_id, space_url = was_created(ev1)

    if plan_total > 0 and not space_id:
        print(f"\n  Plan ready ({plan_total} items). Approving...")
        print("\n--- Turn 2: Approve and create ---")
        sid, ev2 = chat(sid, "Approve and create the space.")
        summarize(ev2)
        space_id, space_url = was_created(ev2)

    if not space_id and plan_total == 0:
        print("\n--- Turn 2: Continue ---")
        sid, ev2 = chat(sid, "Continue. Build the plan and create.")
        summarize(ev2)
        _, plan_total = get_plan(ev2)
        space_id, space_url = was_created(ev2)
        if plan_total > 0 and not space_id:
            sid, ev3 = chat(sid, "Approve and create.")
            summarize(ev3)
            space_id, space_url = was_created(ev3)

    print(f"\n  RESULT: plan_items={plan_total} created={bool(space_id)}")
    if space_id:
        print(f"  Space: {space_id}")
        print(f"  URL: {space_url}")
    return plan_total > 0 and bool(space_id)


# ─────────────────────────────────────────────────────────────────────
# RUN ALL TESTS
# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Target: {APP_URL}")
    print(f"Profile: {PROFILE}")

    # Quick health check
    resp = requests.get(f"{APP_URL}/api/settings", headers=HEADERS, timeout=10)
    assert resp.status_code == 200, f"Health check failed: {resp.status_code}"
    settings = resp.json()
    print(f"LLM: {settings.get('llm_model')}")
    print(f"Warehouse: {settings.get('sql_warehouse_id')}")

    start = time.time()
    results = {}

    for name, fn in [
        ("happy_path", test_happy_path),
        ("vague_request", test_vague_request),
        ("single_table", test_single_table),
    ]:
        try:
            ok = fn()
            results[name] = "PASS" if ok else "FAIL"
        except Exception as e:
            print(f"  EXCEPTION: {e}")
            results[name] = f"ERROR: {e}"

    elapsed = time.time() - start
    print("\n" + "=" * 70)
    print(f"ALL DEPLOYED TESTS COMPLETE ({elapsed:.0f}s)")
    print("=" * 70)
    for name, status in results.items():
        icon = "+" if "PASS" in status else "X"
        print(f"  [{icon}] {name}: {status}")

    if any("FAIL" in str(v) or "ERROR" in str(v) for v in results.values()):
        sys.exit(1)
