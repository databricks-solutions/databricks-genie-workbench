"""Tests for dynamic prompt assembly in backend.prompts_create."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from backend.services.create_agent_session import AgentSession
from backend.prompts_create import (
    detect_step,
    assemble_system_prompt,
    STEP_ORDER,
    STEP_PROMPTS,
    STEP_SUMMARIES,
)


def _make_session(**kwargs) -> AgentSession:
    return AgentSession(session_id="test", **kwargs)


def _add_tool_call(session: AgentSession, tool_name: str) -> None:
    session.history.append({
        "role": "assistant",
        "content": "",
        "tool_calls": [{
            "id": f"tc_{tool_name}",
            "type": "function",
            "function": {"name": tool_name, "arguments": "{}"},
        }],
    })
    session.history.append({
        "role": "tool",
        "tool_call_id": f"tc_{tool_name}",
        "content": '{"ok": true}',
    })


# --- detect_step tests ---

def test_detect_step_empty_session():
    session = _make_session()
    assert detect_step(session) == "requirements"


def test_detect_step_after_discover_schemas():
    session = _make_session()
    _add_tool_call(session, "discover_catalogs")
    _add_tool_call(session, "discover_schemas")
    assert detect_step(session) == "discovery"


def test_detect_step_after_discover_tables():
    session = _make_session()
    _add_tool_call(session, "discover_catalogs")
    _add_tool_call(session, "discover_schemas")
    _add_tool_call(session, "discover_tables")
    assert detect_step(session) == "feasibility"


def test_detect_step_after_describe_table():
    session = _make_session()
    _add_tool_call(session, "discover_tables")
    _add_tool_call(session, "describe_table")
    assert detect_step(session) == "inspection"


def test_detect_step_after_present_plan():
    session = _make_session()
    _add_tool_call(session, "discover_tables")
    _add_tool_call(session, "describe_table")
    _add_tool_call(session, "present_plan")
    assert detect_step(session) == "plan"


def test_detect_step_with_config():
    session = _make_session(space_config={"tables": []})
    _add_tool_call(session, "present_plan")
    _add_tool_call(session, "generate_config")
    assert detect_step(session) == "config_create"


def test_detect_step_post_creation():
    session = _make_session(space_id="abc123", space_config={"tables": []})
    assert detect_step(session) == "post_creation"


def test_detect_step_feasibility_via_session_state():
    """Session state (selected_tables) should gate feasibility even without discover_tables tool call."""
    session = _make_session()
    session.selected_tables = ["catalog.schema.table1", "catalog.schema.table2"]
    assert detect_step(session) == "feasibility"


def test_detect_step_inspection_via_feasibility_confirmed():
    """feasibility_confirmed should advance to inspection even without describe_table."""
    session = _make_session()
    session.selected_tables = ["catalog.schema.table1"]
    session.feasibility_confirmed = True
    assert detect_step(session) == "inspection"


# --- assemble_system_prompt tests ---

SCHEMA_REF = "## Fake Schema\ntest content"


def test_assemble_requirements():
    session = _make_session()
    prompt = assemble_system_prompt(session, SCHEMA_REF)

    assert "expert Databricks Genie Space creation agent" in prompt
    assert "Gather Requirements" in prompt
    assert "Discovery" not in prompt or "Adjacent Steps" in prompt
    assert "Schema Reference" in prompt
    assert SCHEMA_REF in prompt


def test_assemble_discovery():
    session = _make_session()
    _add_tool_call(session, "discover_schemas")
    prompt = assemble_system_prompt(session, SCHEMA_REF)

    assert "Discovery" in prompt
    assert "Adjacent Steps" in prompt
    assert "Step 1 (Requirements)" in prompt   # previous step summary
    assert "Step 3 (Feasibility)" in prompt    # next step summary


def test_assemble_feasibility():
    session = _make_session()
    _add_tool_call(session, "discover_tables")
    prompt = assemble_system_prompt(session, SCHEMA_REF)

    assert "Feasibility Assessment" in prompt
    assert "Adjacent Steps" in prompt
    assert "Step 2 (Discovery)" in prompt      # previous step summary
    assert "Step 4 (Inspection)" in prompt     # next step summary


def test_assemble_inspection():
    session = _make_session()
    _add_tool_call(session, "discover_tables")
    _add_tool_call(session, "describe_table")
    prompt = assemble_system_prompt(session, SCHEMA_REF)

    assert "Inspect & Understand the Data" in prompt
    assert "Step 3 (Feasibility)" in prompt    # previous
    assert "Step 5 (Plan)" in prompt           # next


def test_assemble_includes_backtracking():
    session = _make_session()
    prompt = assemble_system_prompt(session, SCHEMA_REF)
    assert "Handling Changes & Backtracking" in prompt


def test_assemble_includes_tool_rules():
    session = _make_session()
    prompt = assemble_system_prompt(session, SCHEMA_REF)
    assert "Tool Usage Rules" in prompt


def test_prompt_shrinks_vs_monolithic():
    """Dynamic prompt should be materially shorter than the monolithic one."""
    from backend.prompts import get_create_agent_system_prompt

    monolithic = get_create_agent_system_prompt(SCHEMA_REF)
    session = _make_session()
    dynamic = assemble_system_prompt(session, SCHEMA_REF)

    mono_len = len(monolithic)
    dyn_len = len(dynamic)
    print(f"Monolithic: {mono_len} chars, Dynamic (requirements): {dyn_len} chars")
    print(f"Reduction: {(1 - dyn_len / mono_len) * 100:.0f}%")

    assert dyn_len < mono_len, f"Dynamic ({dyn_len}) should be smaller than monolithic ({mono_len})"


def test_all_steps_have_prompts_and_summaries():
    for step in STEP_ORDER:
        assert step in STEP_PROMPTS, f"Missing prompt for step: {step}"
        assert step in STEP_SUMMARIES, f"Missing summary for step: {step}"
        assert len(STEP_PROMPTS[step]) > 50, f"Prompt too short for step: {step}"
        assert len(STEP_SUMMARIES[step]) > 10, f"Summary too short for step: {step}"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        try:
            test()
            print(f"  PASS  {test.__name__}")
        except AssertionError as e:  # noqa: F821
            print(f"  FAIL  {test.__name__}: {e}")
        except Exception as e:
            print(f"  ERROR {test.__name__}: {e}")
