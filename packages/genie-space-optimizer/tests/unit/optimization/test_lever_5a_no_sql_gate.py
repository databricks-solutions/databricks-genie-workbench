"""Edge-case regression tests for _validate_lever_5a_no_sql_output.

The 3-detector gate at optimizer.py:
  1. Forbidden top-level key 'example_sql_proposals' (now a sentinel
     post strict: true typed-IO + additionalProperties: false).
  2. Fenced ```sql code block in instruction_text.
  3. SELECT...FROM... pattern >=40 chars in instruction_text.

Per baseline §6.D1 + §6.F2.
"""
from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    "text, expected_ok, expected_reason_substr",
    [
        # PASS cases — prose that mentions "select" or "from" in English
        (
            "PURPOSE:\nSelect the appropriate fact table from these "
            "options when answering revenue questions.",
            True,
            "",
        ),
        (
            "QUERY RULES:\n- Always filter by date.\n- Group by store, "
            "then by region.",
            True,
            "",
        ),
        ("", True, ""),  # empty pass-through (no-fix sentinel)
        (
            "ASSET ROUTING:\n- For booking_summary questions, route to "
            "the get_booking_summary TVF.",
            True,
            "",
        ),
        # FAIL cases — actual SQL embedded in instructions
        (
            "QUERY PATTERNS:\n```sql\nSELECT cy_sales FROM mv_7now_fact_sales\n```",
            False,
            "fenced SQL",
        ),
        (
            "QUERY PATTERNS:\nUse SELECT cy_sales FROM mv_7now_fact_sales "
            "WHERE same_store_7now = 'Y' GROUP BY zone_combination;",
            False,
            "SELECT",
        ),
        (
            "Example: SELECT customer_id, total_amount, order_date "
            "FROM cat.demo.fact_orders WHERE order_date >= '2025-01-01';",
            False,
            "SELECT",
        ),
    ],
)
def test_validate_lever_5a_no_sql_output_detector_2_and_3(
    text, expected_ok, expected_reason_substr,
):
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    result = {"instruction_text": text, "rationale": "test"}
    ok, reason = _validate_lever_5a_no_sql_output(result)
    assert ok is expected_ok, (
        f"Expected ok={expected_ok} for text={text!r}, got ok={ok} "
        f"with reason={reason!r}"
    )
    if expected_reason_substr:
        assert expected_reason_substr.lower() in reason.lower(), (
            f"Expected reason to contain {expected_reason_substr!r}, "
            f"got {reason!r}"
        )


def test_validate_lever_5a_detector_1_rejects_forbidden_top_level_key():
    """Sentinel test — Detector #1 (top-level example_sql_proposals
    key) is structurally impossible from the LLM side post typed-IO.
    Catches code-path bugs that try to inject the field post-LLM.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    result_with_extra = {
        "instruction_text": "PURPOSE: ok",
        "rationale": "test",
        "example_sql_proposals": [{"foo": "bar"}],
    }
    ok, reason = _validate_lever_5a_no_sql_output(result_with_extra)
    assert ok is False
    assert "example_sql_proposals" in reason


def test_validate_lever_5a_returns_false_for_non_dict():
    """Defensive: non-dict input MUST be rejected."""
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    ok, reason = _validate_lever_5a_no_sql_output(["not", "a", "dict"])
    assert ok is False
    assert "not a dict" in reason.lower()


def test_validate_lever_5a_returns_false_for_non_string_instruction_text():
    """Defensive: instruction_text MUST be a string."""
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    ok, reason = _validate_lever_5a_no_sql_output({
        "instruction_text": 42, "rationale": "t",
    })
    assert ok is False
    assert "not a string" in reason.lower()
