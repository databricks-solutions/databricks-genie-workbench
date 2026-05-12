from __future__ import annotations

from genie_space_optimizer.tools.propagation_diagnostic import (
    PROPAGATION_OUTCOMES,
    classify_outcome,
    locate_query_rules_instruction,
)


def test_propagation_outcomes_enum_values() -> None:
    assert set(PROPAGATION_OUTCOMES) == {
        "propagation_lag",
        "instruction_not_scoped_to_qid",
        "instruction_insufficient_force",
        "eval_cache_stale",
    }


def test_classify_outcome_accepts_known_values() -> None:
    for value in PROPAGATION_OUTCOMES:
        assert classify_outcome(value) == value


def test_classify_outcome_rejects_unknown_values() -> None:
    import pytest
    with pytest.raises(ValueError) as exc:
        classify_outcome("totally_made_up")
    assert "must be one of" in str(exc.value)


def test_locate_query_rules_instruction_returns_text_when_present() -> None:
    serialized_space = {
        "instructions": {
            "text_instructions": [
                {
                    "title": "QUERY RULES",
                    "content": "## QUERY RULES\nFor plural top-N questions, ORDER BY ... LIMIT ...",
                },
            ],
        },
    }
    found = locate_query_rules_instruction(serialized_space)
    assert found is not None
    assert "plural top-N" in found


def test_locate_query_rules_instruction_returns_none_when_absent() -> None:
    serialized_space = {"instructions": {"text_instructions": []}}
    assert locate_query_rules_instruction(serialized_space) is None


def test_locate_query_rules_instruction_handles_missing_keys() -> None:
    assert locate_query_rules_instruction({}) is None
    assert locate_query_rules_instruction({"instructions": {}}) is None
