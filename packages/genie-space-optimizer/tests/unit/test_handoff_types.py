"""Tests for HandoffSource enum + HandoffValue dataclass."""
import pytest

from genie_space_optimizer.jobs._handoff import HandoffSource, HandoffValue


def test_handoff_source_has_four_members():
    assert HandoffSource.TASK_VALUES.value == "task_values"
    assert HandoffSource.DELTA_FALLBACK.value == "delta_fallback"
    assert HandoffSource.DEFAULT.value == "default"
    assert HandoffSource.MISSING.value == "missing"


def test_handoff_value_construction_and_str_source():
    hv = HandoffValue(
        key="run_id",
        value="abc123",
        source=HandoffSource.TASK_VALUES,
    )
    assert hv.key == "run_id"
    assert hv.value == "abc123"
    assert hv.source is HandoffSource.TASK_VALUES
    # Default optional field
    assert hv.delta_query is None


def test_handoff_value_carries_optional_delta_query_for_audit():
    hv = HandoffValue(
        key="overall_accuracy",
        value=72.5,
        source=HandoffSource.DELTA_FALLBACK,
        delta_query="SELECT overall_accuracy FROM genie_opt_iterations WHERE ...",
    )
    assert hv.source is HandoffSource.DELTA_FALLBACK
    assert hv.delta_query is not None
    assert "genie_opt_iterations" in hv.delta_query


def test_handoff_value_is_immutable():
    hv = HandoffValue(key="k", value=1, source=HandoffSource.TASK_VALUES)
    with pytest.raises(Exception):
        hv.value = 2  # type: ignore[misc]
