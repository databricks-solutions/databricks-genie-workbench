"""RCO-2a Task 3 — MergeGateStatus enum tests."""
from __future__ import annotations


def test_enum_has_exactly_three_members() -> None:
    from genie_space_optimizer.optimization.contract_health import MergeGateStatus
    assert {m.name for m in MergeGateStatus} == {
        "HEALTHY", "WARN", "MERGE_GATE_BLOCKED",
    }


def test_enum_member_values_are_lowercase_strings() -> None:
    from genie_space_optimizer.optimization.contract_health import MergeGateStatus
    assert MergeGateStatus.HEALTHY.value == "healthy"
    assert MergeGateStatus.WARN.value == "warn"
    assert MergeGateStatus.MERGE_GATE_BLOCKED.value == "merge_gate_blocked"


def test_enum_is_string_serializable() -> None:
    from genie_space_optimizer.optimization.contract_health import MergeGateStatus
    assert str(MergeGateStatus.HEALTHY.value) == "healthy"
