"""RCO-2a Task 4 — ContractHealthSummary dataclass tests."""
from __future__ import annotations

import json


def test_summary_is_frozen() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        ContractHealthSummary,
        MergeGateStatus,
    )
    s = ContractHealthSummary(
        optimization_run_id="run-1",
        merge_gate_status=MergeGateStatus.HEALTHY,
        high_tier_violations=(),
        medium_tier_violations=(),
        phase_h_listing_status="ok",
        phase_h_validator_status="ok",
        bundle_status="complete",
        replay_is_valid=True,
        replay_violation_count=0,
    )
    import pytest
    with pytest.raises((AttributeError, TypeError)):
        s.optimization_run_id = "run-2"  # type: ignore[misc]


def test_summary_roundtrips_through_json() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        ContractHealthSummary,
        MergeGateStatus,
    )
    s = ContractHealthSummary(
        optimization_run_id="abc-123",
        merge_gate_status=MergeGateStatus.WARN,
        high_tier_violations=(),
        medium_tier_violations=({"invariant_id": "I3", "title": "x"},),
        phase_h_listing_status="skipped",
        phase_h_validator_status="skipped",
        bundle_status="incomplete",
        replay_is_valid=True,
        replay_violation_count=0,
    )
    blob = s.to_json_dict()
    restored = ContractHealthSummary.from_json_dict(blob)
    assert restored == s
    assert json.loads(json.dumps(blob)) == blob


def test_summary_to_json_dict_uses_enum_value_string() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        ContractHealthSummary,
        MergeGateStatus,
    )
    s = ContractHealthSummary(
        optimization_run_id="abc-123",
        merge_gate_status=MergeGateStatus.MERGE_GATE_BLOCKED,
        high_tier_violations=({"invariant_id": "I12", "title": "replay"},),
        medium_tier_violations=(),
        phase_h_listing_status="failed",
        phase_h_validator_status="failed",
        bundle_status="assembly_failed",
        replay_is_valid=False,
        replay_violation_count=25,
    )
    blob = s.to_json_dict()
    assert blob["merge_gate_status"] == "merge_gate_blocked"
