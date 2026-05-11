"""RCO-2a Task 6 — contract_health_summary_marker emitter tests."""
from __future__ import annotations

import json


def test_emitter_returns_well_formed_marker_line() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        ContractHealthSummary,
        MergeGateStatus,
    )
    from genie_space_optimizer.optimization.run_analysis_contract import (
        contract_health_summary_marker,
    )
    summary = ContractHealthSummary(
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
    line = contract_health_summary_marker(summary)
    assert line.startswith("GSO_CONTRACT_HEALTH_V1 ")
    payload = json.loads(line[len("GSO_CONTRACT_HEALTH_V1 "):])
    assert payload["optimization_run_id"] == "run-1"
    assert payload["merge_gate_status"] == "healthy"


def test_emitter_payload_is_sorted_for_byte_stability() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        ContractHealthSummary,
        MergeGateStatus,
    )
    from genie_space_optimizer.optimization.run_analysis_contract import (
        contract_health_summary_marker,
    )
    summary = ContractHealthSummary(
        optimization_run_id="run-1",
        merge_gate_status=MergeGateStatus.MERGE_GATE_BLOCKED,
        high_tier_violations=({"invariant_id": "I12"},),
        medium_tier_violations=(),
        phase_h_listing_status="ok",
        phase_h_validator_status="ok",
        bundle_status="complete",
        replay_is_valid=False,
        replay_violation_count=3,
    )
    line1 = contract_health_summary_marker(summary)
    line2 = contract_health_summary_marker(summary)
    assert line1 == line2
