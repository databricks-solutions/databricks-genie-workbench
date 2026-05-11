"""RCO-2a Task 7 — marker_parser tests for GSO_CONTRACT_HEALTH_V1."""
from __future__ import annotations

import json


def test_parser_captures_contract_health_payload() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers
    payload = {
        "optimization_run_id": "run-1",
        "merge_gate_status": "warn",
        "high_tier_violations": [],
        "medium_tier_violations": [{"invariant_id": "I3"}],
        "phase_h_listing_status": "ok",
        "phase_h_validator_status": "ok",
        "bundle_status": "complete",
        "replay_is_valid": True,
        "replay_violation_count": 0,
    }
    stdout = f"GSO_CONTRACT_HEALTH_V1 {json.dumps(payload, sort_keys=True)}"
    log = parse_markers(stdout)
    assert log.contract_health == payload


def test_contract_health_defaults_to_none_when_marker_absent() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers
    log = parse_markers("GSO_RUN_MANIFEST_V1 {\"optimization_run_id\":\"x\"}")
    assert log.contract_health is None


def test_last_emitted_contract_health_wins() -> None:
    """If multiple GSO_CONTRACT_HEALTH_V1 lines appear, the most recent
    one wins (matches the phase_h_strict_validation convention)."""
    from genie_space_optimizer.tools.marker_parser import parse_markers
    p1 = {"optimization_run_id": "r", "merge_gate_status": "warn"}
    p2 = {"optimization_run_id": "r", "merge_gate_status": "healthy"}
    stdout = "\n".join([
        f"GSO_CONTRACT_HEALTH_V1 {json.dumps(p1, sort_keys=True)}",
        f"GSO_CONTRACT_HEALTH_V1 {json.dumps(p2, sort_keys=True)}",
    ])
    log = parse_markers(stdout)
    assert log.contract_health is not None
    assert log.contract_health["merge_gate_status"] == "healthy"
