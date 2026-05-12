"""RCO-2b — enforce_merge_gate pure-helper behavior."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.contract_health import (
    MergeGateBlockedError,
    enforce_merge_gate,
)


def test_healthy_status_does_not_raise() -> None:
    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "healthy",
            "high_tier_violations": [],
            "medium_tier_violations": [],
            "optimization_run_id": "run-healthy",
            "phase_h_listing_status": "ok",
            "phase_h_validator_status": "ok",
            "bundle_status": "complete",
            "replay_is_valid": True,
            "replay_violation_count": 0,
        },
    }
    enforce_merge_gate(loop_out)  # no raise


def test_warn_status_does_not_raise() -> None:
    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "warn",
            "high_tier_violations": [],
            "medium_tier_violations": [
                {"invariant_id": "I3", "title": "stale_evidence"},
            ],
            "optimization_run_id": "run-warn",
            "phase_h_listing_status": "skipped",
            "phase_h_validator_status": "skipped",
            "bundle_status": "complete",
            "replay_is_valid": True,
            "replay_violation_count": 0,
        },
    }
    enforce_merge_gate(loop_out)  # no raise


def test_blocked_status_raises_with_payload() -> None:
    loop_out = {
        "contract_health_summary": {
            "merge_gate_status": "merge_gate_blocked",
            "high_tier_violations": [
                {"invariant_id": "I12", "title": "replay_validity_violated"},
                {"invariant_id": "I12", "title": "replay_validity_violated"},
            ],
            "medium_tier_violations": [],
            "optimization_run_id": "run-blocked",
            "phase_h_listing_status": "ok",
            "phase_h_validator_status": "ok",
            "bundle_status": "complete",
            "replay_is_valid": False,
            "replay_violation_count": 25,
        },
    }
    with pytest.raises(MergeGateBlockedError) as excinfo:
        enforce_merge_gate(loop_out)
    err = excinfo.value
    assert err.merge_gate_status == "merge_gate_blocked"
    assert err.high_tier_violation_count == 2
    assert err.optimization_run_id == "run-blocked"


def test_missing_contract_health_does_not_raise() -> None:
    """Defensive: if the harness path that builds the summary failed
    silently (e.g. RCO-2a's try/except swallowed an exception), the
    notebook MUST continue. RCO-2b only blocks on a known-blocked
    payload; absence is treated as ``warn`` upstream and not enforced
    here."""
    enforce_merge_gate({})
    enforce_merge_gate({"contract_health_summary": None})
    enforce_merge_gate({"contract_health_summary": {}})
