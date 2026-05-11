"""RCO-2a Task 10 — operator transcript renders the contract-health marker.

We test the pure rendering helper in isolation; full-transcript
rendering is covered by existing tests we don't need to retouch.
"""
from __future__ import annotations


def test_render_section_for_healthy() -> None:
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_contract_health_section,
    )
    payload = {
        "optimization_run_id": "run-x",
        "merge_gate_status": "healthy",
        "high_tier_violations": [],
        "medium_tier_violations": [],
        "phase_h_listing_status": "ok",
        "phase_h_validator_status": "ok",
        "bundle_status": "complete",
        "replay_is_valid": True,
        "replay_violation_count": 0,
    }
    md = render_contract_health_section(payload)
    assert "Contract Health" in md
    assert "healthy" in md.lower()
    assert "merge_gate_status: healthy" in md


def test_render_section_for_blocked_lists_high_tier_invariants() -> None:
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_contract_health_section,
    )
    payload = {
        "optimization_run_id": "run-y",
        "merge_gate_status": "merge_gate_blocked",
        "high_tier_violations": [
            {"invariant_id": "I12", "title": "replay_validity_violated"},
        ],
        "medium_tier_violations": [],
        "phase_h_listing_status": "ok",
        "phase_h_validator_status": "ok",
        "bundle_status": "complete",
        "replay_is_valid": False,
        "replay_violation_count": 25,
    }
    md = render_contract_health_section(payload)
    assert "I12" in md
    assert "replay_validity_violated" in md
    assert "merge_gate_blocked" in md


def test_render_section_for_none_payload_returns_placeholder() -> None:
    from genie_space_optimizer.optimization.operator_process_transcript import (
        render_contract_health_section,
    )
    md = render_contract_health_section(None)
    assert "Contract Health" in md
    assert "not emitted" in md.lower()
