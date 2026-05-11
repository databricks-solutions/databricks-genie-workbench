"""RCO-2a Task 5 — build_contract_health_summary tests.

The builder consumes four evidence inputs:
  1. invariant_violations: list of dicts (from invariants.run_invariants)
  2. phase_h_strict_validation: payload dict (from GSO_PHASE_H_STRICT_VALIDATION_V1) or None
  3. bundle_assembly_failed: tuple of dicts (from GSO_BUNDLE_ASSEMBLY_FAILED_V1)
  4. bundle_assembly_incomplete: tuple of dicts or None (from GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1)
  5. replay_validation: dict with is_valid / violation_count

The result's merge_gate_status is computed deterministically.
"""
from __future__ import annotations


def test_all_green_returns_healthy() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateStatus,
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-green",
        invariant_violations=[],
        phase_h_strict_validation={
            "listing_status": "ok", "validator_status": "ok",
        },
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation={"is_valid": True, "violation_count": 0},
    )
    assert s.merge_gate_status is MergeGateStatus.HEALTHY
    assert s.high_tier_violations == ()
    assert s.medium_tier_violations == ()


def test_medium_tier_only_returns_warn() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateStatus,
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-warn",
        invariant_violations=[
            {"invariant_id": "I3", "title": "acceptance_bucket_mismatch"},
            {"invariant_id": "I7", "title": "rca_grounding_missing"},
        ],
        phase_h_strict_validation={
            "listing_status": "ok", "validator_status": "ok",
        },
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation={"is_valid": True, "violation_count": 0},
    )
    assert s.merge_gate_status is MergeGateStatus.WARN
    assert len(s.medium_tier_violations) == 2
    assert s.high_tier_violations == ()


def test_high_tier_violation_returns_blocked() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateStatus,
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-blocked",
        invariant_violations=[
            {"invariant_id": "I12", "title": "replay_validity_violated"},
        ],
        phase_h_strict_validation={
            "listing_status": "ok", "validator_status": "ok",
        },
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation={"is_valid": False, "violation_count": 25},
    )
    assert s.merge_gate_status is MergeGateStatus.MERGE_GATE_BLOCKED
    assert len(s.high_tier_violations) == 1
    assert s.high_tier_violations[0]["invariant_id"] == "I12"


def test_phase_h_failed_returns_blocked_even_with_no_invariants() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateStatus,
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-h-failed",
        invariant_violations=[],
        phase_h_strict_validation={
            "listing_status": "failed", "validator_status": "skipped",
        },
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation={"is_valid": True, "violation_count": 0},
    )
    assert s.merge_gate_status is MergeGateStatus.MERGE_GATE_BLOCKED
    assert s.phase_h_listing_status == "failed"


def test_phase_h_skipped_returns_warn() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateStatus,
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-h-skipped",
        invariant_violations=[],
        phase_h_strict_validation={
            "listing_status": "skipped", "validator_status": "skipped",
        },
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation={"is_valid": True, "violation_count": 0},
    )
    assert s.merge_gate_status is MergeGateStatus.WARN


def test_bundle_assembly_failed_returns_blocked() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateStatus,
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-bundle-failed",
        invariant_violations=[],
        phase_h_strict_validation={
            "listing_status": "ok", "validator_status": "ok",
        },
        bundle_assembly_failed=({"error_type": "S3WriteError"},),
        bundle_assembly_incomplete=None,
        replay_validation={"is_valid": True, "violation_count": 0},
    )
    assert s.merge_gate_status is MergeGateStatus.MERGE_GATE_BLOCKED
    assert s.bundle_status == "assembly_failed"


def test_bundle_assembly_incomplete_returns_warn() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateStatus,
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-bundle-incomplete",
        invariant_violations=[],
        phase_h_strict_validation={
            "listing_status": "ok", "validator_status": "ok",
        },
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=({"missing_count": 1},),
        replay_validation={"is_valid": True, "violation_count": 0},
    )
    assert s.merge_gate_status is MergeGateStatus.WARN
    assert s.bundle_status == "incomplete"


def test_missing_phase_h_payload_means_skipped() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-no-phase-h",
        invariant_violations=[],
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation={"is_valid": True, "violation_count": 0},
    )
    assert s.phase_h_listing_status == "skipped"
    assert s.phase_h_validator_status == "skipped"


def test_blocked_dominates_warn() -> None:
    """If any signal is blocking, status is blocked even when warn signals also present."""
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateStatus,
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-mixed",
        invariant_violations=[
            {"invariant_id": "I3", "title": "warn"},  # MEDIUM
            {"invariant_id": "I12", "title": "blocked"},  # HIGH
        ],
        phase_h_strict_validation={
            "listing_status": "skipped", "validator_status": "skipped",
        },
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=({"missing_count": 1},),
        replay_validation={"is_valid": False, "violation_count": 5},
    )
    assert s.merge_gate_status is MergeGateStatus.MERGE_GATE_BLOCKED


def test_replay_validation_fields_are_surfaced() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    s = build_contract_health_summary(
        optimization_run_id="run-replay",
        invariant_violations=[],
        phase_h_strict_validation={"listing_status": "ok", "validator_status": "ok"},
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation={"is_valid": False, "violation_count": 7},
    )
    assert s.replay_is_valid is False
    assert s.replay_violation_count == 7
