"""Phase 5 synthetic gate: a clean synthetic run emits a HEALTHY
contract-health summary with no invariant violations.

Maps to user-text spec ``test_contract_health_clean``: after a synthetic
run with all Phase 1-3 changes active, contract_health emits
``merge_gate_status=healthy``, no ``I_CHECK_FAILED`` records, and no
``directive_truthfulness`` violations.

Implementation note — deviation from Task 14's literal Python template:
The task plan in
``docs/final_plan/2026-05-14-final-closeout-phase-5-6-offline-gates.md``
prescribed an import path
(``genie_space_optimizer.contract_health.summary``) and call signature
(``build_contract_health_summary(markers=..., iteration_count=...)``)
that do not match the production RCO-2a surface area. The real builder
lives at ``genie_space_optimizer.optimization.contract_health`` and
consumes already-extracted evidence (invariant violations, Phase H
strict-validation payload, bundle-completeness markers, replay
validation), not a raw marker stream. The result's category lives on
``merge_gate_status: MergeGateStatus`` (HEALTHY / WARN /
MERGE_GATE_BLOCKED), not on a ``bundle_status`` string. The marker
emitted by ``contract_health_summary_marker`` is named
``GSO_CONTRACT_HEALTH_V1`` — ``GSO_CONTRACT_HEALTH_SUMMARY_V1`` is the
default-on emission *flag* (see
``common/config.py::contract_health_summary_v1_enabled``), not the
marker name.

This test exercises the real RCO-2a builder and end-to-end marker
emitter against a synthetic clean-run evidence bundle and asserts the
three properties the user-text gate requires:

1. ``merge_gate_status == "healthy"``
2. zero ``I_CHECK_FAILED`` records in the violations buckets
3. zero ``directive_truthfulness`` violations in the violations buckets

It also pins the end-to-end marker emitter so the contract-health
summary continues to surface a stdout marker line that downstream
parsers (``mlflow_markers.parse_markers``) can consume.

Note on Plan E v1.2 markers (spec §9.5): a clean run is NOT required
to emit any of the five conditional v1.2 markers
(``GSO_BOUNDED_DEBT_VIOLATION_V1``, ``GSO_RETROACTIVE_ROLLBACK_V1``,
``GSO_STRATEGIST_INPUT_LOW_REPAIRABILITY_V1``,
``GSO_LOOP_TERMINATED_NO_VIABLE_WORK_V1``,
``GSO_GT_CORRECTION_FILTER_APPLIED_V1``) — they fire only when the
corresponding anomalous code path runs. The contract-health
verdict's HEALTHY result must NOT require them.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.contract_health import (
    MergeGateStatus,
    build_contract_health_summary,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    contract_health_summary_marker,
)


def _clean_synthetic_evidence() -> dict:
    """Synthetic evidence bundle representing a successful 1-iteration
    run with all Phase 1-3 capabilities active and no invariant
    violations.

    Equivalent (in evidence terms) to the marker stream described in
    Task 14: Phase H strict validation OK, no invariant violations, a
    complete bundle, and a valid replay.
    """
    return {
        "optimization_run_id": "synthetic-clean-run-1",
        "invariant_violations": [],
        "phase_h_strict_validation": {
            "listing_status": "ok",
            "validator_status": "ok",
        },
        "bundle_assembly_failed": (),
        "bundle_assembly_incomplete": None,
        "replay_validation": {"is_valid": True, "violation_count": 0},
    }


def test_contract_health_clean_run_is_healthy() -> None:
    summary = build_contract_health_summary(**_clean_synthetic_evidence())

    assert summary.merge_gate_status is MergeGateStatus.HEALTHY
    assert summary.merge_gate_status.value == "healthy"
    assert summary.high_tier_violations == ()
    assert summary.medium_tier_violations == ()


def test_contract_health_no_i_check_failed_records() -> None:
    """Clean synthetic run carries zero ``I_CHECK_FAILED`` sentinel
    records in either severity bucket.
    """
    summary = build_contract_health_summary(**_clean_synthetic_evidence())

    i_check_failed = [
        v
        for v in (*summary.high_tier_violations, *summary.medium_tier_violations)
        if str(v.get("invariant_id") or "") == "I_CHECK_FAILED"
    ]
    assert i_check_failed == []


def test_contract_health_directive_truthfulness_clean() -> None:
    """Clean synthetic run carries no ``directive_truthfulness``
    violations (the directive-outcome invariant family).
    """
    summary = build_contract_health_summary(**_clean_synthetic_evidence())

    directive_violations = [
        v
        for v in (*summary.high_tier_violations, *summary.medium_tier_violations)
        if "directive" in str(v.get("title") or "").lower()
        or "directive" in str(v.get("invariant_id") or "").lower()
    ]
    assert directive_violations == []


def test_contract_health_marker_emitted_in_stream() -> None:
    """End-to-end: the marker emitter produces a
    ``GSO_CONTRACT_HEALTH_V1`` stdout line carrying the HEALTHY verdict,
    parseable as JSON (per RCO-2a wiring).
    """
    summary = build_contract_health_summary(**_clean_synthetic_evidence())
    line = contract_health_summary_marker(summary)

    prefix = "GSO_CONTRACT_HEALTH_V1 "
    assert line.startswith(prefix)
    payload = json.loads(line[len(prefix):])
    assert payload["merge_gate_status"] == "healthy"
    assert payload["high_tier_violations"] == []
    assert payload["medium_tier_violations"] == []
