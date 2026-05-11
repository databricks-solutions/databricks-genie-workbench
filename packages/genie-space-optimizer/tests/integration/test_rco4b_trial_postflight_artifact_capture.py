"""RCO-4b consolidating-trial postflight — validates the captured
trial-run evidence against ``expected_outcomes.json``.

Skip-by-default: the test runs only when the operator points it at a
captured evidence-bundle directory via the
``RCO4B_TRIAL_EVIDENCE_DIR`` and ``RCO4B_TRIAL_ANCHOR_NAME`` env
vars.

Expected workflow (Task 7 / Task 8 of the consolidating-trial plan):

  export RCO4B_TRIAL_EVIDENCE_DIR=packages/genie-space-optimizer/docs/runid_analysis/<opt_run_id>/evidence
  export RCO4B_TRIAL_ANCHOR_NAME=f9_3b050ec5  # or airline_clean
  uv run pytest tests/integration/test_rco4b_trial_postflight_artifact_capture.py -v

The directory layout the test expects matches what the
``evidence-bundle`` CLI writes (see
``tools.evidence_layout.bundle_paths_for``).
"""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any, Mapping

import pytest

from genie_space_optimizer.optimization.contract_health import (
    ContractHealthSummary,
    MergeGateStatus,
)
from genie_space_optimizer.tools.marker_parser import parse_markers

EXPECTED_OUTCOMES_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "rco4b_trial"
    / "expected_outcomes.json"
)


def _evidence_dir() -> pathlib.Path | None:
    raw = os.environ.get("RCO4B_TRIAL_EVIDENCE_DIR")
    if not raw:
        return None
    p = pathlib.Path(raw)
    return p if p.is_dir() else None


def _anchor_name() -> str | None:
    return os.environ.get("RCO4B_TRIAL_ANCHOR_NAME") or None


@pytest.fixture(scope="module")
def evidence_dir() -> pathlib.Path:
    p = _evidence_dir()
    if p is None:
        pytest.skip(
            "RCO4B_TRIAL_EVIDENCE_DIR not set or directory missing — "
            "Task 7 has not produced evidence yet"
        )
    return p


@pytest.fixture(scope="module")
def anchor_name() -> str:
    name = _anchor_name()
    if name is None:
        pytest.skip("RCO4B_TRIAL_ANCHOR_NAME not set")
    return name


@pytest.fixture(scope="module")
def expected_outcome(anchor_name) -> Mapping[str, Any]:
    payload = json.loads(EXPECTED_OUTCOMES_PATH.read_text())
    anchors = payload.get("anchors") or {}
    if anchor_name not in anchors:
        pytest.fail(
            f"anchor {anchor_name!r} not in expected_outcomes.json — "
            f"valid anchors: {sorted(anchors)}"
        )
    return anchors[anchor_name]


@pytest.fixture(scope="module")
def stdout_text(evidence_dir) -> str:
    candidates = sorted(evidence_dir.glob("stdout*.txt"))
    if not candidates:
        pytest.fail(
            f"no stdout*.txt under {evidence_dir} — evidence-bundle "
            f"did not capture the lever-loop stdout"
        )
    return candidates[0].read_text()


@pytest.fixture(scope="module")
def marker_log(stdout_text):
    return parse_markers(stdout_text)


def test_contract_health_marker_is_present(marker_log):
    assert marker_log.contract_health is not None, (
        "GSO_CONTRACT_HEALTH_V1 marker absent from captured stdout — "
        "RCO-2a end-of-run emission did not fire; trial does not "
        "satisfy the RCO-2b named blocker"
    )


def test_contract_health_payload_roundtrips(marker_log):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    assert summary.optimization_run_id, (
        "ContractHealthSummary missing optimization_run_id"
    )


def test_merge_gate_status_matches_expected(marker_log, expected_outcome):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    expected = MergeGateStatus(
        str(expected_outcome["expected_merge_gate_status"])
    )
    assert summary.merge_gate_status == expected, (
        f"merge_gate_status mismatch: got "
        f"{summary.merge_gate_status.value}, expected {expected.value}"
    )


def test_high_tier_invariants_match_expected(marker_log, expected_outcome):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    expected_ids = set(
        expected_outcome.get("expected_high_tier_invariant_ids") or []
    )
    min_count = int(
        expected_outcome.get("expected_high_tier_min_count") or 0
    )
    actual_ids = {
        str(v.get("invariant_id") or "")
        for v in summary.high_tier_violations
    }
    if expected_ids:
        missing = expected_ids - actual_ids
        assert not missing, (
            f"expected HIGH-tier invariant IDs {sorted(missing)} not "
            f"emitted; got {sorted(actual_ids)}"
        )
    assert len(summary.high_tier_violations) >= min_count, (
        f"too few HIGH-tier violations: got "
        f"{len(summary.high_tier_violations)}, expected ≥ {min_count}"
    )


def test_bundle_status_matches_expected(marker_log, expected_outcome):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    allowed = set(expected_outcome["expected_bundle_status_in"])
    assert summary.bundle_status in allowed, (
        f"bundle_status {summary.bundle_status!r} not in allowed "
        f"set {sorted(allowed)} for anchor"
    )


def test_run_manifest_and_convergence_markers_present(marker_log):
    assert marker_log.run_manifest is not None, (
        "GSO_RUN_MANIFEST_V1 missing — Phase B / end-of-run emission "
        "broken"
    )
    assert marker_log.convergence is not None, (
        "GSO_CONVERGENCE_V1 missing — lever-loop did not finalize"
    )


def test_run_gate_checks_audit_sequence(stdout_text):
    """The six new gate-stage sentinels must fire in the same order
    pinned by ``tests/unit/test_rco4b_run_gate_checks_sequence_guard.py``.

    Encoded as substring positions in stdout. We assert relative
    ordering (not absolute counts) because the trial may run multiple
    iterations.
    """
    sentinels = (
        'gate_name="propagation_wait"',
        'gate_name="slice_gate"',
        'gate_name="p0_gate"',
        'gate_name="asi_extraction"',
        'gate_name="baseline_drift_diagnostic"',
        'gate_name="full_eval_acceptance"',
    )
    seen_at = [
        stdout_text.find(s)
        for s in sentinels
    ]
    for sentinel, pos in zip(sentinels, seen_at):
        assert pos >= 0, (
            f"sentinel {sentinel!r} not found in captured stdout — "
            f"gate-stage extraction did not fire"
        )
    for prev_idx in range(len(seen_at) - 1):
        assert seen_at[prev_idx] < seen_at[prev_idx + 1], (
            f"sentinel order violated: {sentinels[prev_idx]!r} "
            f"appears after {sentinels[prev_idx + 1]!r}"
        )


def test_replay_violation_count_within_expected(
    marker_log, expected_outcome
):
    summary = ContractHealthSummary.from_json_dict(
        marker_log.contract_health
    )
    min_count = expected_outcome.get("expected_replay_violation_min_count")
    max_count = expected_outcome.get("expected_replay_violation_max_count")
    if min_count is not None:
        assert summary.replay_violation_count >= int(min_count), (
            f"replay violation count too low: "
            f"{summary.replay_violation_count} < {min_count}"
        )
    if max_count is not None:
        assert summary.replay_violation_count <= int(max_count), (
            f"replay violation count too high: "
            f"{summary.replay_violation_count} > {max_count}"
        )
