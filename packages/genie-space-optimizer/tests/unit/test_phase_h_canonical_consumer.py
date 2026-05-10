"""Cycle 14-W T6 — Phase H drift alarm helpers.

Anchors:
  - airline run 1105451933925748 F8 (D-6: acceptance writer drift)
  - 7Now run 960148942255012 F8 (D-8: journey validator drift)

The pure helpers detect drift between the canonical decision/state
surface and the Phase H writer outputs. Marker constructors (in
``run_analysis_contract.py``) emit ``GSO_PHASE_H_*_DRIFT_V1``
alarms when the helpers report drift.
"""

from __future__ import annotations

import json
import re

from genie_space_optimizer.optimization.run_analysis_contract import (
    detect_phase_h_acceptance_drift,
    detect_phase_h_journey_drift,
    phase_h_acceptance_drift_marker,
    phase_h_journey_drift_marker,
)


# ── Acceptance drift detector ────────────────────────────────────────


def test_acceptance_drift_detected_on_outcome_disagreement() -> None:
    """Anchor 13 (airline) shape: stdout says ACCEPTED, Phase H
    writer says rolled_back."""
    assert detect_phase_h_acceptance_drift(
        canonical_outcome="accepted",
        canonical_reason_code="accepted",
        phase_h_outcome="rolled_back",
        phase_h_reason_code="missing_pre_rows",
    ) is True


def test_acceptance_drift_detected_on_reason_code_disagreement() -> None:
    """Outcome agrees but reason code differs (still drift)."""
    assert detect_phase_h_acceptance_drift(
        canonical_outcome="rolled_back",
        canonical_reason_code="target_resolution_failed",
        phase_h_outcome="rolled_back",
        phase_h_reason_code="missing_pre_rows",
    ) is True


def test_acceptance_drift_silent_on_clean_payload() -> None:
    assert detect_phase_h_acceptance_drift(
        canonical_outcome="accepted",
        canonical_reason_code="accepted",
        phase_h_outcome="accepted",
        phase_h_reason_code="accepted",
    ) is False


def test_acceptance_drift_case_insensitive() -> None:
    """Case differences must NOT register as drift (whitespace and
    case are normalised by the helper)."""
    assert detect_phase_h_acceptance_drift(
        canonical_outcome="ACCEPTED",
        canonical_reason_code="accepted",
        phase_h_outcome="accepted",
        phase_h_reason_code="ACCEPTED",
    ) is False


# ── Journey drift detector ───────────────────────────────────────────


def test_journey_drift_detected_on_count_mismatch() -> None:
    """Anchor 11 (7Now) shape: local replay reports 25 violations,
    Phase H writer reports 0."""
    assert detect_phase_h_journey_drift(
        canonical_violation_count=25,
        phase_h_violation_count=0,
    ) is True


def test_journey_drift_silent_on_match() -> None:
    assert detect_phase_h_journey_drift(
        canonical_violation_count=0,
        phase_h_violation_count=0,
    ) is False


# ── Marker constructors ──────────────────────────────────────────────


def test_acceptance_drift_marker_v1_payload_shape() -> None:
    line = phase_h_acceptance_drift_marker(
        optimization_run_id="run-airline-13",
        iteration=1,
        canonical_outcome="accepted",
        canonical_reason_code="accepted",
        phase_h_outcome="rolled_back",
        phase_h_reason_code="missing_pre_rows",
    )
    assert line.startswith("GSO_PHASE_H_ACCEPTANCE_DRIFT_V1 ")
    body = re.search(r"\s+(\{.*\})", line).group(1)
    payload = json.loads(body)
    assert payload["canonical_outcome"] == "accepted"
    assert payload["phase_h_outcome"] == "rolled_back"
    assert payload["phase_h_reason_code"] == "missing_pre_rows"


def test_journey_drift_marker_v1_payload_shape() -> None:
    line = phase_h_journey_drift_marker(
        optimization_run_id="run-7now-11",
        iteration=1,
        canonical_violation_count=25,
        phase_h_violation_count=0,
    )
    assert line.startswith("GSO_PHASE_H_JOURNEY_DRIFT_V1 ")
    body = re.search(r"\s+(\{.*\})", line).group(1)
    payload = json.loads(body)
    assert payload["canonical_violation_count"] == 25
    assert payload["phase_h_violation_count"] == 0


# ── Flag accessor ────────────────────────────────────────────────────


def test_phase_h_canonical_consumer_default_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PHASE_H_CANONICAL_CONSUMER", raising=False)
    from genie_space_optimizer.common.config import (
        phase_h_canonical_consumer_enabled,
    )
    assert phase_h_canonical_consumer_enabled() is True


def test_phase_h_canonical_consumer_disabled_via_env(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PHASE_H_CANONICAL_CONSUMER", "0")
    from genie_space_optimizer.common.config import (
        phase_h_canonical_consumer_enabled,
    )
    assert phase_h_canonical_consumer_enabled() is False
