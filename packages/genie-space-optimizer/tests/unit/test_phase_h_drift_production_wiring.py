"""Cycle 14-W hardening — D-6 + D-8 production wiring.

Anchors:
  D-6: airline run 1105451933925748 F8 — stdout iter-1 ACCEPTED
       vs acceptance_decision/output.json says
       outcome=rolled_back, reason_code=missing_pre_rows.
  D-8: 7Now run 960148942255012 F8 — local replay reports 25
       journey violations; Phase H journey_validation_all.json
       reports 0.
"""
from __future__ import annotations

import io
import json
import re
from contextlib import redirect_stdout


def _extract_payload(stdout_text: str, marker: str) -> dict | None:
    match = re.search(rf"{marker}\s+(\{{.*?\}})", stdout_text)
    if match is None:
        return None
    return json.loads(match.group(1))


def test_acceptance_drift_alarm_fires_on_airline_iter1_shape(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PHASE_H_DRIFT_OBSERVE", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_phase_h_acceptance_drift_if_any,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_phase_h_acceptance_drift_if_any(
            run_id="airline_anchor_13",
            iteration=1,
            canonical_outcome="accepted",
            canonical_reason_code="accepted_with_attribution_drift",
            phase_h_outcome="rolled_back",
            phase_h_reason_code="missing_pre_rows",
        )
    payload = _extract_payload(buf.getvalue(), "GSO_PHASE_H_ACCEPTANCE_DRIFT_V1")
    assert payload is not None
    assert payload["canonical_outcome"] == "accepted"
    assert payload["phase_h_outcome"] == "rolled_back"


def test_acceptance_drift_silent_on_canonical_match(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PHASE_H_DRIFT_OBSERVE", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_phase_h_acceptance_drift_if_any,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_phase_h_acceptance_drift_if_any(
            run_id="clean_run",
            iteration=1,
            canonical_outcome="accepted",
            canonical_reason_code="accepted",
            phase_h_outcome="accepted",
            phase_h_reason_code="accepted",
        )
    assert "GSO_PHASE_H_ACCEPTANCE_DRIFT_V1" not in buf.getvalue()


def test_acceptance_drift_disabled_when_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PHASE_H_DRIFT_OBSERVE", "0")
    from genie_space_optimizer.optimization.harness import (
        _emit_phase_h_acceptance_drift_if_any,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_phase_h_acceptance_drift_if_any(
            run_id="airline_anchor_13",
            iteration=1,
            canonical_outcome="accepted",
            canonical_reason_code="accepted_with_attribution_drift",
            phase_h_outcome="rolled_back",
            phase_h_reason_code="missing_pre_rows",
        )
    assert "GSO_PHASE_H_ACCEPTANCE_DRIFT_V1" not in buf.getvalue()


def test_journey_drift_alarm_fires_on_7now_anchor_shape(monkeypatch) -> None:
    """T5 lives in Task 5 but its unit test is co-located here for
    closure-protocol cleanliness — both alarms share the
    GSO_PHASE_H_DRIFT_OBSERVE flag."""
    monkeypatch.setenv("GSO_PHASE_H_DRIFT_OBSERVE", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_phase_h_journey_drift_if_any,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_phase_h_journey_drift_if_any(
            run_id="7now_anchor_11",
            iteration=1,
            canonical_violation_count=25,
            phase_h_violation_count=0,
        )
    payload = _extract_payload(buf.getvalue(), "GSO_PHASE_H_JOURNEY_DRIFT_V1")
    assert payload is not None
    assert payload["canonical_violation_count"] == 25
    assert payload["phase_h_violation_count"] == 0


def test_journey_drift_silent_on_canonical_match(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PHASE_H_DRIFT_OBSERVE", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_phase_h_journey_drift_if_any,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_phase_h_journey_drift_if_any(
            run_id="clean_run", iteration=1,
            canonical_violation_count=0, phase_h_violation_count=0,
        )
    assert "GSO_PHASE_H_JOURNEY_DRIFT_V1" not in buf.getvalue()
