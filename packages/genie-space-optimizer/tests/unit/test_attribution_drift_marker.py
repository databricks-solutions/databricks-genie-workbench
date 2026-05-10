"""Cycle 14-C T5 — GSO_ATTRIBUTION_DRIFT_V1 typed marker.

Anchor: airline run 1105451933925748 iter 1 —
reason_code=accepted_with_attribution_drift; the marker fires
once with the reattribution payload."""
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


def test_attribution_drift_marker_constructor_canonical_payload() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        attribution_drift_marker,
    )
    line = attribution_drift_marker(
        optimization_run_id="airline_anchor_13",
        iteration=1,
        ag_id="AG_DECOMPOSED_H004",
        baseline_accuracy=83.3,
        candidate_accuracy=95.8,
        delta_pp=12.5,
        target_qids=("gs_024",),
        accidentally_improved_qids=("gs_007", "gs_009", "gs_013"),
        unresolved_target_debt_qids=("gs_024",),
    )
    assert line.startswith("GSO_ATTRIBUTION_DRIFT_V1 ")
    payload = json.loads(re.search(r"\s+(\{.*\})", line).group(1))
    assert payload["iteration"] == 1
    assert payload["ag_id"] == "AG_DECOMPOSED_H004"
    assert payload["accidentally_improved_qids"] == ["gs_007", "gs_009", "gs_013"]
    assert payload["unresolved_target_debt_qids"] == ["gs_024"]
    assert payload["target_qids"] == ["gs_024"]
    assert payload["baseline_accuracy"] == 83.3
    assert payload["candidate_accuracy"] == 95.8


def test_marker_parser_populates_attribution_drift_slot() -> None:
    from genie_space_optimizer.tools.marker_parser import parse_markers
    text = (
        'GSO_ATTRIBUTION_DRIFT_V1 {"optimization_run_id":"x","iteration":1,'
        '"ag_id":"AG1","baseline_accuracy":80.0,"candidate_accuracy":85.0,'
        '"delta_pp":5.0,"target_qids":["gs_024"],'
        '"accidentally_improved_qids":["gs_007"],'
        '"unresolved_target_debt_qids":["gs_024"]}\n'
    )
    log = parse_markers(text)
    assert len(log.attribution_drift) == 1
    assert log.attribution_drift[0]["accidentally_improved_qids"] == ["gs_007"]


def test_harness_emits_marker_when_branch_fires(monkeypatch) -> None:
    """When the harness's _maybe_emit_attribution_drift_marker is
    called with a drift decision and the observability flag is on,
    the marker fires."""
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_REATTRIBUTION", "1")
    from genie_space_optimizer.optimization.control_plane import (
        ControlPlaneAcceptance,
    )
    from genie_space_optimizer.optimization.harness import (
        _maybe_emit_attribution_drift_marker,
    )
    decision = ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted_with_attribution_drift",
        baseline_accuracy=83.3, candidate_accuracy=95.8, delta_pp=12.5,
        target_qids=("gs_024",), target_fixed_qids=(),
        target_still_hard_qids=("gs_024",),
        out_of_target_regressed_qids=(),
        accidentally_improved_qids=("gs_007", "gs_009"),
        unresolved_target_debt_qids=("gs_024",),
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _maybe_emit_attribution_drift_marker(
            run_id="airline_anchor_13",
            iteration=1,
            ag_id="AG_DECOMPOSED_H004",
            decision=decision,
        )
    payload = _extract_payload(buf.getvalue(), "GSO_ATTRIBUTION_DRIFT_V1")
    assert payload is not None
    assert payload["accidentally_improved_qids"] == ["gs_007", "gs_009"]


def test_harness_marker_silent_on_non_drift_branch(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_REATTRIBUTION", "1")
    from genie_space_optimizer.optimization.control_plane import (
        ControlPlaneAcceptance,
    )
    from genie_space_optimizer.optimization.harness import (
        _maybe_emit_attribution_drift_marker,
    )
    decision = ControlPlaneAcceptance(
        accepted=True, reason_code="accepted",
        baseline_accuracy=80.0, candidate_accuracy=85.0, delta_pp=5.0,
        target_qids=("gs_001",), target_fixed_qids=("gs_001",),
        target_still_hard_qids=(), out_of_target_regressed_qids=(),
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _maybe_emit_attribution_drift_marker(
            run_id="x", iteration=1, ag_id="AG1", decision=decision,
        )
    assert "GSO_ATTRIBUTION_DRIFT_V1" not in buf.getvalue()


def test_harness_marker_silent_when_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_REATTRIBUTION", "0")
    from genie_space_optimizer.optimization.control_plane import (
        ControlPlaneAcceptance,
    )
    from genie_space_optimizer.optimization.harness import (
        _maybe_emit_attribution_drift_marker,
    )
    decision = ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted_with_attribution_drift",
        baseline_accuracy=83.3, candidate_accuracy=95.8, delta_pp=12.5,
        target_qids=("gs_024",), target_fixed_qids=(),
        target_still_hard_qids=("gs_024",),
        out_of_target_regressed_qids=(),
        accidentally_improved_qids=("gs_007",),
        unresolved_target_debt_qids=("gs_024",),
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _maybe_emit_attribution_drift_marker(
            run_id="x", iteration=1, ag_id="AG1", decision=decision,
        )
    assert "GSO_ATTRIBUTION_DRIFT_V1" not in buf.getvalue()
