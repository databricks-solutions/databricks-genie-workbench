"""RCO-4b Phase D Task 8 — production-shape fixture tests for both
ASI extraction and baseline-drift diagnostic helpers.
"""
from __future__ import annotations

import json
import pathlib
from dataclasses import dataclass

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    build_baseline_drift_diagnostic,
    forward_asi_extraction_audit,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    AsiExtractionInput,
    BaselineDriftDiagnosticInput,
)


FIXTURE_ROOT = pathlib.Path(__file__).parent / "fixtures" / "rco4b"


def _list_dirs(sub: str) -> list[pathlib.Path]:
    root = FIXTURE_ROOT / sub
    return sorted(p for p in root.iterdir() if p.is_dir())


def _asi_outcome_to_dict(o) -> dict:
    return {
        "should_emit": o.should_emit,
        "stage_letter": o.stage_letter,
        "gate_name": o.gate_name,
        "decision": o.decision,
        "reason_code": o.reason_code,
        "metrics": dict(o.metrics) if o.metrics else o.metrics,
    }


def _drift_outcome_to_dict(o) -> dict:
    return {
        "triggered": o.triggered,
        "delta_pp": o.delta_pp,
        "audit_metrics": dict(o.audit_metrics),
        "reason_code": o.reason_code,
        "log_line": o.log_line,
    }


@pytest.mark.parametrize(
    "fixture_dir",
    _list_dirs("asi_extraction"),
    ids=lambda p: p.name,
)
def test_asi_extraction_fixtures(fixture_dir: pathlib.Path) -> None:
    inp = json.loads((fixture_dir / "input.json").read_text(encoding="utf-8"))
    expected = json.loads(
        (fixture_dir / "expected_output.json").read_text(encoding="utf-8")
    )
    asi = AsiExtractionInput(
        ag_id=inp["asi_input"]["ag_id"],
        iteration=int(inp["asi_input"]["iteration"]),
        raw_audit=inp["asi_input"]["raw_audit"],
    )
    out = forward_asi_extraction_audit(asi)
    assert _asi_outcome_to_dict(out) == expected["outcome"]


@dataclass
class _StubDrift:
    triggered: bool
    delta_pp: float
    post_arbiter_current: float
    prev_iter_pre_accept_baseline: float | None
    threshold_pp: float
    reason_code: str | None


@pytest.mark.parametrize(
    "fixture_dir",
    _list_dirs("baseline_drift"),
    ids=lambda p: p.name,
)
def test_baseline_drift_fixtures(fixture_dir: pathlib.Path) -> None:
    inp = json.loads((fixture_dir / "input.json").read_text(encoding="utf-8"))
    expected = json.loads(
        (fixture_dir / "expected_output.json").read_text(encoding="utf-8")
    )
    drift_inp = BaselineDriftDiagnosticInput(
        ag_id=inp["drift_input"]["ag_id"],
        iteration=int(inp["drift_input"]["iteration"]),
        prev_iter_pre_accept_baseline=inp["drift_input"]["prev_iter_pre_accept_baseline"],
        current_post_arbiter_accuracy=float(
            inp["drift_input"]["current_post_arbiter_accuracy"]
        ),
        diagnostic_threshold_pp=float(inp["drift_input"]["diagnostic_threshold_pp"]),
    )
    stub_data = inp["stub_drift_decision"]
    stub = _StubDrift(**stub_data)
    out = build_baseline_drift_diagnostic(
        drift_inp,
        decide_drift_fn=lambda **kw: stub,
    )
    assert _drift_outcome_to_dict(out) == expected["outcome"]
