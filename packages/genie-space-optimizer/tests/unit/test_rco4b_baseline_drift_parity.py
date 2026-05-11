"""RCO-4b Phase D Task 8 — parity between build_baseline_drift_diagnostic
and a faithful reimplementation of the legacy inline emit logic.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    build_baseline_drift_diagnostic,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    BaselineDriftDiagnosticInput,
)


@dataclass
class _StubDrift:
    triggered: bool
    delta_pp: float
    post_arbiter_current: float
    prev_iter_pre_accept_baseline: float | None
    threshold_pp: float
    reason_code: str | None


def _legacy_emit(stub, ag_id: str, iteration: int):
    """Reimplements the legacy inline emission.

    Returns None if not triggered, else a dict with the log_line +
    audit_metrics that the legacy code would render.
    """
    if not stub.triggered:
        return None
    log_line = (
        "BASELINE DRIFT [%s]: iter %d post-arbiter %.1f%% is %.1fpp "
        "below the previous iteration's pre-acceptance baseline "
        "(%.1f%%). Logging suspected_stale_baseline diagnostic; "
        "iteration continues normally."
    ) % (
        ag_id, iteration, stub.post_arbiter_current, stub.delta_pp,
        float(stub.prev_iter_pre_accept_baseline or 0.0),
    )
    audit_metrics = {
        "post_arbiter_candidate": stub.post_arbiter_current,
        "prev_iter_pre_accept_baseline": (
            float(stub.prev_iter_pre_accept_baseline)
            if stub.prev_iter_pre_accept_baseline is not None
            else 0.0
        ),
        "delta_pp": stub.delta_pp,
        "threshold_pp": stub.threshold_pp,
    }
    return {
        "log_line": log_line,
        "audit_metrics": audit_metrics,
        "reason_code": stub.reason_code,
    }


PARITY_MATRIX = [
    ("not_triggered",
        _StubDrift(False, 1.0, 69.0, 70.0, 3.0, None)),
    ("triggered_small_drift",
        _StubDrift(True, 3.5, 66.5, 70.0, 3.0, "suspected_stale_baseline")),
    ("triggered_large_drift",
        _StubDrift(True, 15.0, 55.0, 70.0, 3.0, "suspected_stale_baseline")),
    ("triggered_null_prev_baseline",
        _StubDrift(True, 8.0, 60.0, None, 3.0, "suspected_stale_baseline")),
    ("not_triggered_null_prev_baseline",
        _StubDrift(False, 0.0, 0.0, None, 3.0, None)),
]


@pytest.mark.parametrize(
    "desc,stub",
    PARITY_MATRIX,
    ids=[m[0] for m in PARITY_MATRIX],
)
def test_pure_helper_matches_legacy_reference(desc: str, stub: _StubDrift) -> None:
    inp = BaselineDriftDiagnosticInput(
        ag_id="ag-parity",
        iteration=7,
        prev_iter_pre_accept_baseline=stub.prev_iter_pre_accept_baseline,
        current_post_arbiter_accuracy=stub.post_arbiter_current,
        diagnostic_threshold_pp=stub.threshold_pp,
    )
    pure = build_baseline_drift_diagnostic(
        inp, decide_drift_fn=lambda **kw: stub
    )
    legacy = _legacy_emit(stub, "ag-parity", 7)
    if legacy is None:
        assert pure.triggered is False, f"{desc}: helper must skip"
        assert pure.log_line == "", f"{desc}: helper must clear log_line"
        assert pure.audit_metrics == {}, f"{desc}: helper must clear audit_metrics"
    else:
        assert pure.triggered is True, f"{desc}: helper must emit"
        assert pure.log_line == legacy["log_line"], f"{desc} log_line"
        assert dict(pure.audit_metrics) == legacy["audit_metrics"], f"{desc} metrics"
        assert pure.reason_code == legacy["reason_code"], f"{desc} reason_code"
