"""RCO-2b — MergeGateBlockedError type guard."""
from __future__ import annotations


def test_merge_gate_blocked_error_is_runtime_exception() -> None:
    from genie_space_optimizer.optimization.contract_health import (
        MergeGateBlockedError,
    )
    assert issubclass(MergeGateBlockedError, Exception)
    err = MergeGateBlockedError(
        merge_gate_status="merge_gate_blocked",
        high_tier_violation_count=3,
        optimization_run_id="run-abc",
    )
    assert str(err) == (
        "merge_gate_status=merge_gate_blocked "
        "high_tier_violations=3 "
        "optimization_run_id=run-abc"
    )
    assert err.merge_gate_status == "merge_gate_blocked"
    assert err.high_tier_violation_count == 3
    assert err.optimization_run_id == "run-abc"
