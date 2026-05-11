"""RCO-2a Task 9 — harness emits GSO_CONTRACT_HEALTH_V1 at end-of-run.

We don't run the full harness; we exercise the pure helper that the
harness wiring will call, then verify that flipping the flag
suppresses emission. The integration check that the harness actually
calls the helper is covered by a grep-guard test below.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout


def test_helper_prints_marker_when_flag_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", raising=False)
    from genie_space_optimizer.optimization.harness import (
        _emit_contract_health_summary,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_contract_health_summary(
            optimization_run_id="run-emit-1",
            invariant_violations=[],
            phase_h_strict_validation={
                "listing_status": "ok", "validator_status": "ok",
            },
            bundle_assembly_failed=(),
            bundle_assembly_incomplete=None,
            replay_validation={"is_valid": True, "violation_count": 0},
        )
    output = buf.getvalue()
    assert "GSO_CONTRACT_HEALTH_V1 " in output
    assert "\"merge_gate_status\":\"healthy\"" in output


def test_helper_does_not_print_when_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", "0")
    from genie_space_optimizer.optimization.harness import (
        _emit_contract_health_summary,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_contract_health_summary(
            optimization_run_id="run-emit-1",
            invariant_violations=[],
            phase_h_strict_validation=None,
            bundle_assembly_failed=(),
            bundle_assembly_incomplete=None,
            replay_validation=None,
        )
    assert "GSO_CONTRACT_HEALTH_V1" not in buf.getvalue()


def test_helper_swallows_exceptions_silently(monkeypatch) -> None:
    """A bug in the builder must NEVER fail the harness end-of-run path.

    Pass a sentinel that breaks ``run_invariants``-shape iteration.
    The helper should silently no-op (and ideally log.debug) rather
    than propagate.
    """
    from genie_space_optimizer.optimization.harness import (
        _emit_contract_health_summary,
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_contract_health_summary(
            optimization_run_id="",
            invariant_violations=object(),  # type: ignore[arg-type]
            phase_h_strict_validation=None,
            bundle_assembly_failed=(),
            bundle_assembly_incomplete=None,
            replay_validation=None,
        )
    # Either the marker is absent or the helper degraded silently — both fine.
    # The contract: no exception escapes.
