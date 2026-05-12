"""RCO-2b — _emit_contract_health_summary returns the built summary.

This is the surgical refactor that lets the harness thread the typed
summary into ``loop_out`` (Task 4) instead of forcing the lever-loop
notebook to re-parse stdout.
"""
from __future__ import annotations


def test_emit_returns_contract_health_summary_when_flag_on(
    monkeypatch, capsys,
) -> None:
    monkeypatch.setenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", "1")
    from genie_space_optimizer.optimization.contract_health import (
        ContractHealthSummary,
    )
    from genie_space_optimizer.optimization.harness import (
        _emit_contract_health_summary,
    )

    summary = _emit_contract_health_summary(
        optimization_run_id="run-emit-001",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
    )
    assert isinstance(summary, ContractHealthSummary)
    assert summary.optimization_run_id == "run-emit-001"
    # marker must still be printed to stdout (regression guard against
    # accidentally dropping the side-effect during the refactor)
    captured = capsys.readouterr()
    assert "GSO_CONTRACT_HEALTH_V1 " in captured.out


def test_emit_returns_none_when_flag_off(monkeypatch, capsys) -> None:
    monkeypatch.setenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", "0")
    from genie_space_optimizer.optimization.harness import (
        _emit_contract_health_summary,
    )

    result = _emit_contract_health_summary(
        optimization_run_id="run-emit-off",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
    )
    assert result is None
    captured = capsys.readouterr()
    assert "GSO_CONTRACT_HEALTH_V1 " not in captured.out


def test_emit_returns_none_on_internal_exception(monkeypatch) -> None:
    """The emit path is intentionally fail-soft: any internal exception
    yields ``None``, not a raise. RCO-2b relies on Task 4 treating
    ``None`` as 'no enforcement signal' so a buggy emit cannot crash
    the lever loop."""
    monkeypatch.setenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", "1")

    import genie_space_optimizer.optimization.contract_health as ch
    monkeypatch.setattr(
        ch,
        "build_contract_health_summary",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("simulated")),
    )

    from genie_space_optimizer.optimization.harness import (
        _emit_contract_health_summary,
    )
    result = _emit_contract_health_summary(
        optimization_run_id="run-emit-boom",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
    )
    assert result is None
