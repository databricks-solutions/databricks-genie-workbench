"""RCO-4b Phase D Task 4 — build_baseline_drift_diagnostic tests.

Wraps ``acceptance_policy.decide_baseline_drift`` and packages the
audit-row + log-line payload. Pure function — no logger calls, no
``_audit_emit`` calls.
"""
from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.stages.gate_types import (
    BaselineDriftDiagnosticInput,
)


@dataclass
class _FakeDriftDecision:
    triggered: bool
    delta_pp: float
    post_arbiter_current: float
    prev_iter_pre_accept_baseline: float | None
    threshold_pp: float
    reason_code: str | None


def _make_input(**overrides) -> BaselineDriftDiagnosticInput:
    base = dict(
        ag_id="ag-001",
        iteration=3,
        prev_iter_pre_accept_baseline=70.0,
        current_post_arbiter_accuracy=65.0,
        diagnostic_threshold_pp=3.0,
    )
    base.update(overrides)
    return BaselineDriftDiagnosticInput(**base)


def test_not_triggered_yields_clean_outcome() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        build_baseline_drift_diagnostic,
    )
    fake = _FakeDriftDecision(
        triggered=False,
        delta_pp=1.0,
        post_arbiter_current=69.0,
        prev_iter_pre_accept_baseline=70.0,
        threshold_pp=3.0,
        reason_code=None,
    )
    out = build_baseline_drift_diagnostic(
        _make_input(current_post_arbiter_accuracy=69.0),
        decide_drift_fn=lambda **kw: fake,
    )
    assert out.triggered is False
    assert out.delta_pp == 0.0  # default; not surfaced when not triggered
    assert out.audit_metrics == {}
    assert out.reason_code is None
    assert out.log_line == ""


def test_triggered_yields_full_audit_payload() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        build_baseline_drift_diagnostic,
    )
    fake = _FakeDriftDecision(
        triggered=True,
        delta_pp=5.0,
        post_arbiter_current=65.0,
        prev_iter_pre_accept_baseline=70.0,
        threshold_pp=3.0,
        reason_code="suspected_stale_baseline",
    )
    out = build_baseline_drift_diagnostic(
        _make_input(),
        decide_drift_fn=lambda **kw: fake,
    )
    assert out.triggered is True
    assert out.delta_pp == 5.0
    assert out.reason_code == "suspected_stale_baseline"
    assert out.audit_metrics == {
        "post_arbiter_candidate": 65.0,
        "prev_iter_pre_accept_baseline": 70.0,
        "delta_pp": 5.0,
        "threshold_pp": 3.0,
    }
    assert "BASELINE DRIFT [ag-001]" in out.log_line
    assert "iter 3" in out.log_line
    assert "65.0%" in out.log_line
    assert "5.0pp" in out.log_line
    assert "70.0%" in out.log_line


def test_decide_drift_fn_called_with_correct_kwargs() -> None:
    """The helper must call the injected decide_drift_fn with exactly
    ``post_arbiter_current``, ``prev_iter_pre_accept_baseline``, and
    ``threshold_pp`` — matching the legacy call signature at
    ``harness._run_gate_checks:13712-13716``."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        build_baseline_drift_diagnostic,
    )
    captured = {}

    def spy(**kw):
        captured.update(kw)
        return _FakeDriftDecision(
            triggered=False,
            delta_pp=0.0,
            post_arbiter_current=kw["post_arbiter_current"],
            prev_iter_pre_accept_baseline=kw["prev_iter_pre_accept_baseline"],
            threshold_pp=kw["threshold_pp"],
            reason_code=None,
        )

    build_baseline_drift_diagnostic(
        _make_input(
            current_post_arbiter_accuracy=68.5,
            prev_iter_pre_accept_baseline=72.0,
            diagnostic_threshold_pp=4.0,
        ),
        decide_drift_fn=spy,
    )
    assert captured == {
        "post_arbiter_current": 68.5,
        "prev_iter_pre_accept_baseline": 72.0,
        "threshold_pp": 4.0,
    }


def test_triggered_with_null_prev_iter_baseline_uses_zero_in_log() -> None:
    """Legacy code: ``float(_drift.prev_iter_pre_accept_baseline or 0.0)``
    in the log line. The helper must mirror this fallback."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        build_baseline_drift_diagnostic,
    )
    fake = _FakeDriftDecision(
        triggered=True,
        delta_pp=10.0,
        post_arbiter_current=60.0,
        prev_iter_pre_accept_baseline=None,
        threshold_pp=3.0,
        reason_code="suspected_stale_baseline",
    )
    out = build_baseline_drift_diagnostic(
        _make_input(prev_iter_pre_accept_baseline=None),
        decide_drift_fn=lambda **kw: fake,
    )
    assert out.triggered is True
    assert "0.0%" in out.log_line  # fallback baseline value


def test_default_decide_drift_fn_is_acceptance_policy_one() -> None:
    """When no ``decide_drift_fn`` is passed, the helper must use the
    production ``decide_baseline_drift`` from acceptance_policy."""
    from genie_space_optimizer.optimization.acceptance_policy import (
        decide_baseline_drift,
    )
    from genie_space_optimizer.optimization.stages import eval_gates

    kwd = eval_gates.build_baseline_drift_diagnostic.__kwdefaults__ or {}
    default_fn = kwd.get("decide_drift_fn")
    assert default_fn is decide_baseline_drift
