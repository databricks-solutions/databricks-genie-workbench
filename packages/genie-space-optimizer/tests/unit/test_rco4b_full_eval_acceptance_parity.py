"""RCO-4b Phase E Task 7 — parity between decide_full_eval_acceptance
and an in-test reimplementation of the legacy verdict-consolidation
logic from harness._run_gate_checks.

We do NOT drive the harness; the harness path depends on Spark, the
strict decision computation, control_plane, and Task 4 machinery.
Instead the test re-encodes the verdict-consolidation logic as a
small in-test reference and asserts the helper produces matching
audit-payloads across a parametrized matrix.

If the legacy harness logic changes, this test must be updated in
the same commit — the parity assertion only proves the helper
matches the in-test reference, not the live harness.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    decide_full_eval_acceptance,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    FullEvalAcceptanceInput,
)


@dataclass
class _LegacyAudits:
    verdict: dict
    rollback: dict | None
    accept: dict | None
    rollback_reason: str | None


def _legacy_verdict_audit(inp: FullEvalAcceptanceInput) -> dict:
    """Reimplements harness verdict-emit metrics dict (lines ~13987-14000)."""
    return {
        "delta_pp": inp.strict_decision_delta_pp,
        "min_gain_pp": inp.strict_decision_min_gain_pp,
        "post_arbiter_candidate": inp.strict_decision_post_arbiter_candidate,
        "post_arbiter_baseline": inp.strict_decision_post_arbiter_baseline,
        "previous_pre_arbiter": inp.pre_arbiter_baseline,
        "previous_post_arbiter": inp.strict_decision_post_arbiter_baseline,
    }


def _legacy_rollback_audit(inp: FullEvalAcceptanceInput) -> dict:
    """Reimplements harness rollback-emit metrics dict (lines ~14559-14577)."""
    return {
        "regression_count": len(inp.regressions),
        "post_arbiter_candidate": inp.strict_decision_post_arbiter_candidate,
        "post_arbiter_baseline": inp.strict_decision_post_arbiter_baseline,
        "delta_pp": inp.strict_decision_delta_pp,
        "min_gain_pp": inp.strict_decision_min_gain_pp,
        "pre_arbiter_candidate": inp.pre_arbiter_candidate,
        "pre_arbiter_baseline": inp.pre_arbiter_baseline,
        "diagnostic_regressions": list(inp.diagnostic_regression_judges),
    }


def _legacy_accept_audit(inp: FullEvalAcceptanceInput) -> dict:
    """Reimplements harness accept-emit metrics dict (lines ~14757-14773)."""
    return {
        "post_arbiter_candidate": inp.strict_decision_post_arbiter_candidate,
        "post_arbiter_baseline": inp.strict_decision_post_arbiter_baseline,
        "delta_pp": inp.strict_decision_delta_pp,
        "min_gain_pp": inp.strict_decision_min_gain_pp,
        "pre_arbiter_candidate": inp.pre_arbiter_candidate,
        "pre_arbiter_baseline": inp.pre_arbiter_baseline,
        "diagnostic_regressions": list(inp.diagnostic_regression_judges),
    }


def _legacy_consolidation(inp: FullEvalAcceptanceInput) -> _LegacyAudits:
    """Reimplements the verdict consolidation from harness._run_gate_checks."""
    verdict = _legacy_verdict_audit(inp)
    if inp.regressions:
        rb_reason = f"full_eval: {inp.regressions[0].get('judge', '')}"
        return _LegacyAudits(
            verdict=verdict,
            rollback=_legacy_rollback_audit(inp),
            accept=None,
            rollback_reason=rb_reason,
        )
    return _LegacyAudits(
        verdict=verdict,
        rollback=None,
        accept=_legacy_accept_audit(inp),
        rollback_reason=None,
    )


def _mk(**overrides) -> FullEvalAcceptanceInput:
    base = dict(
        ag_id="ag-parity",
        iteration=1,
        strict_decision_accepted=True,
        strict_decision_reason_code="accepted",
        strict_decision_delta_pp=2.0,
        strict_decision_post_arbiter_candidate=70.0,
        strict_decision_post_arbiter_baseline=68.0,
        strict_decision_min_gain_pp=1.0,
        pre_arbiter_candidate=66.0,
        pre_arbiter_baseline=64.0,
        control_plane_reason_code="accepted",
        diagnostic_regression_judges=(),
        regressions=(),
    )
    base.update(overrides)
    return FullEvalAcceptanceInput(**base)


PARITY_MATRIX = [
    ("clean_accept", _mk()),
    ("accept_with_drift",
        _mk(control_plane_reason_code="accepted_with_attribution_drift")),
    ("accept_with_debt",
        _mk(control_plane_reason_code="accepted_with_regression_debt")),
    ("accept_with_diagnostic_regressions",
        _mk(diagnostic_regression_judges=("j1", "j2"))),
    ("accept_unknown_control_plane_reason",
        _mk(control_plane_reason_code="some_new_branch")),
    ("rollback_single_regression",
        _mk(
            strict_decision_accepted=False,
            strict_decision_reason_code="below_min_gain",
            strict_decision_delta_pp=-1.0,
            regressions=({"judge": "j_main", "drop": 1.0},),
        )),
    ("rollback_multi_regression",
        _mk(
            strict_decision_accepted=False,
            strict_decision_reason_code="below_min_gain",
            regressions=(
                {"judge": "first", "drop": 1.0},
                {"judge": "second", "drop": 0.5},
                {"judge": "third", "drop": 0.2},
            ),
        )),
    ("rollback_with_drift_overridden_by_t4",
        _mk(
            control_plane_reason_code="accepted_with_attribution_drift",
            regressions=({"judge": "t4_per_question_blocker", "drop": 0.0},),
        )),
    ("rollback_with_debt_overridden_by_t4",
        _mk(
            control_plane_reason_code="accepted_with_regression_debt",
            regressions=({"judge": "t4_per_question_blocker", "drop": 0.0},),
        )),
    ("rollback_missing_judge_key",
        _mk(
            strict_decision_accepted=False,
            regressions=({"drop": 1.0},),
        )),
]


@pytest.mark.parametrize(
    "desc,inp",
    PARITY_MATRIX,
    ids=[m[0] for m in PARITY_MATRIX],
)
def test_pure_helper_matches_legacy_reference(
    desc: str,
    inp: FullEvalAcceptanceInput,
) -> None:
    pure = decide_full_eval_acceptance(inp)
    legacy = _legacy_consolidation(inp)

    # Verdict audit metrics fire on both branches.
    assert dict(pure.verdict_audit_metrics) == legacy.verdict, (
        f"{desc}: verdict metrics mismatch"
    )

    if legacy.rollback is not None:
        assert pure.accepted is False, f"{desc}: should rollback"
        assert pure.branch == "rollback", f"{desc}: branch should be 'rollback'"
        assert pure.rollback_reason == legacy.rollback_reason, (
            f"{desc}: rollback_reason mismatch"
        )
        assert dict(pure.rollback_audit_metrics or {}) == legacy.rollback, (
            f"{desc}: rollback metrics mismatch"
        )
        assert pure.accept_audit_metrics is None, (
            f"{desc}: accept metrics should be None on rollback"
        )
    else:
        assert pure.accepted is True, f"{desc}: should accept"
        assert pure.rollback_reason is None, f"{desc}: rollback_reason should be None"
        assert dict(pure.accept_audit_metrics or {}) == legacy.accept, (
            f"{desc}: accept metrics mismatch"
        )
        assert pure.rollback_audit_metrics is None, (
            f"{desc}: rollback metrics should be None on accept"
        )
