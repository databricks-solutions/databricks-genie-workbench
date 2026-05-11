"""RCO-4b Phase E Task 3 — decide_full_eval_acceptance tests.

The helper consolidates already-computed upstream decisions
(``_strict_decision``, ``_t4_verdict``, ``_control_plane_decision``)
plus the populated ``regressions[]`` list into a typed verdict
outcome carrying all three audit-metrics payloads.

Pure function — no logger calls, no ``_audit_emit`` calls, no Spark.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.stages.gate_types import (
    FullEvalAcceptanceInput,
)


def _make_input(**overrides) -> FullEvalAcceptanceInput:
    base = dict(
        ag_id="ag-001",
        iteration=3,
        strict_decision_accepted=True,
        strict_decision_reason_code="accepted",
        strict_decision_delta_pp=3.5,
        strict_decision_post_arbiter_candidate=72.0,
        strict_decision_post_arbiter_baseline=68.5,
        strict_decision_min_gain_pp=1.0,
        pre_arbiter_candidate=70.0,
        pre_arbiter_baseline=66.0,
        control_plane_reason_code="accepted",
        diagnostic_regression_judges=(),
        regressions=(),
    )
    base.update(overrides)
    return FullEvalAcceptanceInput(**base)


# ── Accept-branch tests ───────────────────────────────────────────────


def test_empty_regressions_accepts_with_default_branch() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(_make_input())
    assert out.accepted is True
    assert out.branch == "accept"
    assert out.rollback_reason is None
    assert out.regression_count == 0


def test_accept_with_attribution_drift_branch() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(
        _make_input(control_plane_reason_code="accepted_with_attribution_drift")
    )
    assert out.accepted is True
    assert out.branch == "accept_with_drift"


def test_accept_with_regression_debt_branch() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(
        _make_input(control_plane_reason_code="accepted_with_regression_debt")
    )
    assert out.accepted is True
    assert out.branch == "accept_with_debt"


def test_unknown_control_plane_reason_falls_back_to_accept() -> None:
    """Defensive: any unrecognized control-plane reason code on an
    empty-regressions path falls back to ``branch="accept"``."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(
        _make_input(control_plane_reason_code="some_new_branch_we_havent_seen")
    )
    assert out.accepted is True
    assert out.branch == "accept"


def test_accept_branch_populates_verdict_and_accept_metrics_only() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(_make_input(
        diagnostic_regression_judges=("judge_x", "judge_y"),
    ))
    assert out.verdict_audit_metrics == {
        "delta_pp": 3.5,
        "min_gain_pp": 1.0,
        "post_arbiter_candidate": 72.0,
        "post_arbiter_baseline": 68.5,
        "previous_pre_arbiter": 66.0,
        "previous_post_arbiter": 68.5,
    }
    assert out.rollback_audit_metrics is None
    assert out.accept_audit_metrics == {
        "post_arbiter_candidate": 72.0,
        "post_arbiter_baseline": 68.5,
        "delta_pp": 3.5,
        "min_gain_pp": 1.0,
        "pre_arbiter_candidate": 70.0,
        "pre_arbiter_baseline": 66.0,
        "diagnostic_regressions": ["judge_x", "judge_y"],
    }


# ── Rollback-branch tests ─────────────────────────────────────────────


def test_single_regression_rolls_back() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(_make_input(
        strict_decision_accepted=False,
        strict_decision_reason_code="below_min_gain",
        regressions=({"judge": "overall_accuracy_guard", "drop": 2.0},),
    ))
    assert out.accepted is False
    assert out.branch == "rollback"
    assert out.rollback_reason == "full_eval: overall_accuracy_guard"
    assert out.regression_count == 1


def test_multiple_regressions_uses_first_judge_in_reason() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(_make_input(
        strict_decision_accepted=False,
        strict_decision_reason_code="below_min_gain",
        regressions=(
            {"judge": "first_judge", "drop": 2.0},
            {"judge": "second_judge", "drop": 1.0},
        ),
    ))
    assert out.accepted is False
    assert out.rollback_reason == "full_eval: first_judge"
    assert out.regression_count == 2


def test_rollback_branch_populates_verdict_and_rollback_metrics_only() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(_make_input(
        strict_decision_accepted=False,
        strict_decision_reason_code="below_min_gain",
        strict_decision_delta_pp=-1.5,
        strict_decision_post_arbiter_candidate=67.0,
        diagnostic_regression_judges=("d1",),
        regressions=({"judge": "overall", "drop": 1.5},),
    ))
    assert out.verdict_audit_metrics == {
        "delta_pp": -1.5,
        "min_gain_pp": 1.0,
        "post_arbiter_candidate": 67.0,
        "post_arbiter_baseline": 68.5,
        "previous_pre_arbiter": 66.0,
        "previous_post_arbiter": 68.5,
    }
    assert out.rollback_audit_metrics == {
        "regression_count": 1,
        "post_arbiter_candidate": 67.0,
        "post_arbiter_baseline": 68.5,
        "delta_pp": -1.5,
        "min_gain_pp": 1.0,
        "pre_arbiter_candidate": 70.0,
        "pre_arbiter_baseline": 66.0,
        "diagnostic_regressions": ["d1"],
    }
    assert out.accept_audit_metrics is None


def test_rollback_uses_strict_decision_reason_code_for_verdict() -> None:
    """The verdict-time audit emits ``decision="pass"|"fail"`` based
    on strict_decision_accepted, with ``reason_code`` from the
    strict decision. The helper surfaces this as ``out.reason_code``."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(_make_input(
        strict_decision_accepted=False,
        strict_decision_reason_code="post_arbiter_regression",
        regressions=({"judge": "j1", "drop": 1.0},),
    ))
    assert out.reason_code == "post_arbiter_regression"


def test_accept_with_drift_and_t4_regression_still_rolls_back() -> None:
    """Edge case: control plane says accept-with-drift, but a Task 4
    per-question regression was appended to ``regressions[]``. The
    helper trusts ``regressions[]`` — if it's non-empty, the verdict
    is rollback regardless of the control-plane reason code."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(_make_input(
        control_plane_reason_code="accepted_with_attribution_drift",
        regressions=({"judge": "t4_per_question", "drop": 0.0},),
    ))
    assert out.accepted is False
    assert out.branch == "rollback"


def test_regression_without_judge_key_uses_empty_string_in_reason() -> None:
    """Defensive: a regression entry missing ``judge`` falls back to
    empty string in the rollback_reason."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        decide_full_eval_acceptance,
    )
    out = decide_full_eval_acceptance(_make_input(
        strict_decision_accepted=False,
        strict_decision_reason_code="below_min_gain",
        regressions=({"drop": 1.0},),
    ))
    assert out.accepted is False
    assert out.rollback_reason == "full_eval: "
