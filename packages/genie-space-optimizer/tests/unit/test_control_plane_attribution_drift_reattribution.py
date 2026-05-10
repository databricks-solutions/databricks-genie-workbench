"""Cycle 14-C T2 + T3 — ControlPlaneAcceptance field extension and
reattribution wire-up."""
from __future__ import annotations

from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
)


def test_dataclass_accepts_new_fields_with_defaults() -> None:
    """Back-compat: legacy callers that don't pass the new fields
    still construct a ControlPlaneAcceptance successfully; the new
    fields default to empty tuples."""
    decision = ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted",
        baseline_accuracy=83.3,
        candidate_accuracy=95.8,
        delta_pp=12.5,
        target_qids=("gs_024",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_024",),
        out_of_target_regressed_qids=(),
    )
    assert decision.accidentally_improved_qids == ()
    assert decision.unresolved_target_debt_qids == ()


def test_dataclass_accepts_new_fields_when_explicitly_passed() -> None:
    decision = ControlPlaneAcceptance(
        accepted=True,
        reason_code="accepted_with_attribution_drift",
        baseline_accuracy=83.3,
        candidate_accuracy=95.8,
        delta_pp=12.5,
        target_qids=("gs_024",),
        target_fixed_qids=(),
        target_still_hard_qids=("gs_024",),
        out_of_target_regressed_qids=(),
        accidentally_improved_qids=("gs_007", "gs_009", "gs_013"),
        unresolved_target_debt_qids=("gs_024",),
    )
    assert decision.accidentally_improved_qids == ("gs_007", "gs_009", "gs_013")
    assert decision.unresolved_target_debt_qids == ("gs_024",)


def _hard(qid: str) -> dict:
    """Hard-failure row recognised by both row_status and the
    result_correctness/arbiter predicate used inside
    decide_control_plane_acceptance."""
    return {
        "question_id": qid,
        "row_status": "hard",
        "result_correctness": "no",
        "arbiter": "neither_correct",
    }


def _passing(qid: str) -> dict:
    return {
        "question_id": qid,
        "row_status": "passing",
        "result_correctness": "yes",
        "arbiter": "both_correct",
    }


def test_attribution_drift_branch_populates_reattribution_fields() -> None:
    """Anchor airline iter 1: target=gs_024 stays hard; gs_007/9/13
    flip from hard to passing; net +12.5pp; thresholds met; zero
    regressions. Branch fires and reattribution is populated."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )
    pre = [
        _hard("gs_024"), _hard("gs_007"), _hard("gs_009"), _hard("gs_013"),
        _passing("gs_999"),
    ]
    post = [
        _hard("gs_024"),
        _passing("gs_007"), _passing("gs_009"), _passing("gs_013"),
        _passing("gs_999"),
    ]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=83.3,
        candidate_accuracy=95.8,
        target_qids=("gs_024",),
        pre_rows=pre,
        post_rows=post,
        thresholds_met=True,
    )
    assert decision.reason_code == "accepted_with_attribution_drift"
    assert decision.accepted is True
    assert tuple(sorted(decision.accidentally_improved_qids)) == (
        "gs_007", "gs_009", "gs_013",
    )
    assert decision.unresolved_target_debt_qids == ("gs_024",)


def test_non_drift_branches_leave_reattribution_empty() -> None:
    """Every branch that is NOT accepted_with_attribution_drift
    must return empty tuples for the reattribution fields."""
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(
        baseline_accuracy=80.0, candidate_accuracy=85.0,
        target_qids=("gs_001",),
        pre_rows=[_hard("gs_001"), _hard("gs_002")],
        post_rows=[_passing("gs_001"), _hard("gs_002")],
    )
    assert decision.reason_code == "accepted"
    assert decision.accidentally_improved_qids == ()
    assert decision.unresolved_target_debt_qids == ()

    decision = decide_control_plane_acceptance(
        baseline_accuracy=80.0, candidate_accuracy=80.0,
        target_qids=("gs_001",),
        pre_rows=[_hard("gs_001")], post_rows=[_hard("gs_001")],
    )
    # Whatever non-drift rejection branch fires (reason names vary
    # by input shape), the reattribution fields stay empty.
    assert decision.reason_code != "accepted_with_attribution_drift"
    assert decision.accidentally_improved_qids == ()
    assert decision.unresolved_target_debt_qids == ()


def test_attribution_drift_observability_flag_off_returns_empty(
    monkeypatch,
) -> None:
    """Circuit-breaker: with the observability flag off, the
    branch still fires (it's a behaviour-stable accept) but the
    new fields stay empty. Legacy postmortem tooling sees
    pre-14-C output."""
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_REATTRIBUTION", "0")
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )
    pre = [_hard("gs_024"), _hard("gs_007")]
    post = [_hard("gs_024"), _passing("gs_007")]
    decision = decide_control_plane_acceptance(
        baseline_accuracy=80.0, candidate_accuracy=85.0,
        target_qids=("gs_024",),
        pre_rows=pre, post_rows=post,
        thresholds_met=True,
    )
    assert decision.reason_code == "accepted_with_attribution_drift"
    assert decision.accepted is True
    assert decision.accidentally_improved_qids == ()
    assert decision.unresolved_target_debt_qids == ()
