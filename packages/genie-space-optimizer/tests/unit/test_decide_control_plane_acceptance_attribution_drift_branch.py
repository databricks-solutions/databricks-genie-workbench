"""Phase 1 Task 6 — third branch in decide_control_plane_acceptance.

When GSO_ATTRIBUTION_DRIFT_WITH_DEBT=1 and the existing partial-harvest
branch rejected, the new branch fires and flips the rejection to
``accepted_with_attribution_drift_and_debt`` if the new policy is
under-policy with debt.

These tests pin the behavior on a ccf1d60d-shaped input and confirm
the legacy path is unchanged when the flag is off.
"""

from __future__ import annotations


def _ccf1d60d_inputs() -> dict:
    """ccf1d60d_iter1's inputs reshaped for decide_control_plane_acceptance.

    delta = +4.3pp, target unfixed, 1 unknown_to_hard regression. Same
    shape as the captured fixture; values match
    tests/replay/fixtures/policy_replay/ccf1d60d_iter1.json.
    """
    # row_is_hard_failure() requires result_correctness="no" AND
    # arbiter verdict NOT in the correct set; using a non-correct verdict
    # like "incorrect" makes the row count as a hard failure.
    _hard = {"result_correctness": "no", "arbiter_verdict": "incorrect"}
    return dict(
        baseline_accuracy=87.0,
        candidate_accuracy=91.3,
        target_qids=("7now_delivery_analytics_space_gs_026",),
        # pre_rows: target gs_026 is hard; gs_012 has no pre row
        # (unknown_to_hard pattern).
        pre_rows=[
            {"question_id": "7now_delivery_analytics_space_gs_026", **_hard},
        ],
        # post_rows: target stays hard, gs_012 regresses unknown → hard.
        post_rows=[
            {"question_id": "7now_delivery_analytics_space_gs_026", **_hard},
            {"question_id": "7now_delivery_analytics_space_gs_012", **_hard},
        ],
        thresholds_met=False,  # below thresholds so the legacy drift path's
                               # accept-no-debt branch does not pre-empt us
    )


def test_attribution_drift_branch_off_legacy_rejection_preserved(monkeypatch) -> None:
    """Flag off → legacy rejection reason is preserved."""
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", "0")
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_WITH_DEBT", raising=False)

    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(**_ccf1d60d_inputs())
    assert decision.accepted is False
    # The legacy reason on this shape with thresholds_met=False is one of
    # the pre-Phase-1 rejection reasons that the new branch is allowed to
    # override (target_qids_not_improved /
    # rejected_below_threshold_no_target_progress / similar). We assert
    # only that it's a non-accepted rejection so the test stays robust
    # against pre-existing reason-code renames.
    assert decision.reason_code != "accepted_with_attribution_drift_and_debt"
    assert "accepted" not in decision.reason_code or "not_" in decision.reason_code


def test_attribution_drift_branch_on_flips_to_accepted_with_debt(monkeypatch) -> None:
    """Flag on (default after T11.C) → ccf1d60d shape flips to accepted_with_attribution_drift_and_debt."""
    monkeypatch.delenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", raising=False)
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_WITH_DEBT", raising=False)

    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(**_ccf1d60d_inputs())
    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_attribution_drift_and_debt"
    # Debt qid was captured into out_of_target_regressed/regression_debt.
    assert "7now_delivery_analytics_space_gs_012" in (
        decision.regression_debt_qids
    )


def test_attribution_drift_branch_only_fires_after_partial_harvest_rejects(
    monkeypatch,
) -> None:
    """When BOTH flags are on, the partial-harvest branch is tried
    first. On a ccf1d60d shape (target unfixed) partial-harvest
    rejects on min_target_clusters_fixed, so the drift branch fires.
    On a target-fixed shape, partial-harvest would fire first and the
    drift branch would not see the rejection."""
    monkeypatch.setenv("GSO_ATTRIBUTION_DRIFT_WITH_DEBT", "1")
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")

    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    decision = decide_control_plane_acceptance(**_ccf1d60d_inputs())
    # ccf1d60d shape has target_fixed=0 → partial-harvest rejects on
    # min_target_clusters_fixed → drift branch fires.
    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_attribution_drift_and_debt"
