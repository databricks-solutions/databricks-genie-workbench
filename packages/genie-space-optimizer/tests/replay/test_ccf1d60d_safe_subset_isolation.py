"""Phase 5 fixture: subset isolation salvages a safe subset of patches
from a rolled-back iteration.

Maps to user-text `test_safe_subset_isolation_preserves_aggregate_gain`.
Anchor: ccf1d60d iter-1 (+4.0pp aggregate, gs_021 regressed).

NOTE — blocked on Phase 3+4 plan Tasks 2+3 — re-runs green after those
modules land. This test intentionally imports
``genie_space_optimizer.optimization.acceptance_tier`` (``AcceptanceTier``
and ``classify_acceptance_with_subset_isolation``), which are Phase 3+4
deliverables that have NOT yet shipped on this branch. The plan
(2026-05-14-final-closeout-phase-5-6-offline-gates.md, Task 5) accepts
that failing collection here is the forcing function for the missing
Phase 3+4 modules. Re-runs green once those modules are introduced.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.acceptance_tier import (
    AcceptanceTier,
    classify_acceptance_with_subset_isolation,
)

from tests.replay.fixtures.phase5._helpers import load


def _stub_subset_eval(qids: tuple[str, ...]) -> object:
    """Phase 5 stub for subset_eval_fn closure (Phase 3+4 Task 3 plumbing).

    Simulates a re-eval where dropping the regressing patch fixes gs_021
    and the remainder retains the +4.0pp aggregate gain.
    """
    class FakeReeval:
        accuracy_delta_pp = 4.0
        regressed_qids: tuple[str, ...] = ()
        target_fixed_qids: tuple[str, ...] = ("gs_026",) if False else ()
        target_still_hard_qids: tuple[str, ...] = ("gs_026",)
    return FakeReeval()


def test_subset_isolation_yields_safe_subset_accept() -> None:
    iter1 = load("ccf1d60d_iter1.json")
    surviving = load("ccf1d60d_iter1_surviving_patches.json")

    decision = {
        "iteration": iter1["iteration"],
        "ag_id": iter1["ag_id_selected"],
        "applied_patches": surviving,
        "target_qids": iter1["target_qids"],
        "out_of_target_regressed_qids": iter1["out_of_target_regressed_qids"],
        "target_fixed_qids": iter1["target_fixed_qids"],
        "target_still_hard_qids": iter1["target_still_hard_qids"],
        "baseline_post_arbiter": iter1["baseline_post_arbiter"],
        "candidate_post_arbiter": iter1["candidate_post_arbiter"],
    }

    verdict = classify_acceptance_with_subset_isolation(
        decision=decision,
        tier_policy=None,
        regression_debt_policy=None,
        subset_isolation_live=True,
        attribution_drift_enabled=False,
        subset_eval_fn=_stub_subset_eval,
    )

    assert verdict.tier == AcceptanceTier.SAFE_SUBSET_ACCEPT
    assert verdict.accept is True
    assert verdict.dropped_patches, (
        "subset isolation must identify at least one regressing patch to drop"
    )
    kept_ids = {p["patch_id"] for p in verdict.accepted_patches}
    dropped_ids = {p["patch_id"] for p in verdict.dropped_patches}
    assert kept_ids.isdisjoint(dropped_ids)
    assert kept_ids, "must keep at least one patch"


def test_subset_isolation_disabled_falls_back_to_reject_loss() -> None:
    """Defensive: with subset_isolation_live=False, the same input must
    NOT yield SAFE_SUBSET_ACCEPT — the legacy rollback path stays in force.
    """
    iter1 = load("ccf1d60d_iter1.json")
    surviving = load("ccf1d60d_iter1_surviving_patches.json")

    decision = {
        "iteration": iter1["iteration"],
        "ag_id": iter1["ag_id_selected"],
        "applied_patches": surviving,
        "target_qids": iter1["target_qids"],
        "out_of_target_regressed_qids": iter1["out_of_target_regressed_qids"],
        "target_fixed_qids": iter1["target_fixed_qids"],
        "target_still_hard_qids": iter1["target_still_hard_qids"],
        "baseline_post_arbiter": iter1["baseline_post_arbiter"],
        "candidate_post_arbiter": iter1["candidate_post_arbiter"],
    }

    verdict = classify_acceptance_with_subset_isolation(
        decision=decision,
        tier_policy=None,
        regression_debt_policy=None,
        subset_isolation_live=False,
        attribution_drift_enabled=False,
        subset_eval_fn=None,
    )

    assert verdict.tier != AcceptanceTier.SAFE_SUBSET_ACCEPT
    assert verdict.accept is False
