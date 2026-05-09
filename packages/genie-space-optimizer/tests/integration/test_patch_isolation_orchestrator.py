"""Cycle 14B-T3 integration: synthetic single-patch regression flows
through the pure pipeline to a subset_accepts_clean verdict.

Live re-eval is intentionally not exercised — it is gated on C12-T5's
``patch_survival.json`` producer landing at the contract path.
"""

from __future__ import annotations


def _row(qid: str, rc: str, arbiter: str) -> dict:
    return {"question_id": qid, "result_correctness": rc, "arbiter": arbiter}


def _soft_row(qid: str) -> dict:
    """Actionable-soft baseline (rc=yes / arbiter=both_correct with a
    failed non-info judge)."""
    return {
        "question_id": qid,
        "result_correctness": "yes",
        "arbiter": "both_correct",
        "feedback/sql_correctness/value": "no",
    }


def _patch(patch_id: str, expanded_id: str, cluster_id: str, qids=()) -> dict:
    return {
        "patch_id": patch_id,
        "expanded_patch_id": expanded_id,
        "cluster_id": cluster_id,
        "affected_qids": list(qids),
    }


def test_pipeline_single_patch_regression_routes_to_subset_accepts_clean() -> None:
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )
    from genie_space_optimizer.optimization.patch_isolation import (
        attribute_regression_to_single_patch,
        build_isolation_subset,
        evaluate_isolation_verdict,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    # Original full-AG: target gs_026 fixed, gs_018 soft→hard
    # caused by patch p-bad. Three other patches do unrelated work.
    applied = (
        _patch("p-good-1", "L5:p-good-1#0", "H001", qids=("gs_026",)),  # fixes target
        _patch("p-bad", "L4:p-bad#0", "H002", qids=("gs_018",)),         # culprit
        _patch("p-other", "L3:p-other#0", "H003", qids=("gs_999",)),
    )

    original = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=95.7,
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _soft_row("gs_018"),
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _row("gs_018", "no", "ground_truth_correct"),
        ),
        max_new_hard_regressions=0,
    )
    assert original.accepted is False  # legacy gate rejects

    # Step 1: attribute the regression.
    attribution = attribute_regression_to_single_patch(
        regressed_qid=original.soft_to_hard_regressed_qids[0],
        applied_patches=applied,
    )
    assert attribution is not None
    assert attribution.expanded_patch_id == "L4:p-bad#0"

    # Step 2: build the subset.
    subset = build_isolation_subset(
        applied_patches=applied,
        patch_to_remove=attribution.expanded_patch_id,
    )
    assert {p["patch_id"] for p in subset} == {"p-good-1", "p-other"}

    # Step 3: simulate the subset's hypothetical decision (this is
    # what live re-eval would produce — for the test we synthesize
    # the post-rows directly).
    subset_decision = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=89.1,  # smaller gain without p-bad
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _soft_row("gs_018"),
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _soft_row("gs_018"),  # back to soft
        ),
    )

    # Step 4: route the verdict.
    verdict = evaluate_isolation_verdict(
        original_decision=original,
        subset_decision=subset_decision,
        policy=regression_debt_policy_pilot_default(),
    )
    assert verdict.outcome == "subset_accepts_clean"


def test_pipeline_multi_patch_regression_returns_no_attribution() -> None:
    """Two patches both name gs_018 → attribution returns None →
    orchestrator routes to multi_patch_regression halt.
    """
    from genie_space_optimizer.optimization.patch_isolation import (
        attribute_regression_to_single_patch,
    )

    applied = (
        _patch("p1", "L5:p1#0", "H001", qids=("gs_018", "gs_026")),
        _patch("p2", "L4:p2#0", "H002", qids=("gs_018",)),
    )
    assert (
        attribute_regression_to_single_patch(
            regressed_qid="gs_018", applied_patches=applied
        )
        is None
    )
