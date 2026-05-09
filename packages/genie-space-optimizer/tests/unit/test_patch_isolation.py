"""Cycle 14B-T3 — pure helpers for patch-subset isolation."""

from __future__ import annotations


def _patch(
    patch_id: str,
    expanded_id: str,
    cluster_id: str,
    affected_qids: tuple[str, ...] = (),
) -> dict:
    return {
        "patch_id": patch_id,
        "expanded_patch_id": expanded_id,
        "cluster_id": cluster_id,
        "affected_qids": list(affected_qids),
    }


def test_attribute_returns_single_patch_when_only_one_overlaps_regressed_qid() -> None:
    from genie_space_optimizer.optimization.patch_isolation import (
        attribute_regression_to_single_patch,
        SinglePatchAttribution,
    )

    applied = (
        _patch("p1", "L5:p1#0", "H001", affected_qids=("gs_018",)),  # culprit
        _patch("p2", "L4:p2#0", "H002", affected_qids=("gs_026",)),
        _patch("p3", "L3:p3#0", "H003", affected_qids=("gs_999",)),
    )
    result = attribute_regression_to_single_patch(
        regressed_qid="gs_018",
        applied_patches=applied,
    )
    assert isinstance(result, SinglePatchAttribution)
    assert result.patch_id == "p1"
    assert result.expanded_patch_id == "L5:p1#0"
    assert result.cluster_id == "H001"
    assert result.confidence == 1.0


def test_attribute_returns_none_when_multiple_patches_overlap() -> None:
    from genie_space_optimizer.optimization.patch_isolation import (
        attribute_regression_to_single_patch,
    )

    applied = (
        _patch("p1", "L5:p1#0", "H001", affected_qids=("gs_018",)),
        _patch("p2", "L4:p2#0", "H001", affected_qids=("gs_018",)),  # also overlaps
    )
    assert (
        attribute_regression_to_single_patch(
            regressed_qid="gs_018", applied_patches=applied
        )
        is None
    )


def test_attribute_returns_none_when_no_patch_overlaps() -> None:
    from genie_space_optimizer.optimization.patch_isolation import (
        attribute_regression_to_single_patch,
    )

    applied = (_patch("p1", "L5:p1#0", "H001", affected_qids=("gs_026",)),)
    assert (
        attribute_regression_to_single_patch(
            regressed_qid="gs_018", applied_patches=applied
        )
        is None
    )


def test_attribute_uses_cluster_lineage_when_qid_overlap_unavailable() -> None:
    """Some patches don't carry affected_qids but do carry cluster_id;
    if the cluster's QID set is provided separately, attribution
    falls back to cluster lineage with confidence 0.5.
    """
    from genie_space_optimizer.optimization.patch_isolation import (
        attribute_regression_to_single_patch,
    )

    applied = (
        _patch("p1", "L5:p1#0", "H001"),  # no affected_qids
        _patch("p2", "L4:p2#0", "H002"),
    )
    cluster_qids = {"H001": ("gs_018",), "H002": ("gs_026",)}
    result = attribute_regression_to_single_patch(
        regressed_qid="gs_018",
        applied_patches=applied,
        cluster_qids=cluster_qids,
    )
    assert result is not None
    assert result.patch_id == "p1"
    assert result.confidence == 0.5


def test_attribute_returns_none_for_empty_applied_patches() -> None:
    from genie_space_optimizer.optimization.patch_isolation import (
        attribute_regression_to_single_patch,
    )

    assert (
        attribute_regression_to_single_patch(
            regressed_qid="gs_018", applied_patches=()
        )
        is None
    )


# ── Cycle 14B-T3 Task 2: build_isolation_subset ───────────────────────


def test_build_isolation_subset_removes_named_patch() -> None:
    from genie_space_optimizer.optimization.patch_isolation import (
        build_isolation_subset,
    )

    applied = (
        _patch("p1", "L5:p1#0", "H001"),
        _patch("p2", "L4:p2#0", "H002"),
        _patch("p3", "L3:p3#0", "H003"),
    )
    subset = build_isolation_subset(
        applied_patches=applied, patch_to_remove="L4:p2#0"
    )
    assert len(subset) == 2
    assert {p["patch_id"] for p in subset} == {"p1", "p3"}


def test_build_isolation_subset_falls_back_to_patch_id() -> None:
    from genie_space_optimizer.optimization.patch_isolation import (
        build_isolation_subset,
    )

    applied = (
        {"patch_id": "p1"},  # no expanded_patch_id
        {"patch_id": "p2"},
    )
    subset = build_isolation_subset(applied_patches=applied, patch_to_remove="p2")
    assert subset == ({"patch_id": "p1"},)


def test_build_isolation_subset_no_match_returns_full_input() -> None:
    """Removing a non-existent patch is a no-op (caller's contract
    requires the patch_id to exist; the helper is defensive)."""
    from genie_space_optimizer.optimization.patch_isolation import (
        build_isolation_subset,
    )

    applied = (_patch("p1", "L5:p1#0", "H001"),)
    subset = build_isolation_subset(applied_patches=applied, patch_to_remove="p999")
    assert len(subset) == 1


def test_build_isolation_subset_preserves_order() -> None:
    from genie_space_optimizer.optimization.patch_isolation import (
        build_isolation_subset,
    )

    applied = tuple(
        _patch(f"p{i}", f"L{i}:p{i}#0", f"H00{i}") for i in range(1, 6)
    )
    subset = build_isolation_subset(
        applied_patches=applied, patch_to_remove="L3:p3#0"
    )
    assert [p["patch_id"] for p in subset] == ["p1", "p2", "p4", "p5"]


# ── Cycle 14B-T3 Task 3: evaluate_isolation_verdict ──────────────────


def _row(qid: str, rc: str, arbiter: str) -> dict:
    return {"question_id": qid, "result_correctness": rc, "arbiter": arbiter}


def _soft_row(qid: str) -> dict:
    """Actionable-soft row (rc=yes / arbiter=both_correct with a failed
    non-info judge) — row_status returns "soft"."""
    return {
        "question_id": qid,
        "result_correctness": "yes",
        "arbiter": "both_correct",
        "feedback/sql_correctness/value": "no",
    }


def _decision_anchor():
    """The new-anchor full-AG decision: gs_026 fixed, gs_018
    soft→hard debt, +17.4pp aggregate. Pre-policy under
    max_new_hard_regressions=0 → rejected; for this test we feed
    it directly to the verdict helper.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    return decide_control_plane_acceptance(
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


def _decision_subset_clean():
    """Hypothetical subset decision: gs_026 still fixed, gs_018
    no longer regressed (because we removed the patch that broke it).
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )

    return decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=89.1,  # smaller gain (we removed a patch)
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


def test_verdict_subset_accepts_clean() -> None:
    from genie_space_optimizer.optimization.patch_isolation import (
        evaluate_isolation_verdict,
        IsolationVerdict,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    verdict = evaluate_isolation_verdict(
        original_decision=_decision_anchor(),
        subset_decision=_decision_subset_clean(),
        policy=regression_debt_policy_pilot_default(),
    )
    assert isinstance(verdict, IsolationVerdict)
    assert verdict.outcome == "subset_accepts_clean"
    assert verdict.subset_aggregate_gain_pp == 10.8  # 89.1 - 78.3


def test_verdict_subset_regresses_aggregate() -> None:
    """The patch we removed was load-bearing — without it the
    candidate is worse than baseline.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )
    from genie_space_optimizer.optimization.patch_isolation import (
        evaluate_isolation_verdict,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        regression_debt_policy_pilot_default,
    )

    subset_regressed = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=70.0,  # below baseline
        target_qids=("gs_026",),
        pre_rows=(_row("gs_026", "no", "ground_truth_correct"),),
        post_rows=(_row("gs_026", "no", "ground_truth_correct"),),
    )

    verdict = evaluate_isolation_verdict(
        original_decision=_decision_anchor(),
        subset_decision=subset_regressed,
        policy=regression_debt_policy_pilot_default(),
    )
    assert verdict.outcome == "subset_regresses_aggregate"


def test_verdict_subset_still_over_policy() -> None:
    """Subset still has debt over the per-iteration cap →
    fall through to multi-patch halt.
    """
    from genie_space_optimizer.optimization.control_plane import (
        decide_control_plane_acceptance,
    )
    from genie_space_optimizer.optimization.patch_isolation import (
        evaluate_isolation_verdict,
    )
    from genie_space_optimizer.optimization.acceptance_policy import (
        RegressionDebtPolicy,
    )

    # Restrictive policy: no debt allowed.
    strict_policy = RegressionDebtPolicy(
        max_debt_qids=0,
        allowed_debt_buckets=frozenset(),
        cumulative_debt_max=0,
    )
    subset_with_debt = decide_control_plane_acceptance(
        baseline_accuracy=78.3,
        candidate_accuracy=89.1,
        target_qids=("gs_026",),
        pre_rows=(
            _row("gs_026", "no", "ground_truth_correct"),
            _soft_row("gs_018"),
        ),
        post_rows=(
            _row("gs_026", "yes", "both_correct"),
            _row("gs_018", "no", "ground_truth_correct"),  # still soft→hard
        ),
        max_new_hard_regressions=0,
    )
    verdict = evaluate_isolation_verdict(
        original_decision=_decision_anchor(),
        subset_decision=subset_with_debt,
        policy=strict_policy,
    )
    assert verdict.outcome == "subset_still_over_policy"
