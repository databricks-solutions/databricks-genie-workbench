"""Phase 1.5 — recovery-pivot priority list after rollback."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.recovery_priority import (
    build_recovery_priority_list,
)


def test_no_regression_no_uncovered_returns_original_only():
    """Clean run: original cluster only, nothing to recover."""
    result = build_recovery_priority_list(
        regressed_qids_to_cluster_id={},
        uncovered_cluster_ids=[],
        original_target_cluster_id="c1",
    )
    assert result == ("c1",)


def test_regression_clusters_take_priority_over_original():
    """Regressed clusters MUST be presented first."""
    result = build_recovery_priority_list(
        regressed_qids_to_cluster_id={
            "gs_003": "c_regressed_a",
            "gs_007": "c_regressed_b",
        },
        uncovered_cluster_ids=["c_uncovered"],
        original_target_cluster_id="c_original",
    )
    # Order: regressed (sorted by cluster_id ascending) → uncovered →
    # original. Each cluster id appears at most once.
    assert result == (
        "c_regressed_a", "c_regressed_b", "c_uncovered", "c_original",
    )


def test_uncovered_after_regressed_before_original():
    result = build_recovery_priority_list(
        regressed_qids_to_cluster_id={"gs_001": "c_reg"},
        uncovered_cluster_ids=["c_unc"],
        original_target_cluster_id="c_orig",
    )
    assert result == ("c_reg", "c_unc", "c_orig")


def test_dedup_when_original_appears_in_regressed():
    """If the original_target_cluster is ALSO in the regressed set,
    it appears once (in the regressed position)."""
    result = build_recovery_priority_list(
        regressed_qids_to_cluster_id={"gs_001": "c_orig"},
        uncovered_cluster_ids=[],
        original_target_cluster_id="c_orig",
    )
    assert result == ("c_orig",)


def test_dedup_when_cluster_appears_in_both_regressed_and_uncovered():
    """A cluster id appearing in both regressed and uncovered is
    deduplicated; it stays in the regressed (higher-priority)
    position."""
    result = build_recovery_priority_list(
        regressed_qids_to_cluster_id={"gs_001": "c1"},
        uncovered_cluster_ids=["c1", "c2"],
        original_target_cluster_id="c_orig",
    )
    assert result == ("c1", "c2", "c_orig")


def test_empty_original_does_not_emit():
    """When original_target_cluster_id is blank, it is NOT appended."""
    result = build_recovery_priority_list(
        regressed_qids_to_cluster_id={"gs_001": "c1"},
        uncovered_cluster_ids=["c2"],
        original_target_cluster_id="",
    )
    assert result == ("c1", "c2")
