"""Phase 8.2 — the ``plural_top_n_collapse`` no-archetype branch
emits ``SkippedReason.NO_TOP_N_ARCHETYPE`` instead of
``NO_ARCHETYPE_OR_SLICE``.

This lets postmortem tooling distinguish 'archetype catalog is
missing the top-N shape' (a coverage gap requiring archetype-
catalog work) from 'pick_archetype returned None for some other
reason'.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    SkippedReason,
    run_cluster_driven_synthesis_for_single_cluster,
)


def _build_cluster(root_cause: str, asi_failure_type: str = "") -> dict:
    return {
        "cluster_id": "gs_test",
        "root_cause": root_cause,
        "asi_failure_type": asi_failure_type,
        "question_ids": ["q1"],
        "blame_set": (),
        "asi_blame_set": (),
    }


def _build_metadata_snapshot() -> dict:
    return {
        "_space_id": "test_space",
        "_cluster_synthesis_count": 0,
        "_failure_clusters": [],
        "benchmark_corpus": [],
        "instructions": {"example_question_sqls": []},
        "data_sources": {},
    }


def test_plural_top_n_collapse_emits_no_top_n_archetype():
    cluster = _build_cluster(root_cause="plural_top_n_collapse")
    md = _build_metadata_snapshot()
    with patch(
        "genie_space_optimizer.optimization.cluster_driven_synthesis."
        "pick_archetype",
        return_value=None,
    ):
        result = run_cluster_driven_synthesis_for_single_cluster(
            cluster=cluster,
            metadata_snapshot=md,
            w=MagicMock(),
            benchmarks=[],
        )
    assert result.proposal is None
    assert result.skipped_reason == SkippedReason.NO_TOP_N_ARCHETYPE.value


def test_generic_no_archetype_branch_preserved():
    """Non-top-N RCAs still get the generic NO_ARCHETYPE_OR_SLICE."""
    cluster = _build_cluster(root_cause="missing_join_spec")
    md = _build_metadata_snapshot()
    with patch(
        "genie_space_optimizer.optimization.cluster_driven_synthesis."
        "pick_archetype",
        return_value=None,
    ):
        result = run_cluster_driven_synthesis_for_single_cluster(
            cluster=cluster,
            metadata_snapshot=md,
            w=MagicMock(),
            benchmarks=[],
        )
    assert result.proposal is None
    assert (
        result.skipped_reason == SkippedReason.NO_ARCHETYPE_OR_SLICE.value
    )
