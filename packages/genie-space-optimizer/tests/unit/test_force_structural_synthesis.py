"""Forced structural synthesis for SQL-shape RCAs.

Today run_cluster_driven_synthesis_for_single_cluster returns dict
or None. The caller can't tell whether None means 'no archetype
matched', 'budget exhausted', or 'arbiter gate rejected'. This suite
pins the new ClusterSynthesisResult dataclass with an
``attempted_archetypes`` provenance list.
"""
from __future__ import annotations


def test_cluster_synthesis_result_dataclass_exists() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )

    r = ClusterSynthesisResult(
        proposal=None,
        attempted_archetypes=("ordered_list_by_metric",),
        skipped_reason="archetype_validation_failed",
    )
    assert r.proposal is None
    assert r.attempted_archetypes == ("ordered_list_by_metric",)
    assert r.skipped_reason == "archetype_validation_failed"


def test_synthesis_returns_result_object_not_dict_or_none() -> None:
    """Returns a ClusterSynthesisResult; ``proposal`` field carries
    the legacy ``dict`` payload (or None).
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
        run_cluster_driven_synthesis_for_single_cluster,
    )

    cluster = {
        "cluster_id": "H001",
        "question_ids": ["gs_026"],
        "root_cause": "plural_top_n_collapse",
    }
    metadata_snapshot = {
        "_space_id": "test_space",
        "_cluster_synthesis_count": 999,  # force budget skip
    }
    result = run_cluster_driven_synthesis_for_single_cluster(
        cluster, metadata_snapshot, benchmarks=[],
    )
    assert isinstance(result, ClusterSynthesisResult)
    assert result.proposal is None
    assert "budget" in (result.skipped_reason or "")
