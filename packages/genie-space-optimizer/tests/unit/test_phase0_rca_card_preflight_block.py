"""Phase 0 — synthesis dispatch must refuse clusters with no
RCA card and emit a typed ``missing_rca_card`` NSC record.

Two live runs (airline ``59a173d3``, 7now ``ab65fefe``) showed
invariant I7 ``open_cluster_ungrounded_at_ag_emit`` firing 48 times
for H001/H002, and ``candidate_ledger.rca_card_id_or_provisional``
empty for every iteration. Decomposed AGs reached the synthesizer
without an RCA card; the synthesizer then returned empty
(``attempted_archetypes=[]``) without explaining why. This pre-
flight makes the absence explicit.
"""

from unittest.mock import MagicMock

from genie_space_optimizer.optimization import forced_synthesis_dispatch
from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    ClusterSynthesisResult,
)


def test_ungrounded_cluster_is_refused_before_synthesis_invocation():
    """Cluster with empty rca_card must not reach the synthesizer.

    The dispatcher must emit a no_structural_candidate_record with
    skipped_reason="missing_rca_card" and never invoke synthesize.
    """
    synth_invocations: list = []

    def _synth_should_not_run(*args, **kwargs):
        synth_invocations.append((args, kwargs))
        return ClusterSynthesisResult(proposal=None)

    cluster_no_rca = {
        "cluster_id": "H001",
        "question_ids": ["7now_delivery_analytics_space_gs_013"],
        "root_cause": "wrong_filter_condition",
        "rca_card": {},  # empty — ungrounded
    }
    ag = {
        "id": "AG_DECOMPOSED_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["7now_delivery_analytics_space_gs_013"],
    }
    drop = {
        "ag_id": "AG_DECOMPOSED_H001",
        "source_clusters": ["H001"],
        "root_causes": ("wrong_filter_condition",),
    }

    result = forced_synthesis_dispatch.dispatch_forced_structural_synthesis(
        ag=ag,
        iteration=1,
        l5_ag_drops=[drop],
        reflection_buffer=[],
        iter_source_clusters_by_id={"H001": cluster_no_rca},
        iter_rca_id_by_cluster={"H001": ""},
        w=MagicMock(),
        benchmarks=[],
        run_id="test-run",
        metadata_snapshot={
            "data_sources": {"tables": [], "metric_views": []},
        },
        catalog="",
        schema="",
        spark=None,
        lever_keys=[5],
        current_iter_inputs={},
        synthesize=_synth_should_not_run,
        ag_proposals_so_far=[],
    )

    assert not synth_invocations, (
        "Synthesizer must NOT be invoked on a cluster with no "
        "RCA card."
    )
    emitted_records = result.emitted_decision_records or []
    nsc_records = [
        r for r in emitted_records
        if r.get("metrics", {}).get("skipped_reason") == "missing_rca_card"
    ]
    assert nsc_records, (
        f"Expected at least one NSC record with "
        f"skipped_reason=missing_rca_card; got {emitted_records!r}"
    )


def test_grounded_cluster_proceeds_to_synthesis():
    """Negative control: a cluster with a non-empty rca_card must
    reach the synthesizer."""
    synth_invocations: list = []

    def _synth_runs(*args, **kwargs):
        synth_invocations.append((args, kwargs))
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=("plural_top_n",),
            skipped_reason="validate_afs_rejected",
        )

    cluster_with_rca = {
        "cluster_id": "H001",
        "question_ids": ["7now_delivery_analytics_space_gs_013"],
        "root_cause": "wrong_filter_condition",
        "rca_card": {"id": "rca-1", "root_cause_summary": "ok"},
    }
    ag = {
        "id": "AG_DECOMPOSED_H001",
        "source_cluster_ids": ["H001"],
        "affected_questions": ["7now_delivery_analytics_space_gs_013"],
    }
    drop = {
        "ag_id": "AG_DECOMPOSED_H001",
        "source_clusters": ["H001"],
        "root_causes": ("wrong_filter_condition",),
    }

    forced_synthesis_dispatch.dispatch_forced_structural_synthesis(
        ag=ag,
        iteration=1,
        l5_ag_drops=[drop],
        reflection_buffer=[],
        iter_source_clusters_by_id={"H001": cluster_with_rca},
        iter_rca_id_by_cluster={"H001": "rca-1"},
        w=MagicMock(),
        benchmarks=[],
        run_id="test-run",
        metadata_snapshot={
            "data_sources": {"tables": [], "metric_views": []},
        },
        catalog="",
        schema="",
        spark=None,
        lever_keys=[5],
        current_iter_inputs={},
        synthesize=_synth_runs,
        ag_proposals_so_far=[],
    )

    assert synth_invocations, (
        "Grounded cluster (rca_card present) must reach the synthesizer."
    )
