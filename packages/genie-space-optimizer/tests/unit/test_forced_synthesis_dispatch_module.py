"""Unit tests for the extracted L5 forced-synthesis dispatch callable.

This module pins the contract the replay driver depends on:

1. ``ForcedSynthesisDispatchResult`` is a frozen dataclass with the three
   fields the replay driver reads.
2. ``dispatch_forced_structural_synthesis`` is importable from the new
   module and accepts the parameter list pinned in Task 0.

Behavior parity with ``harness.py:22720-22929`` is verified by the
parity-pin test in Task 4, not here.
"""
from __future__ import annotations


def test_result_dataclass_shape() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        ForcedSynthesisDispatchResult,
    )

    r = ForcedSynthesisDispatchResult(
        attempted_dispatches=(),
        appended_proposals=(),
        emitted_decision_records=(),
    )
    assert r.attempted_dispatches == ()
    assert r.appended_proposals == ()
    assert r.emitted_decision_records == ()


def test_dispatch_function_callable_with_empty_inputs() -> None:
    """When no L5 drops are present, dispatch returns an empty result."""
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        ForcedSynthesisDispatchResult,
        dispatch_forced_structural_synthesis,
    )

    def _synthesize_stub(*args, **kwargs):  # noqa: ARG001
        raise AssertionError(
            "synthesize must not be called when no L5 drops are present"
        )

    result = dispatch_forced_structural_synthesis(
        run_id="test_run",
        iteration=1,
        ag={"id": "AG_TEST", "affected_questions": ["gs_001"]},
        l5_ag_drops=[],
        iter_source_clusters_by_id={},
        iter_rca_id_by_cluster={},
        metadata_snapshot={},
        benchmarks=[],
        catalog="",
        schema="",
        w=None,
        spark=None,
        lever_keys=(),
        reflection_buffer=(),
        current_iter_inputs={},
        synthesize=_synthesize_stub,
    )
    assert isinstance(result, ForcedSynthesisDispatchResult)
    assert result.attempted_dispatches == ()
    assert result.appended_proposals == ()
    assert result.emitted_decision_records == ()


def test_dispatch_visits_matching_cluster_when_labels_aligned() -> None:
    """When ``cluster.root_cause`` equals the drop's ``root_causes[0]``,
    the dispatch loop reaches synthesize and emits the proposal.

    This pins today's behavior for the (aligned-labels) control case.
    The (divergent-labels) bug case is pinned in Task 4's parity test,
    not here.
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        dispatch_forced_structural_synthesis,
    )

    cluster = {
        "cluster_id": "H001",
        "root_cause": "wrong_aggregation",
        "question_ids": ["gs_009"],
        "asi_failure_type": "wrong_aggregation",
    }
    drop = {
        "ag_id": "AG_DECOMPOSED_H001",
        "source_clusters": ("H001",),
        "root_causes": ("wrong_aggregation",),
        "target_lever": 5,
        "had_example_sqls": False,
        "instruction_sections_dropped": True,
        "instruction_guidance_dropped": False,
    }

    def _synthesize_stub(cluster_arg, metadata_arg, **kwargs):  # noqa: ARG001
        return ClusterSynthesisResult(
            proposal={
                "example_question": "How many flights per route?",
                "example_sql": "SELECT route, COUNT(*) FROM flights GROUP BY route",
                "_archetype_name": "ordered_list_by_metric",
                "kit_id": "test_kit",
                "target_qids": ["gs_009"],
                "rca_id": "rca_h001",
                "_cluster_id": "H001",
            },
            attempted_archetypes=("ordered_list_by_metric",),
            skipped_reason=None,
        )

    result = dispatch_forced_structural_synthesis(
        run_id="test_run",
        iteration=1,
        ag={
            "id": "AG_DECOMPOSED_H001",
            "affected_questions": ["gs_009"],
            "source_cluster_ids": ["H001"],
        },
        l5_ag_drops=[drop],
        iter_source_clusters_by_id={"H001": cluster},
        iter_rca_id_by_cluster={"H001": "rca_h001"},
        metadata_snapshot={"_space_id": "test_space"},
        benchmarks=[],
        catalog="",
        schema="",
        w=None,
        spark=None,
        lever_keys=(5,),
        reflection_buffer=(),
        current_iter_inputs={},
        synthesize=_synthesize_stub,
    )
    assert result.attempted_dispatches == (("H001", "wrong_aggregation"),)
    assert len(result.appended_proposals) == 1
    assert result.appended_proposals[0]["patch_type"] == "add_example_sql"
    assert result.emitted_decision_records == ()


def test_label_divergence_visits_cluster_after_canonicalization() -> None:
    """Plan A Part 1 — replaces the legacy ``short_circuits_dispatch`` pin.

    With ``cluster_failure_keys`` canonicalizing the lookup, a cluster
    whose RcaKind ``root_cause`` differs from its ``asi_failure_type``
    is still visited by the dispatch loop. Synthesize is called exactly
    once.
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        dispatch_forced_structural_synthesis,
    )

    cluster_divergent = {
        "cluster_id": "H001",
        "root_cause": "plural_top_n_collapse",
        "question_ids": ["gs_009"],
        "asi_failure_type": "wrong_aggregation",
    }
    drop = {
        "ag_id": "AG_DECOMPOSED_H001",
        "source_clusters": ("H001",),
        "root_causes": ("wrong_aggregation",),
        "target_lever": 5,
        "had_example_sqls": False,
        "instruction_sections_dropped": True,
        "instruction_guidance_dropped": False,
    }

    synthesize_call_count = {"n": 0}

    def _synthesize_success(cluster_arg, metadata_arg, **kwargs):
        synthesize_call_count["n"] += 1
        return ClusterSynthesisResult(
            proposal={
                "example_question": "test",
                "example_sql": "SELECT 1",
                "_archetype_name": "ordered_list_by_metric",
                "kit_id": "kit_h001",
                "target_qids": ["gs_009"],
                "rca_id": "rca_h001",
                "_cluster_id": "H001",
            },
            attempted_archetypes=("ordered_list_by_metric",),
            skipped_reason=None,
        )

    result = dispatch_forced_structural_synthesis(
        run_id="test_run",
        iteration=1,
        ag={
            "id": "AG_DECOMPOSED_H001",
            "affected_questions": ["gs_009"],
            "source_cluster_ids": ["H001"],
        },
        l5_ag_drops=[drop],
        iter_source_clusters_by_id={"H001": cluster_divergent},
        iter_rca_id_by_cluster={"H001": "rca_h001"},
        metadata_snapshot={"_space_id": "test_space"},
        benchmarks=[],
        catalog="",
        schema="",
        w=None,
        spark=None,
        lever_keys=(5,),
        reflection_buffer=(),
        current_iter_inputs={},
        synthesize=_synthesize_success,
    )
    assert result.attempted_dispatches == (("H001", "wrong_aggregation"),)
    assert len(result.appended_proposals) == 1
    assert synthesize_call_count["n"] == 1


def test_dispatch_accepts_ag_proposals_so_far_kwarg() -> None:
    """Phase 2 — dispatch admits ``ag_proposals_so_far`` (defaults to ()).
    The kwarg is read in Task 9; for now we verify the signature accepts
    it without breaking existing callers.
    """
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        dispatch_forced_structural_synthesis,
    )

    def _synthesize_must_not_run(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("no inputs — should not be called")

    result = dispatch_forced_structural_synthesis(
        run_id="test_run",
        iteration=1,
        ag={"id": "AG_TEST", "affected_questions": [], "source_cluster_ids": []},
        l5_ag_drops=[],
        iter_source_clusters_by_id={},
        iter_rca_id_by_cluster={},
        metadata_snapshot={},
        benchmarks=[],
        catalog="",
        schema="",
        w=None,
        spark=None,
        lever_keys=(),
        reflection_buffer=(),
        current_iter_inputs={},
        ag_proposals_so_far=[],
        synthesize=_synthesize_must_not_run,
    )
    assert result.attempted_dispatches == ()
    assert result.appended_proposals == ()
    assert result.emitted_decision_records == ()
