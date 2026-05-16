"""Replay driver tests for L5 forced-structural-synthesis dispatch.

The driver loads an extended PHASE_A fixture (post-Phase 1 schema) and
calls ``dispatch_forced_structural_synthesis`` per iteration with a
stubbed ``synthesize`` callable. Tests verify both today's bug (label
divergence → zero dispatches) and the control case (aligned labels →
dispatch fires).
"""
from __future__ import annotations

import json
from pathlib import Path


def test_replay_result_dataclass_shape() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        ForcedSynthesisReplayResult,
        IterationReplay,
    )

    r = ForcedSynthesisReplayResult(
        fixture_id="test",
        iterations=(
            IterationReplay(
                iteration=1,
                ag_id="AG_TEST",
                attempted_dispatches=(),
                appended_proposals=(),
                emitted_decision_records=(),
            ),
        ),
    )
    assert r.fixture_id == "test"
    assert r.iterations[0].iteration == 1
    assert r.iterations[0].ag_id == "AG_TEST"


def _label_aligned_fixture() -> dict:
    """Minimal aligned-labels fixture: cluster.root_cause == drop root_cause."""
    return {
        "fixture_id": "label_aligned_minimal",
        "iterations": [{
            "iteration": 1,
            "strategist_response": {
                "action_groups": [{
                    "id": "AG_DECOMPOSED_H001",
                    "affected_questions": ["gs_009"],
                    "source_cluster_ids": ["H001"],
                    "patches": [],
                }],
            },
            "clusters": [{
                "cluster_id": "H001",
                "root_cause": "wrong_aggregation",
                "asi_failure_type": "wrong_aggregation",
                "question_ids": ["gs_009"],
            }],
            "iter_source_clusters_by_id": {
                "H001": {
                    "cluster_id": "H001",
                    "root_cause": "wrong_aggregation",
                    "asi_failure_type": "wrong_aggregation",
                    "question_ids": ["gs_009"],
                },
            },
            "iter_rca_id_by_cluster": {"H001": "rca_h001"},
            "metadata_failure_clusters": [],
            "lever5_gate_drops": [{
                "ag_id": "AG_DECOMPOSED_H001",
                "source_clusters": ["H001"],
                "root_causes": ["wrong_aggregation"],
                "target_lever": 5,
                "had_example_sqls": False,
                "instruction_sections_dropped": True,
                "instruction_guidance_dropped": False,
            }],
        }],
    }


def test_replay_aligned_labels_visits_cluster() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_forced_synthesis_replay,
    )

    def _synthesize_success(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal={
                "example_question": "test",
                "example_sql": "SELECT 1",
                "_archetype_name": "ordered_list_by_metric",
                "kit_id": "test",
                "target_qids": ["gs_009"],
                "rca_id": "rca_h001",
                "_cluster_id": "H001",
            },
            attempted_archetypes=("ordered_list_by_metric",),
            skipped_reason=None,
        )

    result = run_forced_synthesis_replay(
        fixture=_label_aligned_fixture(),
        synthesize=_synthesize_success,
    )
    assert result.fixture_id == "label_aligned_minimal"
    assert len(result.iterations) == 1
    iter1 = result.iterations[0]
    assert iter1.ag_id == "AG_DECOMPOSED_H001"
    assert iter1.attempted_dispatches == (("H001", "wrong_aggregation"),)
    assert len(iter1.appended_proposals) == 1
    assert iter1.appended_proposals[0]["patch_type"] == "add_example_sql"
