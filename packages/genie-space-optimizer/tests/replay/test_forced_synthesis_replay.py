"""Replay driver tests for L5 forced-structural-synthesis dispatch.

The driver loads an extended PHASE_A fixture (post-Phase 1 schema) and
calls ``dispatch_forced_structural_synthesis`` per iteration with a
stubbed ``synthesize`` callable. Tests verify both today's bug (label
divergence → zero dispatches) and the control case (aligned labels →
dispatch fires).

# Gate invariant — "Experiment 2 is the gate"
# ===========================================
# This test module is the offline gate for any plan that intends to fix
# the L5 forced-synthesis dispatch trapdoor (Plan A / Plan B from the
# 2026-05-16 trial-7 postmortem):
#
#   1. To land a Plan A fix:
#      - Edit ``optimization/forced_synthesis_dispatch.py`` to canonicalize
#        the label key (e.g., compare against BOTH ``asi_failure_type``
#        and ``root_cause``).
#      - Flip ``test_replay_label_divergence_visits_zero_clusters`` to
#        assert ``attempted_dispatches == (("H001", "wrong_aggregation"),)``.
#      - Verify ``test_replay_label_aligned_visits_cluster`` STILL PASSES.
#
#   2. To land a Plan B fix (rich synthesizer primary):
#      - Extend ``dispatch_forced_structural_synthesis`` so SQL-shape
#        clusters trigger rich synthesis BEFORE the gate-drop ledger
#        path runs.
#      - Add a new fixture (e.g., ``rich_synthesis_primary.json``) with
#        no ``lever5_gate_drops`` entry but a SQL-shape cluster, and a
#        test asserting dispatch reaches synthesize.
#
# No plan that intends to change L5 dispatch behavior should land
# without updating BOTH the test expectation here AND the fixture
# under ``tests/replay/fixtures/forced_synthesis/``. CI runs the replay
# tests on every PR; a behavior change with no fixture/test update will
# fail review.
"""
from __future__ import annotations

import json
from pathlib import Path


_FIXTURES_DIR = (
    Path(__file__).resolve().parent / "fixtures" / "forced_synthesis"
)


def _load_fixture(name: str) -> dict:
    return json.loads((_FIXTURES_DIR / f"{name}.json").read_text())


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


def test_replay_label_aligned_visits_cluster() -> None:
    """Control case — when labels are aligned, dispatch visits exactly
    one cluster and synthesize is reached. This pin protects against a
    regression where someone "fixes" the divergence by ALSO disabling
    the aligned path.
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_forced_synthesis_replay,
    )

    def _synthesize_success(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal={
                "example_question": "How many flights per route?",
                "example_sql": "SELECT route, COUNT(*) FROM flights GROUP BY route",
                "_archetype_name": "ordered_list_by_metric",
                "kit_id": "kit_h001",
                "target_qids": ["gs_009"],
                "rca_id": "rca_h001",
                "_cluster_id": "H001",
            },
            attempted_archetypes=("ordered_list_by_metric",),
            skipped_reason=None,
        )

    result = run_forced_synthesis_replay(
        fixture=_load_fixture("label_aligned_minimal"),
        synthesize=_synthesize_success,
    )
    assert result.fixture_id == "label_aligned_minimal"
    assert len(result.iterations) == 1
    iter1 = result.iterations[0]
    assert iter1.attempted_dispatches == (("H001", "wrong_aggregation"),)
    assert len(iter1.appended_proposals) == 1
    assert iter1.appended_proposals[0]["patch_type"] == "add_example_sql"
    assert (
        iter1.appended_proposals[0]["provenance"]["synthesis_source"]
        == "forced_lever5_drop"
    )


def test_replay_label_divergence_visits_cluster_after_fix() -> None:
    """PLAN A PART 1 GATE — asserts the divergent-labels case fires.

    Today (pre-fix): this test FAILS because the strict-equality lookup
    short-circuits. After ``cluster_failure_keys`` lands and the dispatch
    matches via set membership, this test PASSES.

    Synthesize is stubbed to return a viable proposal. The assertion
    chain proves the rich path was actually reached:

    1. attempted_dispatches contains the (H001, "wrong_aggregation") pair.
    2. appended_proposals contains exactly one add_example_sql.
    3. The forced proposal's provenance.synthesis_source is "forced_lever5_drop".
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_forced_synthesis_replay,
    )

    def _synthesize_success(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal={
                "example_question": "How many flights per route?",
                "example_sql": (
                    "SELECT route, COUNT(*) AS cnt "
                    "FROM flights GROUP BY route ORDER BY cnt DESC LIMIT 10"
                ),
                "_archetype_name": "ordered_list_by_metric",
                "kit_id": "kit_h001",
                "target_qids": ["gs_009"],
                "rca_id": "rca_h001",
                "_cluster_id": "H001",
            },
            attempted_archetypes=("ordered_list_by_metric",),
            skipped_reason=None,
        )

    result = run_forced_synthesis_replay(
        fixture=_load_fixture("label_divergence_minimal"),
        synthesize=_synthesize_success,
    )
    assert result.fixture_id == "label_divergence_minimal"
    assert len(result.iterations) == 1
    iter1 = result.iterations[0]
    assert iter1.attempted_dispatches == (("H001", "wrong_aggregation"),)
    assert len(iter1.appended_proposals) == 1
    proposal = iter1.appended_proposals[0]
    assert proposal["patch_type"] == "add_example_sql"
    assert proposal["provenance"]["synthesis_source"] == "forced_lever5_drop"
    assert proposal["provenance"]["drop_root_cause"] == "wrong_aggregation"


def test_replay_label_divergence_emits_nsc_when_synth_declines() -> None:
    """PLAN A PART 1 GATE (complement) — asserts the NO_STRUCTURAL_CANDIDATE
    branch is reachable after the fix.

    Same fixture; synthesize stub returns ``proposal=None``. The
    assertion chain:

    1. attempted_dispatches still contains (H001, "wrong_aggregation").
       (Dispatch ROUTED to the cluster — the fix's job.)
    2. emitted_decision_records contains one NO_STRUCTURAL_CANDIDATE record.
    3. appended_proposals is empty.

    This proves the fix only changes the cluster-LOOKUP step; downstream
    behavior (decline → emit NSC) is unchanged.
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_forced_synthesis_replay,
    )

    def _synthesize_decline(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=("ordered_list_by_metric",),
            skipped_reason="no_viable_archetype",
        )

    result = run_forced_synthesis_replay(
        fixture=_load_fixture("label_divergence_minimal"),
        synthesize=_synthesize_decline,
    )
    iter1 = result.iterations[0]
    assert iter1.attempted_dispatches == (("H001", "wrong_aggregation"),)
    assert iter1.appended_proposals == ()
    assert len(iter1.emitted_decision_records) == 1
    nsc = iter1.emitted_decision_records[0]
    # ``decision_type`` is the PROPOSAL_GENERATED bucket; the NSC label
    # lives on ``reason_code``. See ``no_structural_candidate_record``.
    assert nsc.get("decision_type") == "proposal_generated"
    assert nsc.get("reason_code") == "no_structural_candidate"
    assert nsc.get("ag_id") == "AG_DECOMPOSED_H001"
    assert nsc.get("cluster_id") == "H001"
    assert nsc.get("root_cause") == "wrong_aggregation"


def test_replay_airline_iter5_h001_fires_dispatch_with_canonical_lookup() -> None:
    """PLAN A PART 1 GATE — exercises the real airline run shape.

    The airline iter-5 postmortem at
    docs/runid_analysis/59a173d3-f71f-4901-90ad-e10f1084cd7f/evidence/
    key_postmortem_facts_173220384276784.json shows a structural-gate
    drop with root_causes=["wrong_aggregation"] for AG_DECOMPOSED_H001
    targeting gs_009. Under the strict-equality bug the dispatch did not
    fire. After Part 1, it must.

    Variant A — synthesize returns a candidate → appended_proposals
    contains exactly one add_example_sql whose target_qids contain
    "airline_ticketing_and_fare_analysis_gs_009".
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_forced_synthesis_replay,
    )

    def _synthesize_success(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal={
                "example_question": (
                    "Show the route with the highest passenger count"
                ),
                "example_sql": (
                    "SELECT route, SUM(passengers) AS total_pax "
                    "FROM flights GROUP BY route "
                    "ORDER BY total_pax DESC LIMIT 1"
                ),
                "_archetype_name": "single_row_top_n",
                "kit_id": "kit_h001_airline",
                "target_qids": ["airline_ticketing_and_fare_analysis_gs_009"],
                "rca_id": "rca_h001_airline",
                "_cluster_id": "H001",
            },
            attempted_archetypes=("single_row_top_n",),
            skipped_reason=None,
        )

    result = run_forced_synthesis_replay(
        fixture=_load_fixture("airline_iter5_h001"),
        synthesize=_synthesize_success,
    )
    assert result.fixture_id == "airline_iter5_h001"
    assert len(result.iterations) == 1
    iter5 = result.iterations[0]
    assert iter5.iteration == 5
    assert iter5.ag_id == "AG_DECOMPOSED_H001"
    assert iter5.attempted_dispatches == (("H001", "wrong_aggregation"),)
    assert len(iter5.appended_proposals) == 1
    proposal = iter5.appended_proposals[0]
    assert proposal["patch_type"] == "add_example_sql"
    assert (
        "airline_ticketing_and_fare_analysis_gs_009"
        in (proposal.get("target_qids") or [])
    )
    assert proposal["provenance"]["synthesis_source"] == "forced_lever5_drop"


def test_replay_airline_iter5_h001_emits_nsc_when_synth_declines() -> None:
    """Variant B — synthesize declines → emitted_decision_records
    contains exactly one NO_STRUCTURAL_CANDIDATE record (which the
    harness turns into the GSO_NO_STRUCTURAL_CANDIDATE_V1 stdout
    marker).
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_forced_synthesis_replay,
    )

    def _synthesize_decline(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=("single_row_top_n", "ordered_list_by_metric"),
            skipped_reason="archetypes_exhausted",
        )

    result = run_forced_synthesis_replay(
        fixture=_load_fixture("airline_iter5_h001"),
        synthesize=_synthesize_decline,
    )
    iter5 = result.iterations[0]
    assert iter5.attempted_dispatches == (("H001", "wrong_aggregation"),)
    assert iter5.appended_proposals == ()
    assert len(iter5.emitted_decision_records) == 1
    nsc = iter5.emitted_decision_records[0]
    assert nsc.get("decision_type") == "proposal_generated"
    assert nsc.get("reason_code") == "no_structural_candidate"
    assert nsc.get("ag_id") == "AG_DECOMPOSED_H001"
    assert nsc.get("cluster_id") == "H001"
    assert nsc.get("root_cause") == "wrong_aggregation"
    assert (
        "airline_ticketing_and_fare_analysis_gs_009"
        in (nsc.get("target_qids") or ())
    )
