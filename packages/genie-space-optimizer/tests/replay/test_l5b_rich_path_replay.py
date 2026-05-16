"""Replay-driven validation for Plan B's Stage-2 L5b rich-path routing.

Each test loads a fixture, monkeypatches the rich synthesizer, runs the
replay driver, and asserts the user-spec invariant:

  "GSO_NO_STRUCTURAL_CANDIDATE_V1 markers with non-empty
  attempted_archetypes for any iteration where lever 5 returns no
  proposals — i.e., we trade a silent skip for a noisy 'we tried,
  here's what we tried.'"
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "forced_synthesis"


def _load_fixture(name: str) -> dict[str, Any]:
    with (_FIXTURE_DIR / f"{name}.json").open() as fp:
        return json.load(fp)


def test_l5b_rich_path_replay_driver_smoke(monkeypatch: Any) -> None:
    """Driver smoke test on an in-memory fixture with one SQL-shape
    cluster whose rich synthesis succeeds."""
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_l5b_rich_path_replay,
    )

    fixture = {
        "fixture_id": "in_memory_l5b_smoke",
        "iterations": [{
            "iteration": 1,
            "strategist_response": {
                "action_groups": [{
                    "id": "AG_C1",
                    "affected_questions": ["q1"],
                    "source_cluster_ids": ["C1"],
                    "patches": [],
                }],
            },
            "clusters": [{
                "cluster_id": "C1",
                "root_cause": "plural_top_n_collapse",
                "asi_failure_type": "wrong_aggregation",
                "question_ids": ["q1"],
            }],
            "iter_source_clusters_by_id": {
                "C1": {
                    "cluster_id": "C1",
                    "root_cause": "plural_top_n_collapse",
                    "asi_failure_type": "wrong_aggregation",
                    "question_ids": ["q1"],
                },
            },
            "iter_rca_id_by_cluster": {"C1": "rca_c1"},
            "metadata_failure_clusters": [],
            "lever5_gate_drops": [],
        }],
    }

    def _synth_success(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal={
                "example_question": "Top route",
                "example_sql": "SELECT route FROM flights LIMIT 1",
                "_archetype_name": "single_row_top_n",
                "target_qids": ["q1"],
                "rca_id": "rca_c1",
                "_cluster_id": "C1",
            },
            attempted_archetypes=("single_row_top_n",),
            skipped_reason=None,
        )

    result = run_l5b_rich_path_replay(
        fixture=fixture,
        synthesize=_synth_success,
    )
    assert result.fixture_id == "in_memory_l5b_smoke"
    iter1 = result.iterations[0]
    assert len(iter1.l5b_proposals) == 1
    assert iter1.l5b_proposals[0]["example_sql"].startswith("SELECT route")
    assert iter1.l5b_rich_path_declines == ()


def test_replay_airline_iter5_l5b_rich_path_emits_nsc_on_decline(monkeypatch: Any) -> None:
    """PLAN B GATE — airline iter-5 case.

    The rich synthesizer is stubbed to decline. The driver must emit one
    decline ledger entry with non-empty attempted_archetypes — the noisy
    NO_STRUCTURAL_CANDIDATE marker required by the user spec.
    """
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_l5b_rich_path_replay,
    )

    def _synth_decline(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=("single_row_top_n", "ordered_list_by_metric"),
            skipped_reason="archetypes_exhausted",
        )

    result = run_l5b_rich_path_replay(
        fixture=_load_fixture("airline_iter5_l5b_rich_path"),
        synthesize=_synth_decline,
    )
    assert result.fixture_id == "airline_iter5_l5b_rich_path"
    iter5 = result.iterations[0]
    assert iter5.iteration == 5
    assert iter5.l5b_proposals == ()
    assert len(iter5.l5b_rich_path_declines) == 1
    decline = iter5.l5b_rich_path_declines[0]
    assert decline["cluster_id"] == "H001"
    assert decline["attempted_archetypes"] == (
        "single_row_top_n", "ordered_list_by_metric",
    )
    assert decline["skipped_reason"] == "archetypes_exhausted"
    assert len(decline["attempted_archetypes"]) >= 1


def test_replay_airline_iter5_l5b_rich_path_emits_proposal_on_success(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_l5b_rich_path_replay,
    )

    def _synth_success(cluster_arg, metadata_arg, **kwargs):
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
                "parameters": [],
                "usage_guidance": "Use when ranking routes by passengers.",
                "_archetype_name": "single_row_top_n",
            },
            attempted_archetypes=("single_row_top_n",),
            skipped_reason=None,
        )

    result = run_l5b_rich_path_replay(
        fixture=_load_fixture("airline_iter5_l5b_rich_path"),
        synthesize=_synth_success,
    )
    iter5 = result.iterations[0]
    assert len(iter5.l5b_proposals) == 1
    proposal = iter5.l5b_proposals[0]
    assert "highest passenger count" in proposal["example_question"]
    assert iter5.l5b_rich_path_declines == ()


def test_replay_seven_now_iter1_l5b_rich_path_emits_nsc(monkeypatch: Any) -> None:
    """PLAN B GATE — 7now iter-1 case."""
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_l5b_rich_path_replay,
    )

    def _synth_decline(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=("single_row_top_n",),
            skipped_reason="no_viable_archetype",
        )

    result = run_l5b_rich_path_replay(
        fixture=_load_fixture("seven_now_iter1_l5b_rich_path"),
        synthesize=_synth_decline,
    )
    iter1 = result.iterations[0]
    assert iter1.iteration == 1
    assert iter1.l5b_proposals == ()
    assert len(iter1.l5b_rich_path_declines) == 1
    decline = iter1.l5b_rich_path_declines[0]
    assert decline["cluster_id"] == "H002"
    assert decline["attempted_archetypes"] == ("single_row_top_n",)
    assert decline["skipped_reason"] == "no_viable_archetype"


def test_replay_flag_off_falls_back_to_lean(monkeypatch: Any) -> None:
    """When the flag is OFF, the replay driver routes to the lean path
    even on SQL-shape clusters."""
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "0")
    from genie_space_optimizer.optimization.forced_synthesis_replay import (
        run_l5b_rich_path_replay,
    )

    def _synth_must_not_run(*args, **kwargs):
        raise AssertionError(
            "rich synthesizer MUST NOT be called when flag is OFF"
        )

    def _lean_stub(*args, **kwargs):
        return {
            "example_question": "Q_lean",
            "example_sql": "SELECT 1",
            "parameters": [],
            "usage_guidance": "G",
        }
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.synthesis.synthesize_example_sqls",
        _lean_stub,
    )

    result = run_l5b_rich_path_replay(
        fixture=_load_fixture("airline_iter5_l5b_rich_path"),
        synthesize=_synth_must_not_run,
    )
    iter5 = result.iterations[0]
    assert len(iter5.l5b_proposals) == 1
    assert iter5.l5b_proposals[0]["example_question"] == "Q_lean"
    assert iter5.l5b_rich_path_declines == ()
