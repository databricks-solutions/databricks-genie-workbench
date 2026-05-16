"""Offline replay driver for L5 forced-structural-synthesis dispatch.

Loads an extended PHASE_A fixture (one that includes the
``lever5_gate_drops``, ``iter_source_clusters_by_id``,
``iter_rca_id_by_cluster``, and ``metadata_failure_clusters`` keys
emitted by harness Task 7) and replays the dispatch per iteration via
``optimization.forced_synthesis_dispatch.dispatch_forced_structural_synthesis``.

The default ``synthesize`` stub returns a canned ``ClusterSynthesisResult``
that signals "no candidate", so the replay measures DISPATCH BEHAVIOR
(was the synthesizer reached at all?) rather than synthesis quality.
Tests can pass alternative stubs to model successful synthesis.

This module is pure: no Databricks, Spark, MLflow, or LLM calls.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class IterationReplay:
    """Per-iteration replay outcome."""
    iteration: int
    ag_id: str
    attempted_dispatches: tuple[tuple[str, str], ...]
    appended_proposals: tuple[dict[str, Any], ...]
    emitted_decision_records: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class ForcedSynthesisReplayResult:
    """Result of replaying a full PHASE_A fixture through dispatch."""
    fixture_id: str
    iterations: tuple[IterationReplay, ...]


def _default_synthesize_stub(cluster, metadata_snapshot, **kwargs):
    """Default ``synthesize`` stub — returns ``ClusterSynthesisResult``
    with ``proposal=None`` so the replay measures dispatch reachability,
    not synthesis quality. Tests can override.
    """
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )

    return ClusterSynthesisResult(
        proposal=None,
        attempted_archetypes=(),
        skipped_reason="replay_stub_default",
    )


def run_forced_synthesis_replay(
    *,
    fixture: dict,
    synthesize: Callable[..., Any] | None = None,
) -> ForcedSynthesisReplayResult:
    """Replay every iteration's L5 dispatch against the fixture.

    For each iteration's action_group, build the parameter bag for
    ``dispatch_forced_structural_synthesis`` from the fixture's snapshot
    keys and call dispatch with the provided ``synthesize`` stub
    (defaulting to ``_default_synthesize_stub``).

    The returned ``ForcedSynthesisReplayResult.iterations`` contains one
    ``IterationReplay`` entry per (iteration, ag_id) pair processed.
    """
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        dispatch_forced_structural_synthesis,
    )

    synth = synthesize or _default_synthesize_stub
    fixture_id = str(fixture.get("fixture_id") or "")
    iters: list[IterationReplay] = []
    for it in (fixture.get("iterations") or []):
        iteration = int(it.get("iteration") or 0)
        l5_drops_all = list(it.get("lever5_gate_drops") or [])
        src_clusters = dict(it.get("iter_source_clusters_by_id") or {})
        rca_id_by_cluster = dict(it.get("iter_rca_id_by_cluster") or {})
        metadata_failure_clusters = list(it.get("metadata_failure_clusters") or [])
        metadata_snapshot = {
            "_failure_clusters": metadata_failure_clusters,
            # The dispatch reads metadata_snapshot through to synthesize;
            # synthesize is stubbed, so we only need to preserve the
            # shape that the dispatch itself does NOT read.
            "_space_id": "replay_space",
        }
        ags = (
            (it.get("strategist_response") or {}).get("action_groups") or []
        )
        for ag in ags:
            ag_id = str(ag.get("id") or "")
            l5_ag_drops = [
                d for d in l5_drops_all
                if str(d.get("ag_id") or "") == ag_id
            ]
            result = dispatch_forced_structural_synthesis(
                run_id=fixture_id,
                iteration=iteration,
                ag=ag,
                l5_ag_drops=l5_ag_drops,
                iter_source_clusters_by_id=src_clusters,
                iter_rca_id_by_cluster=rca_id_by_cluster,
                metadata_snapshot=metadata_snapshot,
                benchmarks=[],
                catalog="",
                schema="",
                w=None,
                spark=None,
                lever_keys=(5,),
                reflection_buffer=(),
                current_iter_inputs={},
                # Plan A Part 2 — pass the AG's patch slate so the
                # safety-net predicate can detect "lever 5 emitted zero
                # proposals". Reads patches[*].patch_type from the
                # fixture, since the journey fixture exporter already
                # captures patch_type (per _ALLOWED_PATCH_KEYS).
                ag_proposals_so_far=list(ag.get("patches") or []),
                synthesize=synth,
            )
            iters.append(IterationReplay(
                iteration=iteration,
                ag_id=ag_id,
                attempted_dispatches=result.attempted_dispatches,
                appended_proposals=result.appended_proposals,
                emitted_decision_records=result.emitted_decision_records,
            ))
    return ForcedSynthesisReplayResult(
        fixture_id=fixture_id,
        iterations=tuple(iters),
    )
