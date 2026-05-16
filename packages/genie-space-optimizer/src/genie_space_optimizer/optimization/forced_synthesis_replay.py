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

from collections.abc import Mapping
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


@dataclass(frozen=True)
class L5bIterationReplay:
    """One iteration's worth of L5b replay output (Plan B)."""

    iteration: int
    l5b_proposals: tuple[Mapping[str, Any], ...]
    l5b_rich_path_declines: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class L5bRichPathReplayResult:
    """Full result of a Plan B L5b rich-path replay."""

    fixture_id: str
    iterations: tuple[L5bIterationReplay, ...]


def run_l5b_rich_path_replay(
    *,
    fixture: Mapping[str, Any],
    synthesize: Callable[..., Any] | None = None,
) -> L5bRichPathReplayResult:
    """Plan B — replay the Stage-2 L5b dispatch path offline.

    For each iteration in ``fixture``, iterates each AG's source
    cluster, calls ``_dispatch_lever_5b_for_cluster`` (which routes
    through Plan B when the flag is on + cluster is SQL-shape), and
    captures both the returned proposals and any decline ledger
    entries.

    ``synthesize`` is the rich synthesizer (defaults to the production
    callable). Tests pass a stub. The lean-path
    ``synthesize_example_sqls`` is consumed via its module path; tests
    that want to assert flag-off behaviour monkeypatch it directly.

    Returns a frozen ``L5bRichPathReplayResult`` with per-iteration
    proposals + declines. The harness drain step
    (``_emit_l5b_rich_path_decline_records``) is NOT invoked here — the
    declines are surfaced raw so tests can pin the ledger contract
    directly.
    """
    from genie_space_optimizer.optimization.optimizer import (
        _dispatch_lever_5b_for_cluster,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        drain_l5b_rich_path_declines,
    )

    fixture_id = str(fixture.get("fixture_id") or "unknown")
    iterations_out: list[L5bIterationReplay] = []

    if synthesize is not None:
        import genie_space_optimizer.optimization.cluster_driven_synthesis as cds
        original_synth = (
            cds.run_cluster_driven_synthesis_for_single_cluster
        )
        cds.run_cluster_driven_synthesis_for_single_cluster = synthesize  # type: ignore[assignment]
    else:
        original_synth = None

    try:
        for iter_data in (fixture.get("iterations") or ()):
            iteration_num = int(iter_data.get("iteration", 0))
            iter_clusters = iter_data.get("iter_source_clusters_by_id") or {}
            drain_l5b_rich_path_declines()  # reset ledger per iteration

            proposals: list[Mapping[str, Any]] = []
            for ag in (iter_data.get("strategist_response") or {}).get(
                "action_groups", ()
            ):
                for cid in (ag.get("source_cluster_ids") or ()):
                    cluster = iter_clusters.get(str(cid)) or {}
                    if not cluster:
                        continue
                    out = _dispatch_lever_5b_for_cluster(
                        cluster=cluster,
                        metadata_snapshot={"_space_id": fixture_id},
                        w=None,
                        benchmark_corpus=None,
                        benchmarks=None,
                    )
                    proposals.extend(out)
            declines = drain_l5b_rich_path_declines()

            iterations_out.append(L5bIterationReplay(
                iteration=iteration_num,
                l5b_proposals=tuple(proposals),
                l5b_rich_path_declines=tuple(declines),
            ))
    finally:
        if original_synth is not None:
            import genie_space_optimizer.optimization.cluster_driven_synthesis as cds
            cds.run_cluster_driven_synthesis_for_single_cluster = original_synth  # type: ignore[assignment]

    return L5bRichPathReplayResult(
        fixture_id=fixture_id,
        iterations=tuple(iterations_out),
    )
