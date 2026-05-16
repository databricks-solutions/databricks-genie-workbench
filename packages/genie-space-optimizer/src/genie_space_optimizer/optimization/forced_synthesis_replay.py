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
