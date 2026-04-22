"""Per-iteration acceptance predicate + held-out semantics helpers.

Phase 4 of Bug #4 plan. The user-visible goal is:

* Start with a fixed benchmark corpus (~30 questions, ~5 held out).
* Measure baseline_accuracy.
* Run N iterations of patches.
* Measure final_accuracy on the same corpus.
* Report ``final - baseline``.

Each iteration must:

1. Full-training eval (~25 questions). Pre/post eval results are used
   to compute cluster attestation as a SLICE (not a separate eval).
2. Accept iteration iff::
       cluster_net_delta        >= MIN_NET_DELTA         (default 1)
       out_of_cluster_newly_failing <= OUT_OF_CLUSTER_REGRESSION_TOLERANCE (default 0)
   Otherwise roll back all patches applied in the iteration.

This module owns the pure decision logic so the harness can stay a thin
orchestrator. The helpers here DO NOT:

* read from Delta or call the Genie API,
* invoke any LLM,
* mutate global state.

All inputs are plain dicts; all outputs are plain dataclasses. That makes
every branch trivially unit-testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from genie_space_optimizer.common.config import (
    ITERATION_ACCEPTANCE_ENABLED,
    MIN_NET_DELTA,
    OUT_OF_CLUSTER_REGRESSION_TOLERANCE,
)


# ── Data shapes ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class QuestionPassMap:
    """Per-question pass/fail from a full-training eval. The key is the
    benchmark ``question_id``; the value is True if the question passed."""
    passes: dict[str, bool] = field(default_factory=dict)

    def passed(self, qid: str) -> bool:
        return bool(self.passes.get(qid, False))

    def pass_rate(self) -> float:
        if not self.passes:
            return 0.0
        return sum(1 for v in self.passes.values() if v) / len(self.passes)

    def __len__(self) -> int:
        return len(self.passes)


@dataclass(frozen=True)
class ClusterAttestation:
    """Cluster-level attestation derived from a (pre, post) eval pair.

    ``net_delta`` is the plan's acceptance signal. ``newly_passing`` and
    ``newly_failing`` count per-question transitions restricted to the
    cluster's own question_ids.
    """
    cluster_id: str
    target_qids: tuple[str, ...]
    newly_passing: int
    newly_failing: int
    pre_passes: dict[str, bool]
    post_passes: dict[str, bool]

    @property
    def net_delta(self) -> int:
        return self.newly_passing - self.newly_failing


@dataclass(frozen=True)
class IterationAcceptanceResult:
    """Structured output of :func:`decide_iteration_acceptance`.

    ``accepted`` — whether the iteration's patches should stay applied.
    ``reason`` — short code explaining rejection (or "accepted").
    ``attestation`` — cluster-level metrics.
    ``out_of_cluster_newly_failing`` — qids OUTSIDE the cluster that were
    passing pre-patch and are failing post-patch.
    """
    accepted: bool
    reason: str
    attestation: ClusterAttestation
    out_of_cluster_newly_failing: tuple[str, ...]


# ── Held-out semantics (P4.2) ──────────────────────────────────────────


class HeldOutLeakError(AssertionError):
    """Raised when an LLM-bound flow attempts to read held-out benchmarks.

    Caught by tests; in production this is a hard fail — it means a
    future change violated the "held-out is never seen by the LLM"
    contract. Fix the upstream code, do not downgrade the error."""


def assert_no_held_out_in_cluster_input(
    cluster_question_ids: Iterable[str], held_out_qids: set[str],
) -> None:
    """Guard for ``cluster_failures`` input. ``held_out_qids`` is the set
    of benchmark ids reserved for final-sweep evaluation only."""
    overlap = {qid for qid in cluster_question_ids if qid in held_out_qids}
    if overlap:
        raise HeldOutLeakError(
            f"Held-out benchmarks {sorted(overlap)} appeared in a cluster "
            "intended for LLM context. Held-out ids must be filtered before "
            "clustering (P4.2)."
        )


def filter_out_held_out(
    benchmarks: Iterable[dict], held_out_qids: set[str],
) -> list[dict]:
    """Return the subset of ``benchmarks`` whose ``id`` is not held-out.

    Intended for any call site that feeds benchmarks into an LLM-bound
    flow (cluster_failures, AFS rendering, synthesis). Preserves order.
    """
    out: list[dict] = []
    for b in benchmarks or []:
        if not isinstance(b, dict):
            continue
        qid = str(b.get("id") or b.get("question_id") or "")
        if qid in held_out_qids:
            continue
        out.append(b)
    return out


# ── Attestation derivation (P4.3) ──────────────────────────────────────


def derive_cluster_attestation(
    cluster_id: str,
    target_qids: Iterable[str],
    pre_iteration_passes: QuestionPassMap,
    post_iteration_passes: QuestionPassMap,
) -> ClusterAttestation:
    """Slice pre/post eval results to the cluster's qids and count
    transitions.

    Missing qids in either map are treated as False (conservative — a
    question with unknown state cannot be counted as passing).
    """
    qids_tuple = tuple(target_qids)
    pre_slice = {q: pre_iteration_passes.passed(q) for q in qids_tuple}
    post_slice = {q: post_iteration_passes.passed(q) for q in qids_tuple}
    newly_passing = sum(
        1 for q in qids_tuple if not pre_slice[q] and post_slice[q]
    )
    newly_failing = sum(
        1 for q in qids_tuple if pre_slice[q] and not post_slice[q]
    )
    return ClusterAttestation(
        cluster_id=cluster_id,
        target_qids=qids_tuple,
        newly_passing=newly_passing,
        newly_failing=newly_failing,
        pre_passes=pre_slice,
        post_passes=post_slice,
    )


def count_out_of_cluster_regressions(
    target_qids: Iterable[str],
    pre_iteration_passes: QuestionPassMap,
    post_iteration_passes: QuestionPassMap,
) -> tuple[str, ...]:
    """Return qids that were passing pre-iteration and failing post-
    iteration, EXCLUDING the target cluster's qids."""
    target_set = set(target_qids)
    regressed: list[str] = []
    for qid, was_pass in pre_iteration_passes.passes.items():
        if qid in target_set:
            continue
        if was_pass and not post_iteration_passes.passed(qid):
            regressed.append(qid)
    return tuple(regressed)


# ── Decision (P4.4) ────────────────────────────────────────────────────


def decide_iteration_acceptance(
    cluster_id: str,
    target_qids: Iterable[str],
    pre_iteration_passes: QuestionPassMap,
    post_iteration_passes: QuestionPassMap,
    *,
    min_net_delta: int | None = None,
    out_of_cluster_tolerance: int | None = None,
    enabled: bool | None = None,
) -> IterationAcceptanceResult:
    """Apply the P4.4 acceptance predicate.

    Returns an ``IterationAcceptanceResult``; the caller (harness) is
    responsible for performing the actual rollback when ``accepted ==
    False``. Separating the decision from the side effect keeps this
    logic unit-testable without Delta / WorkspaceClient mocks.
    """
    if enabled is None:
        enabled = ITERATION_ACCEPTANCE_ENABLED
    if min_net_delta is None:
        min_net_delta = MIN_NET_DELTA
    if out_of_cluster_tolerance is None:
        out_of_cluster_tolerance = OUT_OF_CLUSTER_REGRESSION_TOLERANCE

    attestation = derive_cluster_attestation(
        cluster_id, target_qids, pre_iteration_passes, post_iteration_passes,
    )
    ooc = count_out_of_cluster_regressions(
        target_qids, pre_iteration_passes, post_iteration_passes,
    )

    if not enabled:
        # Metrics are still computed, but we never refuse. Useful for
        # forensics runs where we want to see what *would* have been
        # rolled back without actually rolling back.
        return IterationAcceptanceResult(
            accepted=True,
            reason="acceptance_disabled",
            attestation=attestation,
            out_of_cluster_newly_failing=ooc,
        )

    if attestation.net_delta < min_net_delta:
        return IterationAcceptanceResult(
            accepted=False,
            reason=f"cluster_net_delta_below_min ({attestation.net_delta} < {min_net_delta})",
            attestation=attestation,
            out_of_cluster_newly_failing=ooc,
        )

    if len(ooc) > out_of_cluster_tolerance:
        return IterationAcceptanceResult(
            accepted=False,
            reason=f"out_of_cluster_regression ({len(ooc)} > {out_of_cluster_tolerance})",
            attestation=attestation,
            out_of_cluster_newly_failing=ooc,
        )

    return IterationAcceptanceResult(
        accepted=True,
        reason="accepted",
        attestation=attestation,
        out_of_cluster_newly_failing=ooc,
    )


# ── Baseline + finalize attestation (P4.5) ─────────────────────────────


@dataclass(frozen=True)
class CorpusSweepResult:
    """One full-corpus sweep — baseline at run start, or final at finalize."""
    train_pass_rate: float
    heldout_pass_rate: float
    train_passes: dict[str, bool]
    heldout_passes: dict[str, bool]

    def delta_vs(self, other: "CorpusSweepResult") -> dict[str, float]:
        """Pass-rate deltas (final - baseline), percentage points."""
        return {
            "train_delta_pct": (self.train_pass_rate - other.train_pass_rate) * 100.0,
            "heldout_delta_pct": (self.heldout_pass_rate - other.heldout_pass_rate) * 100.0,
        }


def build_corpus_sweep_result(
    train_passes: dict[str, bool], heldout_passes: dict[str, bool],
) -> CorpusSweepResult:
    def _rate(d: dict[str, bool]) -> float:
        if not d:
            return 0.0
        return sum(1 for v in d.values() if v) / len(d)

    return CorpusSweepResult(
        train_pass_rate=_rate(train_passes),
        heldout_pass_rate=_rate(heldout_passes),
        train_passes=dict(train_passes),
        heldout_passes=dict(heldout_passes),
    )


def improvement_summary(
    baseline: CorpusSweepResult, final: CorpusSweepResult,
) -> dict[str, object]:
    """Human-readable before/after summary — the user-visible story."""
    total_base = (
        sum(1 for v in baseline.train_passes.values() if v)
        + sum(1 for v in baseline.heldout_passes.values() if v)
    )
    total_final = (
        sum(1 for v in final.train_passes.values() if v)
        + sum(1 for v in final.heldout_passes.values() if v)
    )
    total_qs = (
        len(baseline.train_passes) + len(baseline.heldout_passes)
    )
    return {
        "baseline_total_passed": total_base,
        "final_total_passed": total_final,
        "total_questions": total_qs,
        "improvement_questions": total_final - total_base,
        "baseline_train_pass_rate": baseline.train_pass_rate,
        "final_train_pass_rate": final.train_pass_rate,
        "baseline_heldout_pass_rate": baseline.heldout_pass_rate,
        "final_heldout_pass_rate": final.heldout_pass_rate,
        "deltas_pct": final.delta_vs(baseline),
    }
