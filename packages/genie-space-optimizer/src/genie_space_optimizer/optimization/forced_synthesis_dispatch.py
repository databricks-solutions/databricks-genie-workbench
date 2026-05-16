"""Lever-5 forced-structural-synthesis dispatch — extracted from harness.py.

This module exists so the L5 forced-synthesis dispatch (formerly inline
at harness.py:22720-22929) is callable in isolation. The replay harness
calls this function with a stubbed ``synthesize`` callable to verify
dispatch behavior offline against frozen fixtures, without spinning up
the full optimizer.

The function preserves the EXACT behavior of the inline block — including
the label-divergence bug where ``_LEVER5_GATE_DROPS[*].root_causes`` (which
prefers ``asi_failure_type``) is compared with strict equality against
``cluster.root_cause`` (which is the RcaKind label). Fixing that bug is
Plan A's job, not this refactor's.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ForcedSynthesisDispatchResult:
    """Per-call result of ``dispatch_forced_structural_synthesis``.

    Fields:
        attempted_dispatches: tuple of ``(cluster_id, root_cause)`` pairs
            the dispatch loop actually visited (i.e., where
            ``_should_force_structural_synthesis`` returned True AND a
            matching cluster was found in ``iter_source_clusters_by_id``).
            Empty when the label-divergence bug short-circuits the loop.
        appended_proposals: tuple of forced ``add_example_sql`` proposal
            dicts produced by successful synthesis. The harness appends
            these to ``all_proposals`` at the call site.
        emitted_decision_records: tuple of ``DecisionRecord.to_dict()``
            outputs for ``NO_STRUCTURAL_CANDIDATE`` cases. The harness
            extends ``_current_iter_inputs["decision_records"]`` with
            these at the call site.

    Exceptions are NOT caught inside the dispatch function — the harness
    call site's existing outer try-except handles them (same shape as
    the original inline block). This preserves byte-stable exception
    accounting via ``_phase_b_producer_exceptions``.
    """
    attempted_dispatches: tuple[tuple[str, str], ...]
    appended_proposals: tuple[dict[str, Any], ...]
    emitted_decision_records: tuple[dict[str, Any], ...]


def dispatch_forced_structural_synthesis(
    *,
    run_id: str,
    iteration: int,
    ag: Mapping[str, Any],
    l5_ag_drops: Sequence[Mapping[str, Any]],
    iter_source_clusters_by_id: Mapping[str, Mapping[str, Any]],
    iter_rca_id_by_cluster: Mapping[str, str],
    metadata_snapshot: Mapping[str, Any],
    benchmarks: Sequence[Mapping[str, Any]],
    catalog: str,
    schema: str,
    w: Any,
    spark: Any,
    lever_keys: Iterable[int],
    reflection_buffer: Sequence[Any],
    current_iter_inputs: dict[str, Any],
    synthesize: Callable[..., Any] | None = None,
) -> ForcedSynthesisDispatchResult:
    """Run the L5 forced-structural-synthesis dispatch for one AG.

    Parameters mirror the closure-of-locals pinned in Task 0. The
    ``synthesize`` parameter defaults to
    ``run_cluster_driven_synthesis_for_single_cluster`` from
    ``cluster_driven_synthesis`` (resolved lazily inside the function
    to avoid circular imports); tests pass a stub.

    Returns a ``ForcedSynthesisDispatchResult`` instead of mutating the
    caller's locals directly. The harness call site applies the side
    effects (append to ``all_proposals``, extend
    ``_current_iter_inputs["decision_records"]``, bump
    ``_phase_b_producer_exceptions``).

    BUG PRESERVED — the strict-equality cluster lookup at the inner
    ``for _cid in _drop.get("source_clusters")`` loop matches
    ``cluster.root_cause`` against ``_drop.root_causes[*]``. The latter
    prefers ``asi_failure_type`` (per optimizer.py:15338-15342), so
    SQL-shape clusters whose ``asi_failure_type`` differs from their
    RcaKind ``root_cause`` are silently skipped. Plan A fixes this; this
    refactor only moves the bug into a place where it is testable.
    """
    if not l5_ag_drops:
        return ForcedSynthesisDispatchResult(
            attempted_dispatches=(),
            appended_proposals=(),
            emitted_decision_records=(),
        )
    raise NotImplementedError(
        "Task 3 fills in the dispatch body (verbatim from harness.py:22720-22929)"
    )
