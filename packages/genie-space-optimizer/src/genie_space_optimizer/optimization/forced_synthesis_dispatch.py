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

from dataclasses import dataclass
from typing import Any


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
