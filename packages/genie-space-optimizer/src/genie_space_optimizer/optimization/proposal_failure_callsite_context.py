"""Typed call-site bundle for ``_emit_proposal_failure_decided``.

Each of the 5 proposal-failure emit sites in ``harness.py`` builds one
of these from its iteration-local state (cluster dict from
``_iter_source_clusters_by_id``, lazy ``RcaRegenerationCache`` and
``RcaRegenerationPolicy`` from ``_rca_recovery_holder``, the per-run
``_iter_failure_signatures`` counter, and the iteration's findings /
evidence snapshot). Bundling them avoids growing
``_emit_proposal_failure_decided`` to 23 parameters and keeps the
existing 15-kwarg signature stable on the caller side.

A ``noop_context()`` factory returns an empty bundle used by tests and
by call sites that are intentionally non-actionable (e.g. unit tests
of the emit helper itself).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProposalFailureCallSiteContext:
    """Per-emit-site dependencies the policy handler needs."""

    cluster: dict
    findings: list
    evidence_snapshot: dict
    cache: Any  # RcaRegenerationCache | None
    policy: Any  # RcaRegenerationPolicy | None
    signatures_counter: dict[str, int]
    metadata_snapshot: dict
    spark: Any  # SparkSession | None


def noop_context() -> ProposalFailureCallSiteContext:
    """Return a context with no live cache, policy, or counter.

    Useful when ``_emit_proposal_failure_decided`` is called from a
    code path that cannot supply real iteration state (e.g.
    backward-compat callers that have not been refactored yet). The
    handler will see ``cache=None`` / ``policy=None`` and skip the
    regen invocation.
    """
    return ProposalFailureCallSiteContext(
        cluster={},
        findings=[],
        evidence_snapshot={},
        cache=None,
        policy=None,
        signatures_counter={},
        metadata_snapshot={},
        spark=None,
    )
