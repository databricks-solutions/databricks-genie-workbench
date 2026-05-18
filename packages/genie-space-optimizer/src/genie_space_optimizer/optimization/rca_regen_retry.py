"""WU-2 — single-shot RCA regeneration retry for blocked clusters.

After Plan P-D (``run_rca_recovery_for_iteration`` at
``harness.py:24946``) and Phase 2.2 (provisional-RCA from soft
signals at ``harness.py:25051``) have run, the harness computes
``blocked_cluster_ids`` for the grounding gate prelude
(``harness.py:25119`` ``collect_blocked_clusters``). If that set is
non-empty, WU-2 re-invokes ``_regenerate_rca_for_cluster`` once per
blocked cluster, mutates ``cluster.rca_card`` in place when a card
is produced, and reports per-attempt verdicts so the postmortem
can see WHY each retry succeeded or failed.

This is **not** a new LLM call surface — it re-uses the existing
``build_rca_card`` + ``failure_buckets`` / ``asi`` attempt-source
chain. The purpose is to make the absence of a card after this
final retry AUTHORITATIVE: the harness's grounding-gate prelude
re-runs against the post-retry view, so any cluster that still has
``rca_card=None`` after WU-2 will be in ``blocked_cluster_ids``
and the SLATE_AUTHORITATIVE_SKIP marker can fire.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, Sequence


@dataclass(frozen=True, slots=True)
class RcaRegenAttempt:
    cluster_id: str
    rca_id: str
    attempted_sources: tuple[str, ...]
    succeeded: bool


@dataclass(frozen=True, slots=True)
class RcaRegenRetryResult:
    attempts: tuple[RcaRegenAttempt, ...]
    regenerated_cluster_ids: tuple[str, ...]


def retry_rca_regeneration_for_blocked(
    *,
    clusters: list[dict],
    blocked_cluster_ids: Sequence[str],
    spark,
    run_id: str,
    metadata_snapshot: Mapping,
    regenerator: Callable[..., Mapping] | None = None,
    soft_clusters: Sequence[Mapping] | None = None,
) -> RcaRegenRetryResult:
    """Re-invoke the RCA regeneration helper once per blocked cluster.

    Mutates each matching ``cluster["rca_card"]`` in place when the
    regenerator returns a non-empty card. Returns a per-attempt
    verdict tuple so the harness can emit observability markers.

    ``regenerator`` defaults to ``harness._regenerate_rca_for_cluster``;
    tests pass a stub.

    Pure with respect to ``blocked_cluster_ids``, ``spark``, ``run_id``,
    and ``metadata_snapshot`` — those are forwarded unchanged. The
    only side effect is the in-place mutation of cluster dicts.

    Exceptions raised by the regenerator are caught here so a buggy
    regenerator does not break the harness loop; each such cluster
    produces a ``RcaRegenAttempt(succeeded=False)``.
    """
    if regenerator is None:
        from genie_space_optimizer.optimization.harness import (
            _regenerate_rca_for_cluster as _default_regenerator,
        )
        regenerator = _default_regenerator

    blocked = {str(c) for c in (blocked_cluster_ids or ()) if c}
    if not blocked:
        return RcaRegenRetryResult(attempts=(), regenerated_cluster_ids=())

    attempts: list[RcaRegenAttempt] = []
    regenerated: list[str] = []

    for cluster in clusters or []:
        if not isinstance(cluster, dict):
            continue
        cid = str(
            cluster.get("cluster_id")
            or cluster.get("primary_cluster_id")
            or ""
        )
        if not cid or cid not in blocked:
            continue
        if isinstance(cluster.get("rca_card"), Mapping) and cluster.get("rca_card"):
            # Already grounded after Plan P-D / Phase 2.2 mutated it.
            attempts.append(
                RcaRegenAttempt(
                    cluster_id=cid,
                    rca_id=str(
                        cluster["rca_card"].get("rca_id") or ""
                    ),
                    attempted_sources=("already_grounded",),
                    succeeded=True,
                )
            )
            regenerated.append(cid)
            continue
        try:
            out = regenerator(
                spark=spark,
                run_id=str(run_id or ""),
                cluster=cluster,
                metadata_snapshot=dict(metadata_snapshot or {}),
                soft_clusters=(
                    list(soft_clusters) if soft_clusters else None
                ),
            )
        except Exception:
            attempts.append(
                RcaRegenAttempt(
                    cluster_id=cid,
                    rca_id="",
                    attempted_sources=("exception",),
                    succeeded=False,
                )
            )
            continue
        rca_id = str((out or {}).get("rca_id") or "")
        sources = tuple(
            str(s) for s in (
                (out or {}).get("attempted_sources") or ()
            )
        )
        succeeded = bool(rca_id) and isinstance(
            cluster.get("rca_card"), Mapping
        ) and bool(cluster.get("rca_card"))
        attempts.append(
            RcaRegenAttempt(
                cluster_id=cid,
                rca_id=rca_id,
                attempted_sources=sources,
                succeeded=succeeded,
            )
        )
        if succeeded:
            regenerated.append(cid)

    return RcaRegenRetryResult(
        attempts=tuple(attempts),
        regenerated_cluster_ids=tuple(regenerated),
    )
