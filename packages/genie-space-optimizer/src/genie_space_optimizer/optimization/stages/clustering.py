"""Stage 3: Cluster Formation (Phase F3).

Wraps the existing ``optimizer.cluster_failures`` primitive with a
typed ``ClusteringInput`` / ``ClusterFindings`` surface so F4 (action
groups) can read clusters from a stage-aligned dataclass instead of
harness locals. Also splits promoted vs rejected clusters by
``demoted_reason`` so Phase D.5 alternatives capture has a typed
surface.

F3 is observability-only: it does NOT modify any harness call sites.
The two existing inline ``cluster_failures(...)`` calls at
``harness.py:9158`` (hard) and ``9171`` (soft) plus the
``cluster_records`` / ``rca_formed_records`` emissions in
``_run_lever_loop`` (lines 12296+ / 12345+) stay in place.
``stages.clustering.form`` exposes a parallel typed surface that F4
will consume; harness wiring + emission move are deferred to a
follow-up plan to preserve byte-stability.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.optimizer import cluster_failures


STAGE_KEY: str = "cluster_formation"


@dataclass
class ClusteringInput:
    """Input to stages.clustering.form, matching the actual
    optimizer.cluster_failures signature.

    ``eval_result_for_clustering`` is the ``{"rows": filtered_failure_rows}``
    dict the harness builds at ``harness.py:9157``. ``metadata_snapshot``
    is the per-iteration metadata snapshot. ``soft_eval_result`` is the
    optional ``{"rows": soft_signal_rows}`` for soft clustering.
    """

    eval_result_for_clustering: dict[str, Any]
    metadata_snapshot: dict[str, Any]
    soft_eval_result: dict[str, Any] | None = None
    held_out_qids: tuple[str, ...] = ()
    qid_state: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClusterFindings:
    """Output of stages.clustering.form.

    ``clusters`` is the promoted-hard tuple (no ``demoted_reason``).
    ``soft_clusters`` is the promoted-soft tuple.
    ``rejected_cluster_alternatives`` is every cluster the optimizer
    returned that carries a ``demoted_reason`` — F4 reads this to stamp
    Phase D.5 ``AlternativeOption.cluster``.
    """

    clusters: tuple[dict[str, Any], ...]
    soft_clusters: tuple[dict[str, Any], ...] = ()
    rejected_cluster_alternatives: tuple[dict[str, Any], ...] = ()


def _split_by_demoted(
    clusters: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split a cluster list into (promoted, rejected) based on
    ``demoted_reason``.

    A cluster is rejected if it has a non-empty ``demoted_reason``
    field. The optimizer carries demotion reasons inline on every
    cluster it returns; consumers filter rather than rely on a
    fabricated ``emit_rejected=True`` kwarg.
    """
    promoted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for c in clusters or []:
        if str(c.get("demoted_reason") or "").strip():
            rejected.append(c)
        else:
            promoted.append(c)
    return promoted, rejected


def form(ctx, inp: ClusteringInput) -> ClusterFindings:
    """Stage 3 entry. Wraps optimizer.cluster_failures.

    F3 is observability-only — no harness call site is modified.
    Returns a typed ClusterFindings that F4 will consume in addition
    to (or eventually instead of) the existing harness clusters local.
    """
    hard_clusters_raw = cluster_failures(
        inp.eval_result_for_clustering,
        inp.metadata_snapshot,
        spark=None,
        run_id=ctx.run_id,
        catalog=ctx.catalog,
        schema=ctx.schema,
        qid_state=inp.qid_state,
        signal_type="hard",
        namespace="H",
    )
    promoted_hard, rejected_hard = _split_by_demoted(list(hard_clusters_raw or []))

    soft_clusters: list[dict[str, Any]] = []
    rejected_soft: list[dict[str, Any]] = []
    if inp.soft_eval_result and (inp.soft_eval_result.get("rows") or []):
        soft_raw = cluster_failures(
            inp.soft_eval_result,
            inp.metadata_snapshot,
            spark=None,
            run_id=ctx.run_id,
            catalog=ctx.catalog,
            schema=ctx.schema,
            verbose=False,
            qid_state=inp.qid_state,
            signal_type="soft",
            namespace="S",
        )
        soft_clusters, rejected_soft = _split_by_demoted(list(soft_raw or []))

    return ClusterFindings(
        clusters=tuple(promoted_hard),
        soft_clusters=tuple(soft_clusters),
        rejected_cluster_alternatives=tuple(rejected_hard) + tuple(rejected_soft),
    )


# ── Phase H: explicit Input/Output class declarations ─────────────────
# Phase H's per-stage I/O capture decorator imports these to serialize
# the stage's typed input and output to MLflow.
INPUT_CLASS = ClusteringInput
OUTPUT_CLASS = ClusterFindings


# ── G-lite: uniform execute() alias ───────────────────────────────────
# The named verb above is preserved for human-readable harness call
# sites. The ``execute`` alias is what the stage registry, conformance
# test, and Phase H capture decorator import.
execute = form
