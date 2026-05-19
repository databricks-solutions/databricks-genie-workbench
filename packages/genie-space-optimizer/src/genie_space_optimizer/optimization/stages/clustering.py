"""Stage 3: Cluster Formation (Phase F3).

Wraps the existing ``optimizer.cluster_failures`` primitive with a
typed ``ClusteringInput`` / ``ClusterFindings`` surface so F4 (action
groups) can read clusters from a stage-aligned dataclass instead of
harness locals. Also splits promoted vs rejected clusters by
``demoted_reason`` so Phase D.5 alternatives capture has a typed
surface.

F3 wires the harness's hard + soft ``cluster_failures`` pair into a
single typed call (Phase F+H Commit A1). ``form(ctx, inp)`` calls
``optimizer.cluster_failures`` internally for both branches with
identical args except ``spark`` (form passes ``spark=None``; replay-
fixture mode is unaffected because spark is None everywhere there,
but production runs skip the spark-conditional ``read_asi_from_uc``
UC enrichment at ``optimizer.py:1913-1915``). The harness inline
``cluster_failures(...)`` calls (formerly at ``harness.py:9158``
hard, ``9171`` soft) are deleted by A1. The ``cluster_records`` /
``rca_formed_records`` emissions at ``harness.py:12318+`` /
``:12349+`` continue to read the harness's ``clusters`` /
``soft_clusters`` locals which the A1 adapter populates from
``_cluster_findings.clusters`` / ``_cluster_findings.soft_clusters``.

The promoted-vs-rejected split assumes ``cluster_failures`` may stamp
``demoted_reason`` on its returns; today (verified at A1 audit time)
it does not, so ``rejected_cluster_alternatives`` is always empty —
``_split_by_demoted`` is a forward-compatible no-op.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.optimization.optimizer import cluster_failures
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.failure_cluster import (
        FailureCluster,
    )


STAGE_KEY: str = "cluster_formation"


@dataclass
class ClusteringInput(JsonRoundTrip):
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
class ClusterFindings(JsonRoundTrip):
    """Output of stages.clustering.form.

    ``clusters`` is the promoted-hard tuple (no ``demoted_reason``).
    ``soft_clusters`` is the promoted-soft tuple.
    ``rejected_cluster_alternatives`` is every cluster the optimizer
    returned that carries a ``demoted_reason`` — F4 reads this to
    stamp Phase D.5 ``AlternativeOption.cluster``.

    Plan 1 Task 11: ``cluster_records`` is the typed-FailureCluster
    sidecar. Derived from ``clusters`` via
    ``FailureCluster.from_legacy`` in __post_init__ when not supplied
    explicitly. Invalid clusters (empty cluster_id, identity
    mismatch) are silently skipped from typed records but remain in
    the legacy ``clusters`` tuple so byte-stable downstream reads are
    preserved.
    """

    clusters: tuple[dict[str, Any], ...]
    soft_clusters: tuple[dict[str, Any], ...] = ()
    rejected_cluster_alternatives: tuple[dict[str, Any], ...] = ()
    cluster_records: tuple["FailureCluster", ...] = ()

    def __post_init__(self) -> None:
        if not self.cluster_records and self.clusters:
            from genie_space_optimizer.optimization.failure_cluster import (
                FailureCluster,
            )
            derived: list[FailureCluster] = []
            for c in self.clusters:
                try:
                    record = FailureCluster.from_legacy(c)
                except Exception:
                    continue
                if record.cluster_id:
                    derived.append(record)
            self.cluster_records = tuple(derived)

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        # cluster_records is intentionally NOT serialised — it is a
        # derived view of ``clusters``. ``from_json`` re-derives via
        # __post_init__. This keeps the JSON payload byte-stable with
        # pre-Plan-1 fixtures (no new JSON keys).
        return {
            "clusters": [dict(c) for c in (self.clusters or ())],
            "soft_clusters": [dict(c) for c in (self.soft_clusters or ())],
            "rejected_cluster_alternatives": [
                dict(c) for c in (self.rejected_cluster_alternatives or ())
            ],
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "ClusterFindings":  # type: ignore[override]
        return cls(
            clusters=tuple(dict(c) for c in (payload.get("clusters") or [])),
            soft_clusters=tuple(
                dict(c) for c in (payload.get("soft_clusters") or [])
            ),
            rejected_cluster_alternatives=tuple(
                dict(c)
                for c in (payload.get("rejected_cluster_alternatives") or [])
            ),
        )


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
