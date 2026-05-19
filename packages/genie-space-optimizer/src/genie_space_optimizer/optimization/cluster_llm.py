"""Plan 4 — LLM clustering driver + deterministic post-validation.

Public surface (Task 9 / Task 10):
  * ``cluster_failures_llm(...)`` — one-LLM-call-per-iteration
    driver that returns ``list[LlmCluster]`` or ``None``.

Private helpers (Task 8):
  * ``_validate_member_qids_in_input``
  * ``_validate_no_qid_collision``
  * ``_validate_blame_set_in_schema``
  * ``_stamp_cluster_id``
"""
from __future__ import annotations

from collections.abc import Iterable

from genie_space_optimizer.optimization.cluster_typed import (
    ClusterValidationError,
    LlmCluster,
)


def _validate_member_qids_in_input(
    cluster: LlmCluster,
    *,
    input_qids: set[str],
) -> None:
    """Reject when any member_qid is not in the input batch.

    The LLM was given a fixed set of qids; emitting a member outside
    that set indicates hallucination. We reject the cluster and route
    its qids back through the deterministic path.
    """
    unknown = [
        qid for qid in cluster.member_qids if qid not in input_qids
    ]
    if unknown:
        raise ClusterValidationError(
            f"cluster {cluster.cluster_id!r}: member_qids include "
            f"qid(s) not in input batch: {sorted(unknown)}"
        )


def _validate_no_qid_collision(clusters: Iterable[LlmCluster]) -> None:
    """Reject when the same qid appears in two clusters.

    Raised at the SET level — the LLM violated the "no qid in two
    clusters" rule and the entire LLM output is untrustworthy for
    partitioning. The driver falls through to the deterministic
    body in this case.
    """
    seen: dict[str, str] = {}
    for cluster in clusters:
        for qid in cluster.member_qids:
            if qid in seen and seen[qid] != cluster.cluster_id:
                raise ClusterValidationError(
                    f"qid {qid!r} appears in two clusters: "
                    f"{seen[qid]!r} and {cluster.cluster_id!r}"
                )
            seen[qid] = cluster.cluster_id


def _validate_blame_set_in_schema(
    cluster: LlmCluster,
    *,
    schema_columns: set[str],
) -> None:
    """Reject when any primary_blame_set entry is not present in the
    schema. Empty primary_blame_set is vacuously valid
    (metadata-level failure). Case-sensitive — UC identifiers are
    case-sensitive in Genie Spaces.
    """
    unknown = [
        col for col in cluster.primary_blame_set
        if col not in schema_columns
    ]
    if unknown:
        raise ClusterValidationError(
            f"cluster {cluster.cluster_id!r}: primary_blame_set "
            f"includes column(s) not in schema: {sorted(unknown)}"
        )


def _stamp_cluster_id(*, namespace: str, index: int) -> str:
    """Build a deterministic cluster_id.

    Format: ``<namespace><000-padded index>``. Examples:
      _stamp_cluster_id(namespace="H", index=1)  → "H001"
      _stamp_cluster_id(namespace="S", index=42) → "S042"
      _stamp_cluster_id(namespace="H", index=1234) → "H1234"

    Namespace must be non-empty; index must be ≥ 1.
    """
    if not namespace:
        raise ValueError("namespace must be non-empty")
    if index < 1:
        raise ValueError(f"index must be ≥ 1; got {index}")
    return f"{namespace}{index:03d}"
