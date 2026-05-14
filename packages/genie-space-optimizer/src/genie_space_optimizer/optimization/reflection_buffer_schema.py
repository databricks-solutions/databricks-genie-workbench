"""Phase 1.3/1.4 — reflection_buffer entry schema.

The reflection_buffer is the harness's in-memory history of
iteration outcomes. Phase 1.3 adds the cluster-level signature
fields; Phase 1.4 reads them to compute prior_failure_count.

Schema is ADDITIVE: every legacy field that pre-Phase-1 code reads
remains unchanged; the new fields are appended.

Contract-spec alignment (v1.1 Section 12.3):

  * ``terminal_signature`` is a top-level field on each entry,
    carrying the full ``TerminalSignature`` NamedTuple (5 fields per
    spec Section 4: root_cause, blame_set_norm, lever_set,
    target_qids, terminal_reason). It is ``None`` for accepted
    iterations.
  * ``cluster_signature`` is also a top-level field (NOT inside
    ``TerminalSignature``). Canonical shape is
    ``((cluster_id, target_qids_tuple),)`` — the grouping key used
    by Task 8's ``compute_prior_failure_count`` to count prior
    failures on a given cluster.
  * ``emitted_patch_shape`` is the third top-level addition (NOT
    embedded inside ``TerminalSignature``), an
    ``EmittedPatchShape`` enum value (or its string ``.value``).
    Used by Task 14's structural-repair gate and Task 6's retire-key
    computation.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:  # pragma: no cover - import-time only
    # Lazy / typing-only reference so this module can be imported
    # before Task 2 (terminal_signature.py) lands. The constructor
    # below uses runtime-flexible typing.
    from genie_space_optimizer.optimization.terminal_signature import (
        EmittedPatchShape,
        TerminalSignature,
    )


REFLECTION_BUFFER_REQUIRED_FIELDS: tuple[str, ...] = (
    # Legacy fields (pre-Phase-1):
    "iteration",
    "ag_id",
    "rollback_class",
    "accepted",
    # Phase 1.3/1.4 additions (per contract spec v1.1 Section 12.3):
    "cluster_signature",
    "terminal_signature",
    "emitted_patch_shape",
)


def build_reflection_entry(
    *,
    iteration: int,
    ag_id: str,
    rollback_class: str,
    accepted: bool,
    terminal_signature: "TerminalSignature | None",
    cluster_signature: tuple[tuple[str, tuple[str, ...]], ...],
    emitted_patch_shape: "EmittedPatchShape",
    legacy_fields: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a reflection_buffer entry with the Phase-1 signature
    fields plus every legacy field the caller supplies.

    ``terminal_signature`` is ``None`` for accepted iterations
    (the spec's ``AcceptanceTier`` — Plan B — owns the accepted
    vocabulary; we record ``accepted=True`` and leave the
    terminal_signature null per contract spec Section 6.4).

    ``cluster_signature`` is computed independently from cluster
    inputs (see ``_build_cluster_signature`` in the test fixture):
    it carries the cluster_id which is intentionally NOT inside
    ``TerminalSignature`` (the signature is keyed by target_qids +
    blame_set + lever_set + root_cause + terminal_reason).

    ``emitted_patch_shape`` records the shape of patches applied in
    this iteration (or ``EmittedPatchShape.ABSENT`` when no patches
    were applied). Used by Task 14's structural-repair gate and
    Task 6's retire-key computation.

    ``legacy_fields`` is merged after the required fields so it
    cannot accidentally overwrite the required fields; if the caller
    passes those keys in ``legacy_fields``, they are silently
    dropped.
    """
    entry: dict[str, Any] = {
        "iteration": int(iteration),
        "ag_id": str(ag_id or ""),
        "rollback_class": str(rollback_class or ""),
        "accepted": bool(accepted),
        "cluster_signature": cluster_signature,
        "terminal_signature": terminal_signature,
        "emitted_patch_shape": emitted_patch_shape,
    }
    for k, v in (legacy_fields or {}).items():
        if k in entry:
            continue
        entry[k] = v
    return entry
