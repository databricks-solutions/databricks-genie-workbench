"""Stage 7: Patch Application + immediate rollback (Phase F7).

Wraps the existing ``decision_emitters.patch_applied_records`` producer
in a typed ``ApplicationInput`` / ``AppliedPatchSet`` surface so F8
(acceptance) can read the slate from a stage-aligned dataclass.

F7 is observability-only: per the plan's Reality Check, the actual
application primitive is ``applier.apply_patch_set`` (the plan's
earlier draft referenced a non-existent ``apply_levers_to_config``).
The harness call site at ``harness.py:16104`` plus the
post-apply verification block around ``harness.py:16845`` (which is
where ``FailedRollbackVerification`` is actually raised) are
intertwined with downstream eval / acceptance logic. Lifting them
under F7's byte-stability gate is high-risk.

F7 stands up the typed surface and PATCH_APPLIED emission entry that
F8 will consume; the actual apply call + verification stays in
harness for now and is deferred to a follow-up plan.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from genie_space_optimizer.optimization.decision_emitters import (
    patch_applied_records,
)


STAGE_KEY: str = "applied_patches"


@dataclass
class AppliedPatch:
    proposal_id: str
    ag_id: str
    patch_type: str
    target_qids: tuple[str, ...]
    cluster_id: str = ""
    content_fingerprint: str = ""
    rolled_back_immediately: bool = False
    rollback_reason: str | None = None


@dataclass
class ApplicationInput:
    """Input to stages.application.apply.

    ``applied_entries_by_ag`` maps AG id → tuple of apply-log entries
    (each entry has a ``patch`` key carrying the patch dict, matching
    the ``apply_log["applied"][i]`` shape that
    ``decision_emitters.patch_applied_records`` consumes). Entries that
    were applied-then-rolled-back may carry an inline
    ``rolled_back_immediately=True`` + ``rollback_reason`` marker.
    """

    applied_entries_by_ag: dict[str, tuple[Mapping[str, Any], ...]]
    ags: tuple[Mapping[str, Any], ...]
    rca_id_by_cluster: Mapping[str, str] = field(default_factory=dict)
    cluster_root_cause_by_id: Mapping[str, str] = field(default_factory=dict)


@dataclass
class AppliedPatchSet:
    """Output of stages.application.apply.

    ``applied`` is the typed tuple of all AppliedPatch entries across
    every AG, including any that were rolled back immediately.
    ``applied_signature`` is a stable hash F8 / F9 use for cycle
    detection across iterations.
    """

    applied: tuple[AppliedPatch, ...]
    applied_signature: str = ""


def _ag_lookup(ags: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for ag in ags or []:
        ag_id = str(ag.get("ag_id") or ag.get("id") or "")
        if ag_id:
            out[ag_id] = ag
    return out


def _entry_to_applied_patch(
    entry: Mapping[str, Any], ag_id: str,
) -> AppliedPatch | None:
    """Build an AppliedPatch from an apply_log entry.

    Returns None if the entry's patch carries no proposal_id (a
    skip/no-op record from the applier).
    """
    patch = entry.get("patch") or {}
    if not isinstance(patch, Mapping):
        return None
    proposal_id = str(
        patch.get("proposal_id")
        or patch.get("expanded_patch_id")
        or patch.get("id")
        or ""
    )
    if not proposal_id:
        return None
    target_qids = tuple(
        str(q) for q in (patch.get("target_qids") or []) if str(q)
    )
    return AppliedPatch(
        proposal_id=proposal_id,
        ag_id=ag_id,
        patch_type=str(patch.get("patch_type") or patch.get("type") or ""),
        target_qids=target_qids,
        cluster_id=str(patch.get("cluster_id") or ""),
        content_fingerprint=str(patch.get("content_fingerprint") or ""),
        rolled_back_immediately=bool(entry.get("rolled_back_immediately") or False),
        rollback_reason=(
            str(entry.get("rollback_reason"))
            if entry.get("rollback_reason")
            else None
        ),
    )


def _compute_applied_signature(applied: Sequence[AppliedPatch]) -> str:
    """Stable hash of all applied patches for cycle-detection."""
    h = hashlib.sha256()
    for p in sorted(applied, key=lambda x: (x.ag_id, x.proposal_id)):
        h.update(
            f"{p.ag_id}|{p.proposal_id}|{p.patch_type}|{p.content_fingerprint}|"
            .encode()
        )
    return h.hexdigest()[:16]


def apply(ctx, inp: ApplicationInput) -> AppliedPatchSet:
    """Stage 7 entry. Builds AppliedPatch entries from apply_log
    entries, emits PATCH_APPLIED records via ctx.decision_emit, and
    returns the typed slate.

    F7 is observability-only — does NOT invoke the applier or the
    post-apply verification block. Harness still owns those steps and
    feeds the result into ``inp.applied_entries_by_ag`` when the
    harness wire-up lands in a follow-up plan.
    """
    ag_lookup = _ag_lookup(inp.ags)
    all_applied: list[AppliedPatch] = []

    for ag_id, entries in (inp.applied_entries_by_ag or {}).items():
        ag = ag_lookup.get(str(ag_id), {"ag_id": ag_id})
        ag_applied: list[AppliedPatch] = []
        for entry in entries:
            ap = _entry_to_applied_patch(entry, str(ag_id))
            if ap is not None:
                ag_applied.append(ap)
        all_applied.extend(ag_applied)

        # Emit one PATCH_APPLIED DecisionRecord per applied entry.
        # Skip emission for entries that lack target_qids (the producer
        # itself drops them per Cycle-8-Bug-1).
        records = patch_applied_records(
            run_id=ctx.run_id,
            iteration=ctx.iteration,
            ag_id=str(ag_id),
            applied_entries=entries,
            rca_id_by_cluster=inp.rca_id_by_cluster,
            cluster_root_cause_by_id=inp.cluster_root_cause_by_id,
        )
        for record in records:
            ctx.decision_emit(record)

    return AppliedPatchSet(
        applied=tuple(all_applied),
        applied_signature=_compute_applied_signature(all_applied),
    )
