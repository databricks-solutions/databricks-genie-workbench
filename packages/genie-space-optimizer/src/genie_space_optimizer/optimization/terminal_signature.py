"""Phase 1.3 — TerminalSignature: cluster-level retry memory key.

Replaces the ad-hoc forbidden tuple in ``harness.py:12291``
(``(root_cause, blame_set_norm, frozenset(lever_set))``) with a
strict-superset 5-field NamedTuple that adds ``target_qids`` and
``terminal_reason`` so signatures can distinguish 'tried L5 on
gs_021' from 'tried L5 on gs_026', and 'NO_STRUCTURAL_CANDIDATE on
this AG' from 'BLAST_RADIUS_REJECTED on this AG'.

Field order, types, and the ``build_terminal_signature`` constructor
are LOCKED by ``docs/final_plan/2026-05-13-final-closeout-contract-
spec.md`` Section 4.

Distinct from the patch-level ``patch_retry_signature`` in
``optimization/reflection_retry.py`` — both coexist; this one drives
AG retirement at the cluster level, the patch-level one drives
patch-level retry within an iteration.

``EmittedPatchShape`` lives in this module for convenience (callers
already import this module) but is NOT a TerminalSignature field.
Task 5 records it as a separate top-level field on the
reflection_buffer entry, and Task 14's structural-repair gate reads
``rca_card.intended_patch_shape`` directly.
"""
from __future__ import annotations

import json
from enum import StrEnum
from typing import Iterable, NamedTuple, Sequence

from genie_space_optimizer.optimization.terminal_reason import TerminalReason


class EmittedPatchShape(StrEnum):
    """Closed vocabulary for the shape of patches emitted in an
    iteration. Used by Task 5 to label the reflection_buffer entry
    and by Task 14's structural-repair gate to compare against
    ``rca_card.intended_patch_shape``.
    """
    STRUCTURAL = "structural"
    """At least one patch is a structural-repair family member
    (L5 example SQL, narrow L6 SQL, join/routing rule, grain fix)."""
    METADATA = "metadata"
    """Patches change UC metadata (descriptions, comments, schema
    docs) but no structural SQL/routing shape."""
    INSTRUCTION = "instruction"
    """Only space-level or per-question instructions / GSL text."""
    ABSENT = "absent"
    """No patches emitted (PROPOSAL_GENERATION_EMPTY / NO_APPLIED_PATCHES paths)."""


def normalize_signature_str(value: object) -> str:
    """Canonical string normalization for signature fields.

    Returns ``""`` for None / empty. Otherwise lowercases and strips.
    Used to ensure 'CapitalCase' and 'capitalcase' hash equal
    (spec Section 4.3).
    """
    if value is None:
        return ""
    return str(value).strip().lower()


class TerminalSignature(NamedTuple):
    """Cluster-level retry key. Field order is LOCKED by spec
    Section 4.2 — do not reorder.

    Fields (all hashable):

    Args:
        root_cause: The RCA card's ``dominant_root_cause`` value
            (or ``""`` if missing). Normalized lowercase via
            ``normalize_signature_str``.
        blame_set_norm: Canonically sorted tuple of upstream UC asset
            identifiers blamed by the RCA. Empty tuple is valid.
        lever_set: Frozen set of lever indices (e.g.,
            ``frozenset({5})`` or ``frozenset({5, 6})``).
        target_qids: Frozen set of target qids the AG was trying to
            fix (e.g., ``frozenset({"gs_026"})``).
        terminal_reason: The ``TerminalReason.value`` string that
            caused the terminate. Stored as ``str`` (not enum) for
            JSON-roundtrip stability.
    """
    root_cause: str
    blame_set_norm: tuple[str, ...]
    lever_set: frozenset[int]
    target_qids: frozenset[str]
    terminal_reason: str


def build_terminal_signature(
    *,
    root_cause: object,
    blame_set: object,
    lever_set: object,
    target_qids: object,
    terminal_reason: "TerminalReason | str",
) -> TerminalSignature:
    """Constructor with normalization. All callers MUST use this
    instead of the raw NamedTuple constructor (spec Section 4.3).

    Sorts ``blame_set`` ascending into a tuple, freezes
    ``lever_set`` and ``target_qids``, normalizes ``root_cause`` to
    lowercase-stripped form, and stores ``terminal_reason`` as the
    enum's ``.value`` string.
    """
    blame_iter: Iterable[object] = blame_set or ()
    blame_sorted = tuple(sorted(
        str(b).strip() for b in blame_iter if str(b).strip()
    ))
    lever_iter: Iterable[object] = lever_set or ()
    levers = frozenset(int(L) for L in lever_iter)
    qid_iter: Iterable[object] = target_qids or ()
    qids = frozenset(str(q).strip() for q in qid_iter if str(q).strip())
    if isinstance(terminal_reason, TerminalReason):
        tr_value = terminal_reason.value
    else:
        # Validate that the string is in the closed vocabulary.
        tr_value = TerminalReason(str(terminal_reason)).value
    return TerminalSignature(
        root_cause=normalize_signature_str(root_cause),
        blame_set_norm=blame_sorted,
        lever_set=levers,
        target_qids=qids,
        terminal_reason=tr_value,
    )


def resolve_emitted_patch_shape(
    applied_patches: Sequence[object],
) -> EmittedPatchShape:
    """Classify a list of applied patches into one EmittedPatchShape.

    Reads the typed PatchSemantic via patch_semantic.semantic_for_patch_type.
    Unknown patch_types raise — new patch types must register their
    semantic class in patch_semantic.PATCH_TYPE_SEMANTICS, which makes the
    "I forgot to update the substring tuple" failure mode impossible.

    Returns the highest-severity shape present
    (STRUCTURAL > METADATA > INSTRUCTION > ABSENT).
    """
    from genie_space_optimizer.optimization.patch_semantic import (
        PatchSemantic,
        semantic_for_patch_type,
    )

    has_structural = False
    has_metadata = False
    has_instruction = False
    for patch in applied_patches or ():
        if isinstance(patch, dict):
            ptype = str(patch.get("patch_type") or "")
        else:
            ptype = str(getattr(patch, "patch_type", "") or "")
        if not ptype:
            continue
        sem = semantic_for_patch_type(ptype)
        if sem is PatchSemantic.STRUCTURAL:
            has_structural = True
        elif sem is PatchSemantic.METADATA:
            has_metadata = True
        elif sem is PatchSemantic.INSTRUCTION:
            has_instruction = True
    if has_structural:
        return EmittedPatchShape.STRUCTURAL
    if has_metadata:
        return EmittedPatchShape.METADATA
    if has_instruction:
        return EmittedPatchShape.INSTRUCTION
    return EmittedPatchShape.ABSENT


def signature_string(sig: TerminalSignature) -> str:
    """Stable string form for human-readable logs and the Phase 0.4
    candidate ledger ``retire_signature`` field.

    Format: ``rc=<root_cause>|blame=<a>,<b>|levers=<n>,<n>|
            qids=<qid>,<qid>|tr=<terminal_reason>``
    """
    blame = ",".join(sig.blame_set_norm)
    levers = ",".join(str(L) for L in sorted(sig.lever_set))
    qids = ",".join(sorted(sig.target_qids))
    return (
        f"rc={sig.root_cause}|blame={blame}|levers={levers}"
        f"|qids={qids}|tr={sig.terminal_reason}"
    )


def to_jsonable(sig: TerminalSignature) -> dict:
    """Return the spec Section 4.4 JSON-roundtrip shape: sets become
    canonically sorted lists. Used by the candidate ledger writer
    (Phase 0.4) and the ``GSO_ITERATION_TERMINAL_DECIDED_V1`` marker.
    """
    return {
        "root_cause": sig.root_cause,
        "blame_set_norm": list(sig.blame_set_norm),
        "lever_set": sorted(sig.lever_set),
        "target_qids": sorted(sig.target_qids),
        "terminal_reason": sig.terminal_reason,
    }


def from_jsonable(d: dict) -> TerminalSignature:
    """Inverse of :func:`to_jsonable`. Used by ``read_ledger`` to
    deserialize ledger entries back into hashable signatures."""
    return TerminalSignature(
        root_cause=str(d.get("root_cause") or ""),
        blame_set_norm=tuple(d.get("blame_set_norm") or ()),
        lever_set=frozenset(int(L) for L in (d.get("lever_set") or ())),
        target_qids=frozenset(
            str(q) for q in (d.get("target_qids") or ())
        ),
        terminal_reason=str(d.get("terminal_reason") or ""),
    )


# Back-compat helper for the migration step described in spec
# Section 4.6: ``legacy_forbidden_tuple`` returns the first three
# fields in the legacy 3-tuple shape so existing consumers can be
# migrated incrementally before being removed.
def legacy_forbidden_tuple(
    sig: TerminalSignature,
) -> tuple[str, tuple[str, ...], frozenset[int]]:
    """Return the legacy 3-tuple ``(root_cause, blame_set_norm,
    lever_set)`` consumed by callers that have not yet migrated to
    TerminalSignature. Removed in the post-trial cleanup commit."""
    return (sig.root_cause, sig.blame_set_norm, sig.lever_set)
