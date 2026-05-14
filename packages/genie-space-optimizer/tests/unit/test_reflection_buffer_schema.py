"""Phase 1.3/1.4 — reflection_buffer entries carry the cluster
signature and terminal signature so prior_failure_count and
forbidden-AG-set logic can read them.

Task 2 (TerminalSignature) is parallel-safe with Task 5 per the
plan's Batch A grouping. If Task 2 has not yet landed, this test
falls back to a small in-test stub of the TerminalSignature /
EmittedPatchShape surface. Once Task 2 commits, the real types are
imported automatically and these tests exercise the genuine
production surface.
"""
from __future__ import annotations

from typing import Any, Iterable, NamedTuple

import pytest

# Real ``TerminalReason`` enum is required (Task 1, already landed).
from genie_space_optimizer.optimization.terminal_reason import TerminalReason


try:  # pragma: no cover - exercised once Task 2 lands.
    from genie_space_optimizer.optimization.terminal_signature import (
        EmittedPatchShape,
        TerminalSignature,
        build_terminal_signature,
    )
except ImportError:  # pragma: no cover - exercised before Task 2 lands.
    # Minimal stand-in surface; matches the spec Section 4.2 shape
    # used by Task 5's schema module. Removed automatically once
    # Task 2's terminal_signature.py is on disk.
    from enum import StrEnum

    class EmittedPatchShape(StrEnum):  # type: ignore[no-redef]
        STRUCTURAL = "structural"
        METADATA = "metadata"
        INSTRUCTION = "instruction"
        ABSENT = "absent"

    class TerminalSignature(NamedTuple):  # type: ignore[no-redef]
        root_cause: str
        blame_set_norm: tuple[str, ...]
        lever_set: frozenset[int]
        target_qids: frozenset[str]
        terminal_reason: str

    def build_terminal_signature(  # type: ignore[no-redef]
        *,
        root_cause: object,
        blame_set: object,
        lever_set: object,
        target_qids: object,
        terminal_reason: "TerminalReason | str",
    ) -> TerminalSignature:
        blame_iter: Iterable[object] = blame_set or ()
        blame_sorted = tuple(sorted(
            str(b).strip() for b in blame_iter if str(b).strip()
        ))
        lever_iter: Iterable[object] = lever_set or ()
        levers = frozenset(int(L) for L in lever_iter)
        qid_iter: Iterable[object] = target_qids or ()
        qids = frozenset(
            str(q).strip() for q in qid_iter if str(q).strip()
        )
        if isinstance(terminal_reason, TerminalReason):
            tr_value = terminal_reason.value
        else:
            tr_value = TerminalReason(str(terminal_reason)).value
        return TerminalSignature(
            root_cause=str(root_cause or "").strip().lower(),
            blame_set_norm=blame_sorted,
            lever_set=levers,
            target_qids=qids,
            terminal_reason=tr_value,
        )


# Schema module under test (Task 5).
from genie_space_optimizer.optimization.reflection_buffer_schema import (
    REFLECTION_BUFFER_REQUIRED_FIELDS,
    build_reflection_entry,
)


def _build_cluster_signature(
    cluster_id: str,
    target_qids: list[str],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Helper to construct the canonical cluster-grouping signature
    independently of TerminalSignature (which no longer carries the
    cluster_id)."""
    return ((str(cluster_id), tuple(sorted(target_qids))),)


def test_required_fields_include_signatures():
    assert "cluster_signature" in REFLECTION_BUFFER_REQUIRED_FIELDS
    assert "terminal_signature" in REFLECTION_BUFFER_REQUIRED_FIELDS
    assert "emitted_patch_shape" in REFLECTION_BUFFER_REQUIRED_FIELDS


def test_build_reflection_entry_carries_terminal_signature():
    # Spec-compliant build: blame_set, lever_set, target_qids are
    # signature inputs; cluster_signature is computed locally.
    sig = build_terminal_signature(
        root_cause="missing_metric_view",
        blame_set=("catalog.schema.orders",),
        lever_set=[5, 6],
        target_qids=["gs_001"],
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    cluster_sig = _build_cluster_signature("c1", ["gs_001"])
    entry = build_reflection_entry(
        iteration=2,
        ag_id="ag-2",
        rollback_class="no_action",
        accepted=False,
        terminal_signature=sig,
        cluster_signature=cluster_sig,
        emitted_patch_shape=EmittedPatchShape.METADATA,
        legacy_fields={
            "rollback_reason": "no_applied_patches",
            "root_cause": "missing_metric_view",
            "target_qids": ["gs_001"],
            "blame_set": ["catalog.schema.orders"],
            "lever_set": [5, 6],
        },
    )
    for f in REFLECTION_BUFFER_REQUIRED_FIELDS:
        assert f in entry, f"missing required field: {f}"
    assert entry["terminal_signature"] == sig
    assert entry["cluster_signature"] == cluster_sig
    assert entry["emitted_patch_shape"] == EmittedPatchShape.METADATA


def test_legacy_fields_preserved_alongside_new_fields():
    """Schema is ADDITIVE — every legacy reflection_buffer reader
    keeps working."""
    sig = build_terminal_signature(
        root_cause="r",
        blame_set=(),
        lever_set=[5],
        target_qids=["gs_001"],
        terminal_reason=TerminalReason.CONTENT_REGRESSION_ROLLBACK,
    )
    entry = build_reflection_entry(
        iteration=1,
        ag_id="ag-1",
        rollback_class="content_regression",
        accepted=False,
        terminal_signature=sig,
        cluster_signature=_build_cluster_signature("c1", ["gs_001"]),
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        legacy_fields={
            "rollback_reason": "content_regression",
            "root_cause": "r",
            "target_qids": ["gs_001"],
            "blame_set": ["gs_005"],
            "lever_set": [5],
            "escalation_handled": False,
        },
    )
    assert entry["rollback_class"] == "content_regression"
    assert entry["rollback_reason"] == "content_regression"
    assert entry["root_cause"] == "r"
    assert entry["target_qids"] == ["gs_001"]
    assert entry["blame_set"] == ["gs_005"]
    assert entry["lever_set"] == [5]
    assert entry["escalation_handled"] is False
