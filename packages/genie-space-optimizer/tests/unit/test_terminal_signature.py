"""Phase 1.3 — TerminalSignature: cluster-level retry key.

Shape locked by ``docs/final_plan/2026-05-13-final-closeout-
contract-spec.md`` Section 4.2.
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
    TerminalSignature,
    build_terminal_signature,
    normalize_signature_str,
    resolve_emitted_patch_shape,
    signature_string,
)


def test_terminal_signature_is_a_namedtuple_with_5_fields_in_spec_order():
    sig = TerminalSignature(
        root_cause="missing_metric_view",
        blame_set_norm=("catalog.schema.orders",),
        lever_set=frozenset({5, 6}),
        target_qids=frozenset({"gs_001"}),
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES.value,
    )
    # Field order is locked — accessing by index MUST follow spec order.
    assert sig[0] == "missing_metric_view"
    assert sig[1] == ("catalog.schema.orders",)
    assert sig[2] == frozenset({5, 6})
    assert sig[3] == frozenset({"gs_001"})
    assert sig[4] == TerminalReason.NO_APPLIED_PATCHES.value
    # And by name.
    assert sig.root_cause == "missing_metric_view"
    assert sig.blame_set_norm == ("catalog.schema.orders",)
    assert sig.lever_set == frozenset({5, 6})
    assert sig.target_qids == frozenset({"gs_001"})
    assert sig.terminal_reason == "no_applied_patches"


def test_signatures_are_equal_when_fields_equal_and_hashable():
    a = build_terminal_signature(
        root_cause="r",
        blame_set=("catalog.schema.orders",),
        lever_set=[5],
        target_qids=["gs_001"],
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    b = build_terminal_signature(
        root_cause="r",
        blame_set=["catalog.schema.orders"],
        lever_set=(5,),
        target_qids={"gs_001"},
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    assert a == b
    assert hash(a) == hash(b)
    # And hashable for use in frozenset[TerminalSignature].
    s = frozenset({a, b})
    assert len(s) == 1


def test_signatures_differ_when_terminal_reason_differs():
    a = build_terminal_signature(
        root_cause="r", blame_set=(), lever_set=(5,),
        target_qids=("gs_001",),
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    b = build_terminal_signature(
        root_cause="r", blame_set=(), lever_set=(5,),
        target_qids=("gs_001",),
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    assert a != b


def test_build_canonicalizes_blame_set_and_lever_set_order_insensitive():
    """Two callers passing the same logical inputs in different order
    produce identical signatures (lever_set as a frozenset and
    blame_set_norm as a SORTED tuple)."""
    sig1 = build_terminal_signature(
        root_cause="r",
        blame_set=["catalog.schema.products", "catalog.schema.orders"],
        lever_set=[6, 5, 3],
        target_qids=["gs_002", "gs_001"],
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    sig2 = build_terminal_signature(
        root_cause="r",
        blame_set=["catalog.schema.orders", "catalog.schema.products"],
        lever_set=[3, 5, 6],
        target_qids=["gs_001", "gs_002"],
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    assert sig1 == sig2
    assert sig1.blame_set_norm == (
        "catalog.schema.orders",
        "catalog.schema.products",
    )
    assert sig1.lever_set == frozenset({3, 5, 6})
    assert sig1.target_qids == frozenset({"gs_001", "gs_002"})


def test_normalize_signature_str_lowercases_and_strips():
    assert normalize_signature_str(None) == ""
    assert normalize_signature_str("") == ""
    assert normalize_signature_str("  CapitalCase  ") == "capitalcase"
    assert normalize_signature_str("ALREADY_LOWER") == "already_lower"


def test_root_cause_is_normalized_at_build_time():
    """``build_terminal_signature`` normalizes ``root_cause`` via
    ``normalize_signature_str`` so 'CapitalCase' and 'capitalcase'
    hash equal (spec Section 4.3)."""
    a = build_terminal_signature(
        root_cause="MissingMetricView",
        blame_set=(), lever_set=(5,), target_qids=("gs_001",),
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    b = build_terminal_signature(
        root_cause="missingmetricview",
        blame_set=(), lever_set=(5,), target_qids=("gs_001",),
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    assert a == b
    assert a.root_cause == "missingmetricview"


def test_asdict_serializes_sets_as_canonically_sorted_lists():
    """Spec Section 4.4: ``_asdict()`` produces lever_set as
    numerically ascending list and target_qids as lexicographically
    ascending list, blame_set_norm already sorted tuple."""
    sig = build_terminal_signature(
        root_cause="no_metric_view_for_gross_sales",
        blame_set=["catalog.schema.products", "catalog.schema.orders"],
        lever_set=[6, 5],
        target_qids=["gs_026"],
        terminal_reason=TerminalReason.NO_STRUCTURAL_CANDIDATE,
    )
    d = sig._asdict()
    # JSON-roundtrippable per spec example.
    serialized = json.dumps({
        "root_cause": d["root_cause"],
        "blame_set_norm": list(d["blame_set_norm"]),
        "lever_set": sorted(d["lever_set"]),
        "target_qids": sorted(d["target_qids"]),
        "terminal_reason": d["terminal_reason"],
    })
    parsed = json.loads(serialized)
    assert parsed["root_cause"] == "no_metric_view_for_gross_sales"
    assert parsed["blame_set_norm"] == [
        "catalog.schema.orders",
        "catalog.schema.products",
    ]
    assert parsed["lever_set"] == [5, 6]
    assert parsed["target_qids"] == ["gs_026"]
    assert parsed["terminal_reason"] == "no_structural_candidate"


def test_emitted_patch_shape_enum_values_kept_for_reflection_buffer():
    """EmittedPatchShape stays in this module but is NOT a
    TerminalSignature field (spec Section 4 reserves 5 fields).
    Task 5 records it on the reflection_buffer entry directly, and
    Task 14's structural-repair gate reads it from RCA card
    ``intended_patch_shape`` rather than from the signature."""
    assert EmittedPatchShape.STRUCTURAL == "structural"
    assert EmittedPatchShape.METADATA == "metadata"
    assert EmittedPatchShape.INSTRUCTION == "instruction"
    assert EmittedPatchShape.ABSENT == "absent"


def test_resolve_emitted_patch_shape_helper():
    """The helper classifies a list of applied patches into one
    ``EmittedPatchShape``. Used by Task 5 to populate the
    reflection_buffer entry.

    v4 Task 1.2 replaced the substring classifier with typed PatchSemantic
    reads. The fixtures below use real PatchType enum values rather than
    synthetic substring-matchable names — unknown types now soft-skip
    rather than misclassify silently.
    """
    structural = [{"patch_type": "add_metric_view"}]
    metadata = [{"patch_type": "update_column_description"}]
    instruction = [{"patch_type": "add_instruction"}]
    assert resolve_emitted_patch_shape(structural) == EmittedPatchShape.STRUCTURAL
    assert resolve_emitted_patch_shape(metadata) == EmittedPatchShape.METADATA
    assert resolve_emitted_patch_shape(instruction) == EmittedPatchShape.INSTRUCTION
    assert resolve_emitted_patch_shape([]) == EmittedPatchShape.ABSENT


def test_signature_string_round_trips_stably():
    sig = build_terminal_signature(
        root_cause="missing_metric_view",
        blame_set=["catalog.schema.orders"],
        lever_set=[5, 6],
        target_qids=["gs_001"],
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    s = signature_string(sig)
    assert "catalog.schema.orders" in s
    assert "gs_001" in s
    assert "missing_metric_view" in s
    assert "5,6" in s
    assert "no_applied_patches" in s


def test_empty_inputs_produce_canonical_empty_collections():
    sig = build_terminal_signature(
        root_cause="",
        blame_set=[],
        lever_set=[],
        target_qids=[],
        terminal_reason=TerminalReason.NO_RCA_GROUND,
    )
    assert sig.root_cause == ""
    assert sig.blame_set_norm == ()
    assert sig.lever_set == frozenset()
    assert sig.target_qids == frozenset()
