"""Phase 2.3 — structural-repair-shape gate."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
)
from genie_space_optimizer.optimization.structural_repair_gate import (
    enforce_structural_repair_shape,
    StructuralRepairGateVerdict,
)


def test_non_structural_intended_shape_returns_admitted():
    """When intended_patch_shape != 'structural', the gate is a
    no-op."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="metadata",
        emitted_patch_shape=EmittedPatchShape.METADATA,
    )
    assert verdict.outcome == StructuralRepairGateVerdict.ADMITTED.outcome


def test_structural_intended_with_structural_emitted_admitted():
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
    )
    assert verdict.outcome == "admitted"


def test_structural_intended_with_metadata_emitted_rejected():
    """The key 7now iter-1 case: structural intent, metadata
    emission → reject before full eval."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.METADATA,
    )
    assert verdict.outcome == "rejected"
    assert verdict.terminal_reason == "structural_gate_dropped_instruction_only"


def test_structural_intended_with_instruction_emitted_rejected():
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.INSTRUCTION,
    )
    assert verdict.outcome == "rejected"
    assert verdict.terminal_reason == "structural_gate_dropped_instruction_only"


def test_structural_intended_with_absent_emitted_rejected():
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
    )
    assert verdict.outcome == "rejected"
    assert verdict.terminal_reason == "structural_gate_dropped_instruction_only"


def test_empty_intended_shape_admitted():
    """Missing intended_patch_shape (legacy RCA card without
    Phase 2.3 metadata) is admitted — the gate fails OPEN to
    preserve legacy behavior on fixtures."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="",
        emitted_patch_shape=EmittedPatchShape.METADATA,
    )
    assert verdict.outcome == "admitted"
