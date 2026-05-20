"""Phase 2.3 + spec Section 8.4 — structural-repair-shape gate.

Read ``rca_card.intended_patch_shape``. If shape is ``structural``
and surviving patches contain NO member of the causal patch families
(L5 example SQL, narrow L6 SQL, join/routing rule, grain fix), reject
the iteration with TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.

The verdict carries a :class:`RepairabilityScore` for downstream
marker emission and DecisionRecord payload.
"""
from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.repairability_score import (
    RepairabilityScore,
    compute_repairability,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
)


@dataclass(frozen=True, slots=True)
class StructuralRepairGateVerdict:
    outcome: str  # "admitted" | "rejected"
    terminal_reason: str  # empty when admitted
    repairability: RepairabilityScore | None = None

    @classmethod
    def admitted(cls, score: RepairabilityScore | None = None) -> "StructuralRepairGateVerdict":
        return cls(outcome="admitted", terminal_reason="", repairability=score)


ADMITTED = StructuralRepairGateVerdict(outcome="admitted", terminal_reason="")
_REJECTED = StructuralRepairGateVerdict(
    outcome="rejected",
    terminal_reason=TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value,
)

# Workaround for dataclass class-level access in tests (we expose
# the constants on the class):
StructuralRepairGateVerdict.ADMITTED = ADMITTED  # type: ignore[attr-defined]
StructuralRepairGateVerdict.REJECTED = _REJECTED  # type: ignore[attr-defined]


def enforce_structural_repair_shape(
    *,
    intended_patch_shape: str,
    emitted_patch_shape: EmittedPatchShape,
    narrow_replacement_available: bool = False,
) -> StructuralRepairGateVerdict:
    """Plan 9 Task 7 — rejection priority:

      1. ABSENT emitted + 0.0 repairability → REJECT regardless of
         intent (closes the 7Now fail-open bug).
      2. intent == 'structural' AND emitted != STRUCTURAL → REJECT
         (legacy rule).
      3. Otherwise → ADMIT (legacy fail-open for non-structural intent
         or for legacy RCA cards without Phase-2.3 metadata, IFF
         emitted shape is non-ABSENT).
    """
    score = compute_repairability(
        intended_patch_shape=intended_patch_shape,
        emitted_patch_shape=emitted_patch_shape,
        narrow_replacement_available=narrow_replacement_available,
    )
    intent = str(intended_patch_shape or "").strip().lower()

    # Plan 9 — degenerate ABSENT emission: 0.0 repairability, or legacy
    # empty intent (compute_repairability fail-open returns 1.0 for "").
    if emitted_patch_shape == EmittedPatchShape.ABSENT and (
        score.value == 0.0 or not intent
    ):
        return StructuralRepairGateVerdict(
            outcome="rejected",
            terminal_reason=(
                TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value
            ),
            repairability=score,
        )

    # Pre-Plan-9 — structural intent must match structural emitted.
    if intent == "structural" and emitted_patch_shape != EmittedPatchShape.STRUCTURAL:
        return StructuralRepairGateVerdict(
            outcome="rejected",
            terminal_reason=(
                TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value
            ),
            repairability=score,
        )

    return StructuralRepairGateVerdict(
        outcome="admitted",
        terminal_reason="",
        repairability=score,
    )
