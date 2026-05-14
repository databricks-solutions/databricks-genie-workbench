"""Spec Section 8.4 — RepairabilityScore.

Scalar diagnostic (0.0-1.0) of how structurally repairable an
iteration's surviving patch set is for the AG's intended shape.
Consumed by ``structural_repair_gate.py`` and emitted in the
GSO_STRUCTURAL_REPAIR_DECISION_V1 marker payload.
"""
from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
)


@dataclass(frozen=True, slots=True)
class RepairabilityScore:
    value: float
    """In [0.0, 1.0]. 1.0 = fully repairable, 0.0 = unrepairable."""
    intended_shape: str
    emitted_shape: str
    narrow_replacement_available: bool

    def to_jsonable(self) -> dict[str, object]:
        return {
            "value": float(self.value),
            "intended_shape": self.intended_shape,
            "emitted_shape": self.emitted_shape,
            "narrow_replacement_available": bool(
                self.narrow_replacement_available
            ),
        }


def compute_repairability(
    *,
    intended_patch_shape: str,
    emitted_patch_shape: EmittedPatchShape,
    narrow_replacement_available: bool,
) -> RepairabilityScore:
    """Scoring table:

      * intended == "" (legacy)           → 1.0 (fail OPEN)
      * intended == emitted shape          → 1.0
      * intended structural, emitted not,
        narrow replacement available       → 0.5
      * intended structural, emitted not,
        no narrow replacement              → 0.0
      * otherwise                          → 0.0
    """
    intent = str(intended_patch_shape or "").strip().lower()
    emitted_str = emitted_patch_shape.value if hasattr(
        emitted_patch_shape, "value"
    ) else str(emitted_patch_shape)

    if not intent:
        value = 1.0
    elif intent == emitted_str:
        value = 1.0
    elif intent == "structural" and emitted_str != "structural":
        value = 0.5 if narrow_replacement_available else 0.0
    else:
        value = 0.0

    return RepairabilityScore(
        value=max(0.0, min(1.0, value)),
        intended_shape=intent,
        emitted_shape=emitted_str,
        narrow_replacement_available=bool(narrow_replacement_available),
    )
