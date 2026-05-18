"""WU-5 — harness-level backstop around ``enforce_structural_repair_shape``.

The structural repair gate at ``structural_repair_gate.py`` fails OPEN
(admits) when ``intended_patch_shape`` is empty because legacy RCA
cards without Phase 2.3 metadata should not be penalized.

In production we observed (7now iter-1 attempt-11) that a patch with
``intended_patch_shape == "" AND rca_root_cause == ""`` was admitted
and burned the iteration's L6 budget. This module overrides the
verdict to REJECTED when BOTH fields are empty — a strictly narrower
condition than the gate's empty-intent fall-through.

Pure. No I/O. Caller is responsible for emitting the
``GSO_STRUCTURAL_REPAIR_DECISION_V1`` marker and the paired
DecisionRecord using the returned verdict (the existing harness code
path).
"""
from __future__ import annotations

from genie_space_optimizer.optimization.structural_repair_gate import (
    StructuralRepairGateVerdict,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason


def apply_empty_shape_backstop(
    *,
    verdict: StructuralRepairGateVerdict,
    intended_patch_shape: str,
    rca_root_cause: str,
) -> StructuralRepairGateVerdict:
    """Return a possibly-overridden verdict.

    Override conditions (ALL must hold):
      * The WU-5 flag (``GSO_STRUCTURAL_GATE_GUARD_EMPTY_SHAPE``) is on.
      * The gate's verdict is currently ``"admitted"`` (we never
        unrelax an existing rejection).
      * Both ``intended_patch_shape`` AND ``rca_root_cause`` are
        empty/whitespace.

    When overridden, the returned verdict carries
    ``terminal_reason=TerminalReason.NO_RCA_GROUND``. The repairability
    score is preserved unchanged so downstream marker payloads stay
    consistent.

    When not overridden, returns the input ``verdict`` by identity (no
    copy) so callers can use ``is``-checks for short-circuit behavior.
    """
    from genie_space_optimizer.common.config import (
        structural_gate_guard_empty_shape_enabled,
    )
    if not structural_gate_guard_empty_shape_enabled():
        return verdict
    if verdict.outcome != "admitted":
        return verdict
    if str(intended_patch_shape or "").strip():
        return verdict
    if str(rca_root_cause or "").strip():
        return verdict
    return StructuralRepairGateVerdict(
        outcome="rejected",
        terminal_reason=TerminalReason.NO_RCA_GROUND.value,
        repairability=verdict.repairability,
    )
