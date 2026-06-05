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
    outcome: str  # "admitted" | "rejected" | "retry_with_typed_feedback"
    terminal_reason: str  # empty when admitted / retry
    repairability: RepairabilityScore | None = None
    # Trial 19 B4 — typed feedback string returned to Stage 3 when the
    # verdict is ``retry_with_typed_feedback``. Empty otherwise.
    retry_feedback: str = ""

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


# Trial 19 B4 — typed retry verdict outcome string. The dispatch loop
# already understands Trial 17's retry pattern; this constant gives
# postmortem readers a stable label to filter on.
RETRY_WITH_TYPED_FEEDBACK = "retry_with_typed_feedback"


def _build_typed_retry_feedback(intended_patch_shape: str) -> str:
    """Trial 19 B4 — typed feedback string returned to Stage 3.

    The wording names the specific intent the LLM emitted so the
    retry has a concrete target. Keep stable: workbench invariant G3
    asserts the verdict carries this feedback verbatim.
    """
    return (
        "emitted shape was absent or prose-only; emit a concrete "
        "SQL-shape patch (e.g., add_sql_snippet_filter, "
        "update_metric_view_definition, add_table_metadata_join_spec) "
        f"that realizes the named intent {intended_patch_shape!r}."
    )


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

    Trial 19 B4 — when the Trial 19 LLM-first RCA flag is ON AND the
    LLM emitted a non-empty free-text ``intended_patch_shape`` AND the
    emitted patch was ABSENT, return a ``retry_with_typed_feedback``
    verdict instead of a terminal rejection. The dispatch loop reuses
    Trial 17's retry pattern to ask Stage 3 to re-emit a concrete
    SQL-shape patch that realizes the named intent. Falls back to
    legacy reject behavior when the flag is OFF or when the intent is
    empty / equal to the legacy "structural" sentinel.
    """
    score = compute_repairability(
        intended_patch_shape=intended_patch_shape,
        emitted_patch_shape=emitted_patch_shape,
        narrow_replacement_available=narrow_replacement_available,
    )
    intent_raw = str(intended_patch_shape or "").strip()
    intent_lower = intent_raw.lower()

    is_absent = emitted_patch_shape == EmittedPatchShape.ABSENT

    # Trial 19 B4 — preferred branch when the LLM named a concrete
    # repair intent (anything other than empty or the legacy
    # ``"structural"`` sentinel) AND we got an ABSENT emission. Ask
    # for a re-emit with typed feedback instead of terminating.
    if is_absent and intent_raw and intent_lower != "structural":
        try:
            from genie_space_optimizer.optimization.trial19_flags import (
                trial19_llm_first_rca_enabled,
            )
            flag_on = trial19_llm_first_rca_enabled()
        except Exception:
            flag_on = False
        if flag_on:
            return StructuralRepairGateVerdict(
                outcome=RETRY_WITH_TYPED_FEEDBACK,
                terminal_reason="",
                repairability=score,
                retry_feedback=_build_typed_retry_feedback(intent_raw),
            )

    # Plan 9 — degenerate ABSENT emission: 0.0 repairability, or legacy
    # empty intent (compute_repairability fail-open returns 1.0 for "").
    if is_absent and (score.value == 0.0 or not intent_raw):
        return StructuralRepairGateVerdict(
            outcome="rejected",
            terminal_reason=(
                TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value
            ),
            repairability=score,
        )

    # Pre-Plan-9 — structural intent must match structural emitted.
    if intent_lower == "structural" and emitted_patch_shape != EmittedPatchShape.STRUCTURAL:
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
