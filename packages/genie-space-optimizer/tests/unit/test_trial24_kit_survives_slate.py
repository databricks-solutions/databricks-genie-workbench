"""Trial 24 W24.3/W24.4 — kit survives the slate compiler.

The e943 gap: a corrective ``add_instruction`` shipped as a lone single
lever and was dropped by ``_check_required_assets`` as
``UNJUSTIFIED_SINGLE_LEVER`` before the bundle-invariants group check
could see it was part of a kit; its snippet sibling then cascaded out and
the cluster terminated with ``survivor_count=0``.

Trial 24 fixes this at source (KIT_FOR_RCA forces the kit; the prompt
emits it) and with the kit-aware ``required_assets`` waiver (the kit IS
the justification). These tests pin the slate behaviour:

  * flag-ON: the instruction member (empty justification) AND its
    grounded snippet sibling both survive the slate.
  * flag-OFF: the instruction member drops as
    ``UNJUSTIFIED_SINGLE_LEVER`` (byte-stable legacy behaviour).
"""
from __future__ import annotations

from typing import Any

from genie_space_optimizer.optimization.proposal_slate_compiler import (
    DropReason,
    SlateCompilerContext,
    compile_slate,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


_KIT = ("lever-5a", "lever-6")


def _kit_member(
    *,
    intent_id: str,
    patch_type: str,
    patch_body: dict[str, Any] | None = None,
) -> RepairProposal:
    return RepairProposal(
        intent_id=intent_id,
        intent_name=f"intent {intent_id}",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(patch_type),
        rationale="r",
        confidence="medium",
        patch_body=patch_body or {},
        blame_set=("qid_a",),
        target_qids=("qid_a",),
        selected_lever="lever-5a",
        # Every kit member carries the FULL kit list (Trial 24 prompt
        # contract) so effective_selected_levers() yields >= 2 distinct.
        selected_levers=_KIT,
        bundle_id="kit_bundle_1",
    )


def _kit_pair() -> list[RepairProposal]:
    instruction = _kit_member(
        intent_id="kit_instruction",
        patch_type="add_instruction",
    )
    snippet = _kit_member(
        intent_id="kit_snippet",
        patch_type="add_sql_snippet_filter",
        patch_body={"example_sql": "WHERE status <> 'cancelled'"},
    )
    return [instruction, snippet]


def _ctx() -> SlateCompilerContext:
    # Instruction has NO justification (the e943 condition); the snippet
    # carries its grounded implicated asset (W5 grounding supplies this
    # from effective_blame_set in production).
    return SlateCompilerContext(
        implicated_assets_by_proposal_id={
            "kit_instruction": [],
            "kit_snippet": ["main.airline.fares.status"],
        },
        justification_by_proposal_id={
            "kit_instruction": "",
            "kit_snippet": "override the defensive WHERE clause",
        },
    )


def test_kit_survives_slate_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    result = compile_slate(_kit_pair(), _ctx())
    survivor_ids = {p.intent_id for p in result.surviving_proposals}
    assert survivor_ids == {"kit_instruction", "kit_snippet"}, (
        f"expected the full kit to survive, got drops: "
        f"{[(p.intent_id, dr) for p, dr in result.dropped_proposals]}"
    )


def test_instruction_member_dropped_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "0")
    result = compile_slate(_kit_pair(), _ctx())
    drop_reasons = {
        p.intent_id: dr for p, dr in result.dropped_proposals
    }
    # Legacy: the unjustified instruction member dies at Phase 1.
    assert (
        drop_reasons.get("kit_instruction")
        == DropReason.UNJUSTIFIED_SINGLE_LEVER
    )
    assert "kit_instruction" not in {
        p.intent_id for p in result.surviving_proposals
    }


def test_kit_survives_with_distinct_single_levers_per_member(
    monkeypatch,
) -> None:
    # Realistic production shape: the LLM gives each bundle member ONE
    # lever (instruction=lever-5a, snippet=lever-6) rather than the full
    # kit list on every member. The bundle-derived pre-scan must still
    # recognise the >= 2-lever kit and waive the instruction member.
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    instruction = RepairProposal(
        intent_id="kit_instruction",
        intent_name="i",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType("add_instruction"),
        rationale="r",
        confidence="medium",
        patch_body={},
        blame_set=("qid_a",),
        target_qids=("qid_a",),
        selected_lever="lever-5a",
        selected_levers=("lever-5a",),
        bundle_id="kit_bundle_1",
    )
    snippet = RepairProposal(
        intent_id="kit_snippet",
        intent_name="s",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType("add_sql_snippet_filter"),
        rationale="r",
        confidence="medium",
        patch_body={"example_sql": "WHERE status <> 'cancelled'"},
        blame_set=("qid_a",),
        target_qids=("qid_a",),
        selected_lever="lever-6",
        selected_levers=("lever-6",),
        bundle_id="kit_bundle_1",
    )
    result = compile_slate([instruction, snippet], _ctx())
    survivor_ids = {p.intent_id for p in result.surviving_proposals}
    assert survivor_ids == {"kit_instruction", "kit_snippet"}, (
        "bundle-derived kit detection must waive the instruction member "
        "even when members carry distinct SINGLE levers; got drops: "
        f"{[(p.intent_id, dr) for p, dr in result.dropped_proposals]}"
    )


def test_waiver_subflag_off_keeps_strict_gate(monkeypatch) -> None:
    # Master ON but the waiver sub-flag explicitly OFF: the kit is still
    # forced at synthesis (W24.1) but the per-proposal justification gate
    # stays strict, so the instruction member drops. This isolates the
    # waiver as the landing mechanism.
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    monkeypatch.setenv("GSO_TRIAL24_REQUIRED_ASSETS_KIT_WAIVER", "0")
    result = compile_slate(_kit_pair(), _ctx())
    drop_reasons = {
        p.intent_id: dr for p, dr in result.dropped_proposals
    }
    assert (
        drop_reasons.get("kit_instruction")
        == DropReason.UNJUSTIFIED_SINGLE_LEVER
    )
