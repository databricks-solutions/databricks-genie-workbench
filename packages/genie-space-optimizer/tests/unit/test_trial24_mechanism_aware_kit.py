"""Trial 24 Follow-on A — mechanism-aware kit detection.

The live e943 replay reproduced a kit the LLM emitted correctly as a
shared ``bundle_id`` of ``add_instruction`` + ``add_sql_snippet_filter``
— but tagged BOTH members ``lever-5``. The declared-lever union was then
``{lever-5}`` (< 2), so neither the W24.3 waiver pre-scan nor the Phase-2
bundle invariant recognised the kit, the instruction dropped as
``UNJUSTIFIED_SINGLE_LEVER``, and the snippet sibling cascaded out.

Follow-on A adds a mechanism-derived acceptance path: the bundle spans
two distinct ``patch_type`` mechanisms (INSTRUCTION_TEXT + SQL_SNIPPET)
regardless of the mis-tagged levers, so it is recognised as a kit when
``GSO_TRIAL24_MECHANISM_AWARE_KIT`` is on (default ON under the master).
The lever path is never weakened — these tests pin both the new
mechanism path (flag-on) and byte-stable legacy behaviour (flag-off).
"""
from __future__ import annotations

from typing import Any

from genie_space_optimizer.optimization.proposal_slate_compiler import (
    DropReason,
    SlateCompilerContext,
    _bundle_distinct_mechanisms,
    compile_slate,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


def _member(
    *,
    intent_id: str,
    patch_type: str,
    lever: str,
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
        # The e943 mis-tag: BOTH members carry the SAME single lever, so
        # the lever union is length 1 and cannot trigger the kit.
        selected_lever=lever,
        selected_levers=(lever,),
        bundle_id="kit_bundle_1",
    )


def _mistagged_kit() -> list[RepairProposal]:
    instruction = _member(
        intent_id="kit_instruction",
        patch_type="add_instruction",
        lever="lever-5",
    )
    snippet = _member(
        intent_id="kit_snippet",
        patch_type="add_sql_snippet_filter",
        lever="lever-5",
        patch_body={"example_sql": "WHERE status <> 'cancelled'"},
    )
    return [instruction, snippet]


def _ctx() -> SlateCompilerContext:
    # Instruction has NO justification (the e943 condition); the snippet
    # carries its grounded implicated asset.
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


def test_bundle_distinct_mechanisms_spans_two_families() -> None:
    mechs = _bundle_distinct_mechanisms(_mistagged_kit())
    assert mechs == {"instruction_text", "sql_snippet"}


def test_mistagged_kit_survives_flag_on(monkeypatch) -> None:
    # Master ON (mechanism-aware sub-flag defaults ON): the bundle is a
    # kit by mechanism even though the lever union is {lever-5} (< 2),
    # so the instruction is waived and both members survive.
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    monkeypatch.delenv("GSO_TRIAL24_MECHANISM_AWARE_KIT", raising=False)
    result = compile_slate(_mistagged_kit(), _ctx())
    survivor_ids = {p.intent_id for p in result.surviving_proposals}
    assert survivor_ids == {"kit_instruction", "kit_snippet"}, (
        "mechanism-derived kit detection must recognise the bundle and "
        "waive the instruction even when both members are mis-tagged "
        f"lever-5; got drops: "
        f"{[(p.intent_id, dr) for p, dr in result.dropped_proposals]}"
    )


def test_mistagged_kit_drops_when_mechanism_subflag_off(monkeypatch) -> None:
    # Master ON but the mechanism-aware sub-flag explicitly OFF: the
    # lever union is < 2 and there is no mechanism fallback, so the
    # unjustified instruction drops (byte-stable with pre-Follow-on-A).
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    monkeypatch.setenv("GSO_TRIAL24_MECHANISM_AWARE_KIT", "0")
    result = compile_slate(_mistagged_kit(), _ctx())
    drop_reasons = {p.intent_id: dr for p, dr in result.dropped_proposals}
    assert (
        drop_reasons.get("kit_instruction")
        == DropReason.UNJUSTIFIED_SINGLE_LEVER
    )
    assert "kit_instruction" not in {
        p.intent_id for p in result.surviving_proposals
    }


def test_mistagged_kit_drops_flag_off(monkeypatch) -> None:
    # Master OFF entirely: legacy path, unjustified instruction drops.
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "0")
    monkeypatch.delenv("GSO_TRIAL24_MECHANISM_AWARE_KIT", raising=False)
    result = compile_slate(_mistagged_kit(), _ctx())
    drop_reasons = {p.intent_id: dr for p, dr in result.dropped_proposals}
    assert (
        drop_reasons.get("kit_instruction")
        == DropReason.UNJUSTIFIED_SINGLE_LEVER
    )
