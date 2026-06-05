"""Trial 24 Follow-on B — filter-removal solo survives the slate.

The live e943 replay showed the ``extra_defensive_filter`` corrective
``add_instruction`` dying because (1) it shipped with an empty
justification and (2) its no-op ``1=1`` snippet sibling was rejected,
cascading the instruction out via the Phase-1.5 cohesion sweep.

Follow-on B drops the no-op snippet AT SYNTHESIS and emits the
instruction SOLO (``bundle_id`` cleared) with a grounded justification.
These tests pin the downstream slate contract for that degraded output:
a lone, grounded instruction must SURVIVE (no cascade), and a lone
instruction with NO justification must still drop (the grounding is what
lands it).
"""
from __future__ import annotations

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


def _solo_instruction() -> RepairProposal:
    # The degrade-to-solo output: an add_instruction with NO bundle_id
    # (the no-op snippet sibling was dropped at synthesis) and a single
    # instruction lever.
    return RepairProposal(
        intent_id="corrective_instruction",
        intent_name="do not inject defensive filter",
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
        bundle_id="",
    )


def _ctx(justification: str) -> SlateCompilerContext:
    return SlateCompilerContext(
        implicated_assets_by_proposal_id={
            "corrective_instruction": ["main.airline.fares.status"],
        },
        justification_by_proposal_id={
            "corrective_instruction": justification,
        },
    )


def test_grounded_solo_instruction_survives_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    # FB2 grounds the justification from expected_behavioral_change /
    # rationale; here the ctx already carries the grounded text.
    result = compile_slate(
        [_solo_instruction()],
        _ctx("stop emitting the unrequested status <> 'cancelled' filter"),
    )
    survivor_ids = {p.intent_id for p in result.surviving_proposals}
    assert survivor_ids == {"corrective_instruction"}, (
        "a grounded filter-removal instruction must land solo; got drops: "
        f"{[(p.intent_id, dr) for p, dr in result.dropped_proposals]}"
    )


def test_ungrounded_solo_instruction_still_drops(monkeypatch) -> None:
    # The grounding is what lands it: an EMPTY justification still drops
    # as UNJUSTIFIED_SINGLE_LEVER (this is the e943 failure FB2 fixes by
    # supplying a grounded justification at synthesis).
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    result = compile_slate([_solo_instruction()], _ctx(""))
    drop_reasons = {p.intent_id: dr for p, dr in result.dropped_proposals}
    assert (
        drop_reasons.get("corrective_instruction")
        == DropReason.UNJUSTIFIED_SINGLE_LEVER
    )
