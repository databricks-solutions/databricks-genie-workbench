"""Trial 24 postmortem-replay gate — kit at source on the e943 anchor.

The faithful e943 replay (source postmortem
``e94376a3-d8a6-4570-a605-9fe231e5f99c``, target
``airline_ticketing_and_fare_analysis_gs_009``, RCA
``extra_defensive_filter``) proposed a CORRECTIVE ``add_instruction``
(not the inert ``add_example_sql``) but emitted it as a LONE lever-5a.
``_check_required_assets`` dropped it as ``UNJUSTIFIED_SINGLE_LEVER`` and
its bundle sibling cascaded out, terminating the cluster with
``survivor_count=0``. Flag-on and flag-off failed identically — it was
NOT a Trial 23 regression.

This gate replays the e943 slate condition through the live production
decision boundary (``compile_slate``) and pins the Trial 24 contract:

  * flag-ON: the corrective ``add_instruction`` AND its grounded
    ``add_sql_snippet_filter`` sibling BOTH survive the slate (the
    positive-criterion proof the bar previously failed).
  * flag-OFF: byte-stable legacy behaviour — the instruction member
    drops as ``unjustified_single_lever`` and nothing survives.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from genie_space_optimizer.optimization.proposal_slate_compiler import (
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

FIXTURE_DIR = Path(__file__).parent / "fixtures"
RUN_E943_FIXTURE = FIXTURE_DIR / "run_e943_231749822620014.json"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def e943_kit() -> dict[str, Any]:
    return _load(RUN_E943_FIXTURE)["trial24_kit_at_source_replay"]


def _proposals_and_ctx(
    f: dict[str, Any],
) -> tuple[list[RepairProposal], SlateCompilerContext]:
    kit = tuple(f["kit"])
    proposals: list[RepairProposal] = []
    assets_by_id: dict[str, list[str]] = {}
    just_by_id: dict[str, str] = {}
    for m in f["members"]:
        proposals.append(
            RepairProposal(
                intent_id=m["intent_id"],
                intent_name=m["intent_id"],
                intent_description="",
                repair_shape=RepairShape.OTHER,
                patch_type=PatchType(m["patch_type"]),
                rationale="r",
                confidence="medium",
                patch_body={"example_sql": m.get("example_sql", "")},
                blame_set=(f["target_qid"],),
                target_qids=(f["target_qid"],),
                selected_lever=kit[0],
                selected_levers=kit,
                bundle_id=f["bundle_id"],
            )
        )
        assets_by_id[m["intent_id"]] = list(m.get("implicated_assets") or [])
        just_by_id[m["intent_id"]] = str(m.get("justification", "") or "")
    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id=assets_by_id,
        justification_by_proposal_id=just_by_id,
    )
    return proposals, ctx


def test_trial24_e943_kit_survives_slate_flag_on(e943_kit, monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    proposals, ctx = _proposals_and_ctx(e943_kit)
    result = compile_slate(proposals, ctx)

    survivor_ids = sorted(p.intent_id for p in result.surviving_proposals)
    exp = e943_kit["expected_flag_on"]
    assert survivor_ids == sorted(exp["surviving_intent_ids"]), (
        "Trial 24 replay gate: the e943 corrective instruction + grounded "
        "snippet kit MUST survive the slate flag-on; got drops: "
        f"{[(p.intent_id, str(dr)) for p, dr in result.dropped_proposals]}"
    )


def test_trial24_e943_instruction_drops_flag_off(e943_kit, monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "0")
    proposals, ctx = _proposals_and_ctx(e943_kit)
    result = compile_slate(proposals, ctx)

    survivor_ids = sorted(p.intent_id for p in result.surviving_proposals)
    exp = e943_kit["expected_flag_off"]
    assert survivor_ids == sorted(exp["surviving_intent_ids"]), (
        "Trial 24 replay gate: flag-off must be byte-stable legacy — the "
        "lone unjustified instruction dies and nothing survives."
    )
    drop_reasons = {
        p.intent_id: str(dr) for p, dr in result.dropped_proposals
    }
    assert (
        drop_reasons.get("e943_instruction") == exp["instruction_drop_reason"]
    ), (
        "Trial 24 replay gate: flag-off, the instruction member must drop as "
        f"unjustified_single_lever; got {drop_reasons}"
    )


# ── Trial 24 Follow-on B — filter-removal solo replay bright-line ─────


@pytest.fixture(scope="module")
def e943_solo() -> dict[str, Any]:
    return _load(RUN_E943_FIXTURE)["trial24_followons_filter_removal_solo_replay"]


def _solo_proposal(f: dict[str, Any]) -> RepairProposal:
    # The DEGRADED synthesis output: a lone add_instruction with the
    # bundle_id cleared (the no-op snippet sibling was dropped at
    # synthesis) carrying a single instruction lever.
    return RepairProposal(
        intent_id=f["intent_id"],
        intent_name=f["intent_id"],
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(f["patch_type"]),
        rationale="r",
        confidence="medium",
        patch_body={},
        blame_set=(f["target_qid"],),
        target_qids=(f["target_qid"],),
        selected_lever=f["selected_lever"],
        selected_levers=(f["selected_lever"],),
        bundle_id="",
    )


def test_trial24_followon_b_solo_instruction_survives_flag_on(
    e943_solo, monkeypatch
) -> None:
    # Flag-on: FB2 grounded the justification at synthesis, so the
    # degraded solo instruction lands without a kit and without cascade.
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id={
            e943_solo["intent_id"]: list(e943_solo["implicated_assets"]),
        },
        justification_by_proposal_id={
            e943_solo["intent_id"]: e943_solo["grounded_justification"],
        },
    )
    result = compile_slate([_solo_proposal(e943_solo)], ctx)
    survivor_ids = sorted(p.intent_id for p in result.surviving_proposals)
    exp = e943_solo["expected_flag_on"]
    assert survivor_ids == sorted(exp["surviving_intent_ids"]), (
        "Follow-on B replay gate: the grounded filter-removal instruction "
        "MUST land solo flag-on; got drops: "
        f"{[(p.intent_id, str(dr)) for p, dr in result.dropped_proposals]}"
    )


def test_trial24_followon_b_solo_instruction_drops_without_grounding(
    e943_solo, monkeypatch
) -> None:
    # Flag-off / no FB2 grounding: the lone instruction carries an empty
    # justification (the pre-Follow-on-B e943 condition) and drops as
    # unjustified_single_lever — byte-stable legacy.
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "0")
    ctx = SlateCompilerContext(
        implicated_assets_by_proposal_id={
            e943_solo["intent_id"]: list(e943_solo["implicated_assets"]),
        },
        justification_by_proposal_id={e943_solo["intent_id"]: ""},
    )
    result = compile_slate([_solo_proposal(e943_solo)], ctx)
    survivor_ids = sorted(p.intent_id for p in result.surviving_proposals)
    exp = e943_solo["expected_flag_off"]
    assert survivor_ids == sorted(exp["surviving_intent_ids"])
    drop_reasons = {
        p.intent_id: str(dr) for p, dr in result.dropped_proposals
    }
    assert (
        drop_reasons.get(e943_solo["intent_id"])
        == exp["instruction_drop_reason"]
    ), (
        "Follow-on B replay gate: ungrounded lone instruction must drop as "
        f"unjustified_single_lever; got {drop_reasons}"
    )
