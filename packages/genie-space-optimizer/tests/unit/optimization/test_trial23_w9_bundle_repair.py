"""Trial 23 W9 — bundle repair over drop.

Trial 22 W2 dropped EVERY member of a same-lever / cohesion-failing
multi-member bundle as ``BUNDLE_INVARIANT_VIOLATED`` — discarding
patches each of which already passed every per-proposal check and is
independently applicable. W9 RECOMPOSES such a bundle: it dissolves the
invalid kit into its independently-valid solo members instead of
dropping them. Only the kit *hypothesis* was wrong, not the members.
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


def _make_proposal(
    *,
    intent_id: str,
    patch_type: str = "add_example_sql",
    qids: tuple[str, ...] = ("qid_a",),
    selected_lever: str = "lever-5",
    bundle_id: str = "",
    patch_body: dict[str, Any] | None = None,
    selected_levers: tuple[str, ...] = (),
) -> RepairProposal:
    if selected_levers:
        levers: tuple[str, ...] = selected_levers
    elif selected_lever:
        levers = (selected_lever,)
    else:
        levers = ()
    return RepairProposal(
        intent_id=intent_id,
        intent_name=f"intent {intent_id}",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(patch_type),
        rationale="r",
        confidence="medium",
        patch_body=patch_body or {},
        blame_set=qids,
        target_qids=qids,
        selected_lever=selected_lever,
        selected_levers=levers,
        bundle_id=bundle_id,
    )


def _same_lever_bundle():
    p1 = _make_proposal(
        intent_id="b9_member_a",
        patch_type="add_example_sql",
        bundle_id="b9",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
    )
    p2 = _make_proposal(
        intent_id="b9_member_b",
        patch_type="add_example_sql",
        bundle_id="b9",
        selected_lever="lever-5",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q2", "example_sql": "SELECT 2"},
    )
    return [p1, p2]


def test_same_lever_bundle_recomposes_to_solo_by_default():
    result = compile_slate(_same_lever_bundle(), SlateCompilerContext())
    # Both members survive — as SOLO proposals (bundle_id cleared).
    survivors = {p.intent_id: p for p in result.surviving_proposals}
    assert set(survivors) == {"b9_member_a", "b9_member_b"}, (
        f"W9 must recompose same-lever bundle to solo, not drop. "
        f"Drops: {result.dropped_proposals}"
    )
    for p in result.surviving_proposals:
        assert p.bundle_id == "", (
            "recomposition must clear bundle_id so the group invariant "
            "never re-fires downstream"
        )
    assert result.dropped_proposals == ()


def test_recompose_emits_marker_per_member():
    result = compile_slate(_same_lever_bundle(), SlateCompilerContext())
    recomposed = [
        m
        for m in result.actuator_markers
        if m.get("marker") == "GSO_TRIAL23_BUNDLE_RECOMPOSED_V1"
    ]
    assert len(recomposed) == 2, (
        f"expected one recompose marker per member, got "
        f"{result.actuator_markers}"
    )
    assert {m.get("proposal_id") for m in recomposed} == {
        "b9_member_a",
        "b9_member_b",
    }
    assert all(m.get("bundle_id") == "b9" for m in recomposed)
    assert all(
        m.get("reason") == "same_lever_bundle_recomposed_to_solo"
        for m in recomposed
    )


def test_rollback_drops_same_lever_bundle_when_flag_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL23_BUNDLE_REPAIR", "0")
    result = compile_slate(_same_lever_bundle(), SlateCompilerContext())
    assert result.surviving_proposals == ()
    assert [r for _, r in result.dropped_proposals] == [
        DropReason.BUNDLE_INVARIANT_VIOLATED,
        DropReason.BUNDLE_INVARIANT_VIOLATED,
    ]


def test_master_flag_off_also_drops(monkeypatch):
    # The master Trial 23 gate forces every sub-flag off.
    monkeypatch.setenv("GSO_TRIAL23_LOOP_REPAIR", "0")
    result = compile_slate(_same_lever_bundle(), SlateCompilerContext())
    assert result.surviving_proposals == ()
    assert all(
        r == DropReason.BUNDLE_INVARIANT_VIOLATED
        for _, r in result.dropped_proposals
    )


def test_valid_multi_lever_bundle_still_kept_intact():
    # A genuine >=2-family bundle is untouched by W9 — it stays a bundle.
    p1 = _make_proposal(
        intent_id="ok_a",
        patch_type="add_example_sql",
        bundle_id="okb",
        selected_levers=("lever-5",),
        patch_body={"example_question": "q", "example_sql": "SELECT 1"},
    )
    p2 = _make_proposal(
        intent_id="ok_b",
        patch_type="add_instruction",
        bundle_id="okb",
        selected_levers=("lever-1",),
        patch_body={"content": "Use order_id consistently."},
    )
    result = compile_slate([p1, p2], SlateCompilerContext())
    survivors = {p.intent_id: p for p in result.surviving_proposals}
    assert set(survivors) == {"ok_a", "ok_b"}
    # bundle_id preserved (NOT recomposed) — it is a valid kit.
    assert all(p.bundle_id == "okb" for p in result.surviving_proposals)
    recomposed = [
        m
        for m in result.actuator_markers
        if m.get("marker") == "GSO_TRIAL23_BUNDLE_RECOMPOSED_V1"
    ]
    assert recomposed == []
