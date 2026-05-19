"""Plan 1 Task 9 — AppliedPatch.intent_id + AppliedPatchSet carrier.

The applier already produces apply-log entries whose ``patch`` dict
carries the original proposal's keys (including ``intent_id`` after
Task 8 stamping). AppliedPatch reads the field and AppliedPatchSet
exposes a typed ``applied_by_intent_id`` rollup so the acceptance and
learning stages can key on intent_id directly.
"""

from __future__ import annotations

from types import SimpleNamespace

from genie_space_optimizer.optimization.stages.application import (
    AppliedPatch,
    ApplicationInput,
    AppliedPatchSet,
    apply,
)


def _ctx():
    emitted: list = []
    return SimpleNamespace(
        run_id="run",
        iteration=1,
        decision_emit=lambda r: emitted.append(r),
        _emitted=emitted,
    )


def test_applied_patch_has_intent_id_field() -> None:
    p = AppliedPatch(
        proposal_id="p1",
        ag_id="AG_X",
        patch_type="add_example_sql",
        target_qids=("gs_009",),
    )
    assert hasattr(p, "intent_id")
    assert p.intent_id == ""


def test_applied_patch_round_trip_preserves_intent_id() -> None:
    p = AppliedPatch(
        proposal_id="p1",
        ag_id="AG_X",
        patch_type="add_example_sql",
        target_qids=("gs_009",),
        intent_id="i1",
    )
    out = AppliedPatchSet(applied=(p,))
    restored = AppliedPatchSet.from_json(out.to_json())
    assert restored.applied[0].intent_id == "i1"


def test_apply_reads_intent_id_from_patch_dict() -> None:
    inp = ApplicationInput(
        applied_entries_by_ag={
            "AG_X": (
                {
                    "patch": {
                        "proposal_id": "p1",
                        "patch_type": "add_example_sql",
                        "target_qids": ["gs_009"],
                        "intent_id": "i1",
                    }
                },
            )
        },
        ags=({"ag_id": "AG_X"},),
    )
    out = apply(_ctx(), inp)
    assert len(out.applied) == 1
    assert out.applied[0].intent_id == "i1"


def test_applied_patch_set_exposes_applied_by_intent_id() -> None:
    p1 = AppliedPatch(
        proposal_id="p1", ag_id="AG_X",
        patch_type="add_example_sql", target_qids=("gs_009",), intent_id="i1",
    )
    p2 = AppliedPatch(
        proposal_id="p2", ag_id="AG_X",
        patch_type="add_example_sql", target_qids=("gs_010",), intent_id="i2",
    )
    p_legacy = AppliedPatch(
        proposal_id="p_legacy", ag_id="AG_X",
        patch_type="add_example_sql", target_qids=("gs_011",),
    )
    out = AppliedPatchSet(applied=(p1, p2, p_legacy))
    assert set(out.applied_by_intent_id) == {"i1", "i2"}
    assert out.applied_by_intent_id["i1"] is p1


def test_legacy_apply_entry_without_intent_id_records_empty_string() -> None:
    """Plan 1 tolerates legacy entries — intent_id defaults to ''."""
    inp = ApplicationInput(
        applied_entries_by_ag={
            "AG_X": (
                {
                    "patch": {
                        "proposal_id": "p_legacy",
                        "patch_type": "add_example_sql",
                        "target_qids": ["gs_009"],
                    }
                },
            )
        },
        ags=({"ag_id": "AG_X"},),
    )
    out = apply(_ctx(), inp)
    assert out.applied[0].intent_id == ""
    assert "p_legacy" not in out.applied_by_intent_id
