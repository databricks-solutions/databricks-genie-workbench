"""Lever-qualified expanded patch ids.

Today _stamp_expanded_patch_identity builds child ids as
``{parent_id}#{child_index}``. Two parents with the same proposal_id
but different levers (e.g. P001 lever=1 + P001 lever=5) both produce
``P001#2``. This collision was observed in run 2423b960 iter 1 where
the CAP RECONCILIATION printed ``P001#1, P001#2, P001#2`` for a Lever-1
column-synonym and a Lever-5 instruction-section. This suite pins the
new ``L{lever}:{parent_id}#{child_index}`` format.
"""
from __future__ import annotations


def test_lever_qualified_id_when_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_LEVER_QUALIFIED_PATCH_IDS", "1")
    from genie_space_optimizer.optimization.applier import (
        _stamp_expanded_patch_identity,
    )

    proposal = {"proposal_id": "P001"}
    patch = {"lever": 1}
    _stamp_expanded_patch_identity(patch, proposal, child_index=2)
    assert patch["expanded_patch_id"] == "L1:P001#2"
    assert patch["proposal_id"] == "L1:P001#2"
    assert patch["parent_proposal_id"] == "P001"


def test_two_parents_same_id_different_lever_produce_distinct_ids(monkeypatch) -> None:
    """The 7Now reproducer: AG1 had P001 Lever-1 (column synonym) AND
    P001 Lever-5 (instruction section), and both produced child #2.
    With the flag on, they must diverge.
    """
    monkeypatch.setenv("GSO_LEVER_QUALIFIED_PATCH_IDS", "1")
    from genie_space_optimizer.optimization.applier import (
        _stamp_expanded_patch_identity,
    )

    proposal_l1 = {"proposal_id": "P001"}
    proposal_l5 = {"proposal_id": "P001"}
    patch_l1 = {"lever": 1}
    patch_l5 = {"lever": 5}

    _stamp_expanded_patch_identity(patch_l1, proposal_l1, child_index=2)
    _stamp_expanded_patch_identity(patch_l5, proposal_l5, child_index=2)
    assert patch_l1["expanded_patch_id"] != patch_l5["expanded_patch_id"]
    assert patch_l1["expanded_patch_id"] == "L1:P001#2"
    assert patch_l5["expanded_patch_id"] == "L5:P001#2"


def test_missing_lever_falls_back_to_unqualified(monkeypatch) -> None:
    """Defensive: if the patch has no ``lever`` field, the
    qualifier prefix is omitted so we don't accidentally produce
    ``L:P001#2``.
    """
    monkeypatch.setenv("GSO_LEVER_QUALIFIED_PATCH_IDS", "1")
    from genie_space_optimizer.optimization.applier import (
        _stamp_expanded_patch_identity,
    )

    proposal = {"proposal_id": "P001"}
    patch = {}  # no lever
    _stamp_expanded_patch_identity(patch, proposal, child_index=1)
    assert patch["expanded_patch_id"] == "P001#1"


def test_zero_lever_is_qualified(monkeypatch) -> None:
    """Lever 0 is a valid lever; falsy-check must use ``is None``,
    not bool truthiness.
    """
    monkeypatch.setenv("GSO_LEVER_QUALIFIED_PATCH_IDS", "1")
    from genie_space_optimizer.optimization.applier import (
        _stamp_expanded_patch_identity,
    )

    proposal = {"proposal_id": "P001"}
    patch = {"lever": 0}
    _stamp_expanded_patch_identity(patch, proposal, child_index=1)
    assert patch["expanded_patch_id"] == "L0:P001#1"


def test_patch_selection_proposal_id_prefers_expanded_over_proposal_id() -> None:
    """When both fields are set and they disagree (the post-stamp
    state on conversion), prefer expanded_patch_id so downstream
    selection / dedup paths see the lever-qualified form.
    """
    from genie_space_optimizer.optimization.patch_selection import (
        _proposal_id,
    )
    patch = {
        "proposal_id": "P001#2",  # legacy
        "expanded_patch_id": "L1:P001#2",  # lever-qualified
    }
    assert _proposal_id(patch, index=0) == "L1:P001#2"


def test_patch_selection_proposal_id_falls_back_to_proposal_id() -> None:
    from genie_space_optimizer.optimization.patch_selection import (
        _proposal_id,
    )
    patch = {"proposal_id": "P001#2"}
    assert _proposal_id(patch, index=0) == "P001#2"


def test_static_judge_replay_proposal_id_prefers_expanded() -> None:
    from genie_space_optimizer.optimization.static_judge_replay import (
        _proposal_id,
    )
    item = {
        "proposal_id": "P001#2",
        "expanded_patch_id": "L1:P001#2",
    }
    assert _proposal_id(item) == "L1:P001#2"
