"""Cycle 2 Task 3 — DOA ledger keyed on selected proposal IDs.

Today's DOA ledger keys on applied-patch IDs only, which are empty
when blast-radius drops every patch. That allows the same AG with
the same proposals to retry verbatim two iterations later (the iter-3
/ iter-5 AG_COVERAGE_H001 replay in run
2afb0be2-88b6-4832-99aa-c7e78fbc90f7). This task adds a parallel
ledger keyed on selected proposal IDs.
"""
from __future__ import annotations


def test_compute_selected_proposal_signature_orders_stable() -> None:
    from genie_space_optimizer.optimization.harness import (
        _compute_selected_proposal_signature,
    )

    proposals = [
        {"proposal_id": "P002"},
        {"proposal_id": "P001"},
    ]
    sig = _compute_selected_proposal_signature(proposals)
    assert sig == ("P001", "P002")


def test_compute_selected_proposal_signature_empty_input() -> None:
    from genie_space_optimizer.optimization.harness import (
        _compute_selected_proposal_signature,
    )

    assert _compute_selected_proposal_signature([]) == ()
    assert _compute_selected_proposal_signature(None) == ()


def test_doa_selected_signature_blocks_repeat_when_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_DOA_SELECTED_PROPOSAL_SIGNATURE", "1")
    from genie_space_optimizer.optimization.harness import (
        _is_doa_selected_signature_blocked,
        _record_doa_selected_signature,
    )

    seen: dict[str, set[tuple[str, ...]]] = {}
    sig = ("P001", "P002")
    _record_doa_selected_signature(
        seen=seen, ag_id="AG_COVERAGE_H001", signature=sig,
    )
    assert _is_doa_selected_signature_blocked(
        seen=seen, ag_id="AG_COVERAGE_H001", signature=sig,
    ) is True
    # A different AG with the same signature is NOT blocked — the
    # ledger is keyed by AG to allow distinct AGs with overlapping
    # proposal IDs.
    assert _is_doa_selected_signature_blocked(
        seen=seen, ag_id="AG_OTHER", signature=sig,
    ) is False
    # A different signature on the same AG is NOT blocked — the
    # strategist is allowed to vary patch shape.
    assert _is_doa_selected_signature_blocked(
        seen=seen, ag_id="AG_COVERAGE_H001", signature=("P003",),
    ) is False


def test_doa_selected_signature_no_op_when_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_DOA_SELECTED_PROPOSAL_SIGNATURE", "0")
    from genie_space_optimizer.optimization.harness import (
        _is_doa_selected_signature_blocked,
        _record_doa_selected_signature,
    )

    seen: dict[str, set[tuple[str, ...]]] = {}
    sig = ("P001", "P002")
    _record_doa_selected_signature(seen=seen, ag_id="AG_X", signature=sig)
    # Recording is a no-op when flag is off.
    assert seen == {}
    assert _is_doa_selected_signature_blocked(
        seen=seen, ag_id="AG_X", signature=sig,
    ) is False


def test_compute_selected_proposal_signature_distinguishes_cross_lever_collisions() -> None:
    """Regression for the iter-1 P001#2 / P001#2 collision in run
    2423b960. Two patches under the same parent_proposal_id but
    different levers must produce distinct signature elements.
    """
    from genie_space_optimizer.optimization.harness import (
        _compute_selected_proposal_signature,
    )

    proposals = [
        {"proposal_id": "P001#1", "expanded_patch_id": "L1:P001#1"},
        {"proposal_id": "P001#2", "expanded_patch_id": "L1:P001#2"},
        {"proposal_id": "P001#2", "expanded_patch_id": "L5:P001#2"},
    ]
    sig = _compute_selected_proposal_signature(proposals)
    assert sig == ("L1:P001#1", "L1:P001#2", "L5:P001#2")
    assert len(set(sig)) == 3, "all three proposals must be distinguishable"
