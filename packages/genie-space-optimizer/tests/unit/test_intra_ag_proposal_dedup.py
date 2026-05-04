"""Cycle 2 Task 1 — intra-AG content-fingerprint dedup.

The proposer can emit two proposals with identical body text under
different ``patch_type`` values. Today's content_fingerprint includes
``patch_type``, so the cross-iteration dedup gate misses them. This
suite pins the new body-only fingerprint and the intra-AG dedup that
consumes it.
"""
from __future__ import annotations


_DUP_BODY = (
    "In the Genie Space metadata for tkt_payment table, add a "
    "description for VCR_CREATE_DT column indicating it is the ticket "
    "creation date used for filtering tickets by creation date."
)


def _proposal(pid: str, patch_type: str, body: str = _DUP_BODY) -> dict:
    return {
        "proposal_id": pid,
        "patch_type": patch_type,
        "body": body,
        "target_table": "tkt_payment",
        "target_column": "VCR_CREATE_DT",
        "section_set": frozenset(),
        "parent_proposal_id": "",
    }


def test_patch_body_fingerprint_ignores_patch_type() -> None:
    from genie_space_optimizer.optimization.reflection_retry import (
        patch_body_fingerprint,
    )

    instruction = _proposal("P001", "rewrite_instruction")
    sql_filter = _proposal("P002", "add_sql_snippet_filter")
    assert patch_body_fingerprint(instruction) == patch_body_fingerprint(sql_filter)


def test_patch_body_fingerprint_distinguishes_different_bodies() -> None:
    from genie_space_optimizer.optimization.reflection_retry import (
        patch_body_fingerprint,
    )

    a = _proposal("P001", "rewrite_instruction", body="foo")
    b = _proposal("P002", "rewrite_instruction", body="bar")
    assert patch_body_fingerprint(a) != patch_body_fingerprint(b)


def test_intra_ag_dedup_collapses_duplicate_bodies(monkeypatch) -> None:
    """Three proposals with identical body across different
    patch_types collapse to a single survivor when the flag is on.
    """
    monkeypatch.setenv("GSO_INTRA_AG_PROPOSAL_DEDUP", "1")

    from genie_space_optimizer.optimization.stages.gates import (
        _run_intra_ag_dedup,
    )

    proposals = {
        "AG_DECOMPOSED_H001": (
            _proposal("P001", "rewrite_instruction"),
            _proposal("P002", "add_sql_snippet_filter"),
            _proposal("P003", "add_sql_snippet_filter"),
        ),
    }
    survived, drops = _run_intra_ag_dedup(ctx=None, proposals_by_ag=proposals)

    assert len(survived["AG_DECOMPOSED_H001"]) == 1
    assert survived["AG_DECOMPOSED_H001"][0]["proposal_id"] == "P001"
    dropped_ids = sorted(d.proposal_id for d in drops)
    assert dropped_ids == ["P002", "P003"]
    for d in drops:
        assert d.gate == "intra_ag_dedup"
        assert d.reason == "duplicate_body_within_ag"


def test_intra_ag_dedup_keeps_distinct_bodies(monkeypatch) -> None:
    monkeypatch.setenv("GSO_INTRA_AG_PROPOSAL_DEDUP", "1")
    from genie_space_optimizer.optimization.stages.gates import (
        _run_intra_ag_dedup,
    )

    proposals = {
        "AG_X": (
            _proposal("P001", "rewrite_instruction", body="alpha"),
            _proposal("P002", "rewrite_instruction", body="beta"),
        ),
    }
    survived, drops = _run_intra_ag_dedup(ctx=None, proposals_by_ag=proposals)
    assert len(survived["AG_X"]) == 2
    assert drops == []


def test_intra_ag_dedup_no_op_when_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_INTRA_AG_PROPOSAL_DEDUP", "0")
    from genie_space_optimizer.optimization.stages.gates import (
        _run_intra_ag_dedup,
    )

    proposals = {
        "AG_X": (
            _proposal("P001", "rewrite_instruction"),
            _proposal("P002", "add_sql_snippet_filter"),
        ),
    }
    survived, drops = _run_intra_ag_dedup(ctx=None, proposals_by_ag=proposals)
    assert len(survived["AG_X"]) == 2
    assert drops == []
