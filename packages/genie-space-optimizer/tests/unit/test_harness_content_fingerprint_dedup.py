"""TDD: content-fingerprint dedup drops byte-identical re-proposals."""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _drop_proposals_matching_rolled_back_content_fingerprints,
)


def _instr_patch(text: str, section: str = "QUERY PATTERNS") -> dict:
    return {
        "type": "update_instruction_section",
        "patch_type": "update_instruction_section",
        "instruction_section": section,
        "target": section,
        "value": text,
        "new_text": text,
        "lever": 5,
        "root_cause": "wrong_join_spec",
    }


def test_drops_proposal_with_identical_content_fingerprint_to_rolled_back():
    text = "When joining route to passengers, restrict to 2023 only."
    rolled_back = [_instr_patch(text)]
    proposed = [_instr_patch(text)]  # byte-identical re-proposal

    kept, dropped = _drop_proposals_matching_rolled_back_content_fingerprints(
        proposals=proposed,
        rolled_back_patches=rolled_back,
    )
    assert kept == []
    assert len(dropped) == 1
    assert dropped[0][1] == "content_fingerprint_seen_in_rolled_back_set"


def test_keeps_proposal_with_different_content():
    text_old = "When joining route to passengers, restrict to 2023 only."
    text_new = "When joining route to passengers, use the latest fact table."
    rolled_back = [_instr_patch(text_old)]
    proposed = [_instr_patch(text_new)]

    kept, dropped = _drop_proposals_matching_rolled_back_content_fingerprints(
        proposals=proposed,
        rolled_back_patches=rolled_back,
    )
    assert kept == proposed
    assert dropped == []


def test_dedupes_uniformly_across_rollback_classes():
    """Whether the rollback was CONTENT_REGRESSION, INFRA, or anything
    else, an identical content fingerprint still gets dropped."""
    text = "Identical content"
    rolled_back = [_instr_patch(text)]  # rollback_class is not consulted
    proposed = [_instr_patch(text)]
    kept, dropped = _drop_proposals_matching_rolled_back_content_fingerprints(
        proposals=proposed,
        rolled_back_patches=rolled_back,
    )
    assert kept == []
    assert dropped


def test_handles_empty_inputs():
    assert _drop_proposals_matching_rolled_back_content_fingerprints(
        proposals=[], rolled_back_patches=[]
    ) == ([], [])
    assert _drop_proposals_matching_rolled_back_content_fingerprints(
        proposals=[_instr_patch("x")], rolled_back_patches=[]
    ) == ([_instr_patch("x")], [])


def test_dedup_is_per_unique_fingerprint_not_per_proposal():
    """Two distinct proposals with the same content should both be kept
    if neither matches a rolled-back fingerprint."""
    p1 = _instr_patch("text-A")
    p2 = _instr_patch("text-A")  # same content as p1
    rolled_back = [_instr_patch("text-B")]
    kept, dropped = _drop_proposals_matching_rolled_back_content_fingerprints(
        proposals=[p1, p2],
        rolled_back_patches=rolled_back,
    )
    assert kept == [p1, p2]
    assert dropped == []
