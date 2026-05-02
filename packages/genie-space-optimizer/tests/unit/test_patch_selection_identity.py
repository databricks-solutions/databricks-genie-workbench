"""Tests for cap-side identity stability (Track A in Phase A burn-down MVP)."""
from __future__ import annotations


def test_stable_identity_distinguishes_split_child_from_standalone_with_same_id() -> None:
    """Two patches sharing P001#2 but differing in lever/type must NOT collide.

    This is the May-01 ESR / 7Now collision pattern: a rewrite_instruction
    splits into L5 update_instruction_section children stamped P001#1,
    P001#2, P001#3, and a separate standalone L6 add_sql_snippet_expression
    inherits the parent index and lands on P001#2 as well. The cap's
    dedup helper must treat them as distinct.
    """
    from genie_space_optimizer.optimization.patch_selection import _stable_identity

    l5_split_child = {
        "proposal_id": "P001#2",
        "expanded_patch_id": "P001#2",
        "parent_proposal_id": "P001",
        "lever": 5,
        "type": "update_instruction_section",
        "section_name": "QUERY PATTERNS",
    }
    l6_standalone = {
        "proposal_id": "P001#2",
        "expanded_patch_id": "P001#2",
        "parent_proposal_id": "P001",
        "lever": 6,
        "type": "add_sql_snippet_expression",
        "table": "cat.sch.mv_fact",
    }

    assert _stable_identity(l5_split_child) != _stable_identity(l6_standalone)


def test_stable_identity_distinguishes_two_split_children_targeting_different_sections() -> None:
    """Even with same lever/type, two patches with different targets do not collide."""
    from genie_space_optimizer.optimization.patch_selection import _stable_identity

    a = {
        "proposal_id": "P001#1",
        "expanded_patch_id": "P001#1",
        "parent_proposal_id": "P001",
        "lever": 5,
        "type": "update_instruction_section",
        "section_name": "QUERY RULES",
    }
    b = {
        "proposal_id": "P001#1",
        "expanded_patch_id": "P001#1",
        "parent_proposal_id": "P001",
        "lever": 5,
        "type": "update_instruction_section",
        "section_name": "ASSET ROUTING",
    }

    assert _stable_identity(a) != _stable_identity(b)
