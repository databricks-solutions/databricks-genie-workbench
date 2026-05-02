"""Track A — split-child stamping must not let a standalone patch in the
same patch list inherit the parent's sequential index."""
from __future__ import annotations


def test_expand_rewrite_splits_does_not_reuse_index_for_standalone_patch_in_same_list() -> None:
    """A rewrite_instruction split into K children must not produce an id
    that collides with a standalone patch processed in the same list."""
    from genie_space_optimizer.optimization.applier import _expand_rewrite_splits

    patches = [
        {
            "type": "rewrite_instruction",
            "proposal_id": "P001",
            "parent_proposal_id": "P001",
            "lever": 5,
            "invoked_levers": [5],
            "proposed_value": (
                "QUERY RULES:\n- rule one\n\n"
                "ASSET ROUTING:\n- rule two\n\n"
                "QUERY PATTERNS:\n- rule three\n"
            ),
        },
        {
            "type": "add_sql_snippet_expression",
            "proposal_id": "P001#2",
            "parent_proposal_id": "P001",
            "expanded_patch_id": "P001#2",
            "lever": 6,
            "table": "cat.sch.mv_fact",
            "sql": "SUM(cy_sales)",
        },
    ]

    expanded = _expand_rewrite_splits(patches)
    ids = [(p.get("proposal_id"), p.get("lever"), p.get("type")) for p in expanded]
    seen: dict[tuple, int] = {}
    for triple in ids:
        seen[triple] = seen.get(triple, 0) + 1
    duplicates = [t for t, n in seen.items() if n > 1]
    assert not duplicates, (
        f"split-children collide with standalone patch on (id, lever, type): {duplicates!r}; "
        f"all ids: {ids!r}"
    )
