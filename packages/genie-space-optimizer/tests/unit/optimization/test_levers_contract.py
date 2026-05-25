"""Trial 17 — pin the Lever Selection Contract validator.

Sanity-check the closed lever set, the membership table, and the
``validate_plan_vs_proposal_consistency`` guardrail. This module is
the cornerstone of Step 2's plan-vs-proposal validation and of Step
3's ``selected_lever`` inference for legacy proposals.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.levers_contract import (
    LEVER_IDS,
    LEVER_TO_PATCH_TYPES,
    archetype_catalog_menu_for_prompt,
    infer_lever_from_patch_type,
    is_lever_id,
    lever_menu_for_prompt,
    patch_types_for_lever,
    validate_plan_vs_proposal_consistency,
)
from genie_space_optimizer.optimization.repair_intent import PatchType


def test_closed_lever_set_has_six_entries():
    assert LEVER_IDS == (
        "lever-1",
        "lever-2",
        "lever-3",
        "lever-4",
        "lever-5",
        "lever-6",
    )
    assert all(is_lever_id(x) for x in LEVER_IDS)
    assert not is_lever_id("lever-7")
    assert not is_lever_id("")


def test_membership_table_covers_observed_patch_types():
    """The four patch_types observed in the latest postmortems must
    all be reachable from at least one lever."""
    observed = {
        PatchType.ADD_INSTRUCTION,
        PatchType.ADD_EXAMPLE_SQL,
        PatchType.ADD_COLUMN_DESCRIPTION,
        PatchType.UPDATE_COLUMN_DESCRIPTION,
        PatchType.ADD_JOIN_SPEC,
    }
    covered = {
        pt
        for pts in LEVER_TO_PATCH_TYPES.values()
        for pt in pts
    }
    missing = observed - covered
    assert not missing, f"membership table missing patch_types: {missing}"


def test_infer_lever_for_add_instruction_is_lever_5():
    assert infer_lever_from_patch_type("add_instruction") == "lever-5"
    assert infer_lever_from_patch_type(PatchType.ADD_INSTRUCTION) == "lever-5"


def test_infer_lever_for_add_example_sql_is_lever_5():
    """5a and 5b share lever-5."""
    assert infer_lever_from_patch_type("add_example_sql") == "lever-5"


def test_infer_lever_for_metadata_patch_is_lever_1():
    assert infer_lever_from_patch_type("add_column_description") == "lever-1"
    assert infer_lever_from_patch_type("update_column_description") == "lever-1"


def test_infer_lever_for_join_patch_is_lever_4():
    assert infer_lever_from_patch_type("add_join_spec") == "lever-4"


def test_infer_lever_for_snippet_patch_is_lever_6():
    assert infer_lever_from_patch_type("add_sql_snippet_expression") == "lever-6"


def test_infer_lever_returns_empty_for_unknown_patch_type():
    assert infer_lever_from_patch_type("not_a_patch_type") == ""
    assert infer_lever_from_patch_type(None) == ""
    assert infer_lever_from_patch_type("") == ""


def test_validate_returns_none_when_lever_empty():
    """Legacy proposals (selected_lever='') skip the validator."""
    assert (
        validate_plan_vs_proposal_consistency(
            selected_lever="", patch_type="add_instruction"
        )
        is None
    )


def test_validate_returns_none_when_consistent():
    assert (
        validate_plan_vs_proposal_consistency(
            selected_lever="lever-5", patch_type="add_instruction"
        )
        is None
    )
    assert (
        validate_plan_vs_proposal_consistency(
            selected_lever="lever-1",
            patch_type=PatchType.ADD_COLUMN_DESCRIPTION,
        )
        is None
    )


def test_validate_flags_inconsistent_pair():
    reason = validate_plan_vs_proposal_consistency(
        selected_lever="lever-1", patch_type="add_instruction"
    )
    assert reason is not None
    assert "lever_plan_violation" in reason
    assert "plan=lever-1" in reason
    assert "patch=add_instruction" in reason


def test_validate_flags_unknown_lever():
    reason = validate_plan_vs_proposal_consistency(
        selected_lever="lever-99", patch_type="add_instruction"
    )
    assert reason is not None
    assert "unknown_lever" in reason


def test_validate_flags_unknown_patch_type():
    reason = validate_plan_vs_proposal_consistency(
        selected_lever="lever-5", patch_type="not_a_real_patch_type"
    )
    assert reason is not None
    assert "unknown_patch_type" in reason


def test_lever_menu_for_prompt_emits_six_entries_with_patch_types():
    menu = lever_menu_for_prompt()
    assert len(menu) == 6
    by_id = {m["id"]: m for m in menu}
    assert set(by_id.keys()) == set(LEVER_IDS)
    # lever-5 must list both add_instruction and add_example_sql so the
    # LLM can pick either variant.
    lever5 = by_id["lever-5"]["allowed_patch_types"]
    assert "add_instruction" in lever5
    assert "add_example_sql" in lever5


def test_lever_menu_for_prompt_carries_trial17_1_semantic_fields():
    """Trial 17.1 — each lever_menu entry must surface a human-readable
    ``description`` and a closed ``prefer_when`` list so the iteration-1
    Stage 3 LLM has semantic context (not just structural allow-lists)
    when picking ``selected_lever``. The lever menu is the single
    source of truth — bias signals live next to the allow-list so they
    cannot drift apart.
    """
    menu = lever_menu_for_prompt()
    by_id = {m["id"]: m for m in menu}
    for lever_id, entry in by_id.items():
        assert isinstance(entry.get("description"), str)
        assert entry["description"], (
            f"{lever_id}: empty description (Trial 17.1 regression)"
        )
        assert isinstance(entry.get("prefer_when"), list)
        assert entry["prefer_when"], (
            f"{lever_id}: empty prefer_when list (Trial 17.1 regression)"
        )


def test_lever_6_prefer_when_includes_grammar_pivot_tokens():
    """The gs_009 production gap: top-N pivots ('RANK() instead of
    LIMIT N') kept landing on lever-5a because nothing told the LLM
    lever-6 was the right tool. Trial 17.1 fixes that by surfacing
    closed ``grammar_pivot:*`` tokens on lever-6."""
    menu = lever_menu_for_prompt()
    by_id = {m["id"]: m for m in menu}
    lever6_prefers = set(by_id["lever-6"]["prefer_when"])
    grammar_tokens = {t for t in lever6_prefers if t.startswith("grammar_pivot:")}
    assert grammar_tokens, (
        "lever-6 must carry at least one 'grammar_pivot:*' prefer_when "
        "token so the iteration-1 LLM has a signal for top-N / "
        "missing-ORDER-BY / missing-GROUP-BY diagnoses."
    )
    # Spot-check the specific gs_009 RCA shape.
    assert "grammar_pivot:rank_to_limit" in lever6_prefers


def test_lever_5_description_warns_against_prose_for_grammar_pivots():
    """The lever-5 entry must call out the prose-vs-example_sql split
    so the LLM doesn't habitually default to ``add_instruction`` for
    grammar-shape diagnoses."""
    menu = lever_menu_for_prompt()
    by_id = {m["id"]: m for m in menu}
    desc = by_id["lever-5"]["description"].lower()
    assert "5a" in desc and "5b" in desc, (
        "lever-5 description must distinguish 5a (prose) from 5b "
        "(example_sql) so the LLM can choose between them deliberately."
    )


def test_patch_types_for_lever_returns_frozen_set():
    pts = patch_types_for_lever("lever-1")
    assert isinstance(pts, frozenset)
    assert PatchType.ADD_COLUMN_DESCRIPTION in pts


def test_patch_types_for_unknown_lever_is_empty():
    assert patch_types_for_lever("lever-99") == frozenset()


def test_archetype_catalog_menu_for_prompt_returns_serialisable_entries():
    """Trial 17 Step 7 — archetype catalog now travels as menu context
    inside the Stage 3 prompt. Each entry must be a JSON-serialisable
    dict carrying ``name``, ``applicable_root_causes`` (sorted list),
    ``required_constructs`` (list), and ``patch_type`` (string).
    """
    import json

    menu = archetype_catalog_menu_for_prompt()
    assert isinstance(menu, list)
    assert menu, "archetype catalog menu must not be empty"
    for entry in menu:
        assert isinstance(entry, dict)
        assert set(entry.keys()) >= {
            "name",
            "applicable_root_causes",
            "required_constructs",
            "patch_type",
        }
        assert isinstance(entry["name"], str)
        assert isinstance(entry["applicable_root_causes"], list)
        assert isinstance(entry["required_constructs"], list)
        assert isinstance(entry["patch_type"], str)
    json.dumps(menu)
    names = {e["name"] for e in menu}
    assert "simple_enumerate" in names
