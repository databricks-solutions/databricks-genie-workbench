"""Plan 5 Task 5 — pin the repair-intent-synthesis skill loadability.

Plan-2's conformance suite covers generic invariants; this test pins
skill-specific values that drift would silently break.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.skills._loader import _SKILL_LOADER


SKILL_ID = "repair_intent_synthesis"


def test_metadata_skill_id_uses_hyphenated_form() -> None:
    meta = _SKILL_LOADER.load_metadata(SKILL_ID)
    assert meta["skill_id"] == "repair-intent-synthesis"


def test_prompt_constant_name_matches_expected() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="REPAIR_INTENT_SYNTHESIS_PROMPT",
    )
    assert body


def test_output_schema_class_resolves_to_repair_proposal_pydantic() -> None:
    cls = _SKILL_LOADER.load_output_schema_class(SKILL_ID)
    assert cls.__name__ == "LlmRepairProposalOutput"
    assert issubclass(cls, LLMOutputContract)


def test_max_tokens_matches_planned_budget() -> None:
    """5 clusters/iter × 1200 max output = 6k OTPM/iter. With Plan-3
    (16k) + Plan-4 (2k) totals 24k OTPM/iter; Databricks 20k OTPM cap
    is per-minute and iterations are minutes apart, so this fits."""
    rsm = _SKILL_LOADER.load_reasoning_metadata(SKILL_ID)
    assert rsm is not None
    assert rsm.max_tokens == 1200


def test_examples_count_is_four() -> None:
    examples = list(_SKILL_LOADER.iter_examples(SKILL_ID))
    assert len(examples) == 4


def test_eval_cases_cover_three_in_lane_and_three_abstain() -> None:
    import json
    from pathlib import Path
    eval_path = (
        Path(_SKILL_LOADER._root) / SKILL_ID / "eval" / "test_cases.jsonl"
    )
    cases = [
        json.loads(line)
        for line in eval_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    by_branch: dict[str, int] = {}
    for c in cases:
        by_branch[c["expected_result_branch"]] = by_branch.get(
            c["expected_result_branch"], 0,
        ) + 1
    assert by_branch == {"result": 3, "declined": 3}


def test_prompt_registry_name_frontmatter_present() -> None:
    """Plan 4 Task 11's walker reads this field to auto-register the
    SKILL.md body to MLflow Prompt Registry on harness startup."""
    rsm = _SKILL_LOADER.load_reasoning_metadata(SKILL_ID)
    assert rsm is not None
    assert rsm.prompt_registry_name == "gso_reasoning_repair_intent_synthesis"


def test_skill_md_documents_cross_lever_override_in_instructions() -> None:
    """Cross-lever override is Plan 5's defining capability — the SKILL.md
    must instruct the LLM on when/how to use it."""
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="REPAIR_INTENT_SYNTHESIS_PROMPT",
    )
    assert "Cross-lever override" in body or "cross-lever override" in body
    assert "add_sql_snippet_expression" in body


def test_skill_md_documents_patch_body_shapes() -> None:
    """Per-patch-type field expectations must be inline in the SKILL.md
    so the LLM has just-in-time context."""
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="REPAIR_INTENT_SYNTHESIS_PROMPT",
    )
    assert "<patch_body_shapes>" in body
    for patch_type in ("add_example_sql", "add_sql_snippet_expression",
                       "add_join_spec", "add_instruction"):
        assert patch_type in body
