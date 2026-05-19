"""Plan 7 Task 5 — pin the rollback-learning skill loadability."""
from __future__ import annotations

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.skills._loader import _SKILL_LOADER


SKILL_ID = "rollback_learning"


def test_metadata_skill_id_uses_hyphenated_form() -> None:
    meta = _SKILL_LOADER.load_metadata(SKILL_ID)
    assert meta["skill_id"] == "rollback-learning"


def test_prompt_constant_name_matches_expected() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="ROLLBACK_LEARNING_PROMPT",
    )
    assert body


def test_output_schema_class_resolves_to_hypothesis_pydantic() -> None:
    cls = _SKILL_LOADER.load_output_schema_class(SKILL_ID)
    assert cls.__name__ == "LlmNextAttemptHypothesisOutput"
    assert issubclass(cls, LLMOutputContract)


def test_max_tokens_matches_planned_budget() -> None:
    rsm = _SKILL_LOADER.load_reasoning_metadata(SKILL_ID)
    assert rsm is not None
    assert rsm.max_tokens == 700


def test_examples_count_is_four() -> None:
    examples = list(_SKILL_LOADER.iter_examples(SKILL_ID))
    assert len(examples) == 4


def test_eval_cases_cover_four_result_two_declined() -> None:
    import json
    from collections import Counter
    from pathlib import Path
    eval_path = (
        Path(_SKILL_LOADER._root) / SKILL_ID / "eval" / "test_cases.jsonl"
    )
    cases = [
        json.loads(line)
        for line in eval_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    by_branch = Counter(c["expected_result_branch"] for c in cases)
    assert by_branch["result"] == 4
    assert by_branch["declined"] == 2


def test_prompt_registry_name_frontmatter_present() -> None:
    rsm = _SKILL_LOADER.load_reasoning_metadata(SKILL_ID)
    assert rsm is not None
    assert rsm.prompt_registry_name == "gso_reasoning_rollback_learning"


def test_skill_md_documents_reasoning_rubric() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="ROLLBACK_LEARNING_PROMPT",
    )
    assert "<reasoning_rubric>" in body
    for q in (
        "WHY did the deterministic gate roll back",
        "WHAT TYPED dimension should change for the next attempt",
        "WHAT EVIDENCE or CONSTRAINTS should the next attempt have",
    ):
        assert q in body


def test_skill_md_documents_forbidden_signature_subset_constraint() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="ROLLBACK_LEARNING_PROMPT",
    )
    assert "applied_patch_fingerprints" in body
    assert "subset" in body.lower() or "silently drop" in body.lower()


def test_skill_md_documents_revised_blame_set_subset_constraint() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="ROLLBACK_LEARNING_PROMPT",
    )
    assert "identifier_allowlist" in body
    assert "subset" in body.lower()
