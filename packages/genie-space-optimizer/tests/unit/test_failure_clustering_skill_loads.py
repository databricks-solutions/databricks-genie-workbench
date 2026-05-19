"""Plan 4 Task 7 — pin failure-clustering end-to-end loadability."""
from __future__ import annotations

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.skills._loader import _SKILL_LOADER


SKILL_ID = "failure_clustering"


def test_metadata_skill_id_uses_hyphenated_form() -> None:
    meta = _SKILL_LOADER.load_metadata(SKILL_ID)
    assert meta["skill_id"] == "failure-clustering"


def test_prompt_constant_name_matches_expected() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="FAILURE_CLUSTERING_PROMPT",
    )
    assert body


def test_output_schema_class_resolves_to_cluster_set_pydantic() -> None:
    cls = _SKILL_LOADER.load_output_schema_class(SKILL_ID)
    assert cls.__name__ == "LlmClusterSetOutput"
    assert issubclass(cls, LLMOutputContract)


def test_max_tokens_matches_planned_budget() -> None:
    rsm = _SKILL_LOADER.load_reasoning_metadata(SKILL_ID)
    assert rsm is not None
    assert rsm.max_tokens == 2000


def test_examples_count_is_four() -> None:
    examples = list(_SKILL_LOADER.iter_examples(SKILL_ID))
    assert len(examples) == 4


def test_eval_cases_cover_both_branches() -> None:
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
    branches = {c["expected_result_branch"] for c in cases}
    assert branches == {"result", "declined"}


def test_prompt_registry_name_frontmatter_present() -> None:
    """Plan 4 Task 11 adds this field to ReasoningSkillMetadata; the
    SKILL.md frontmatter must already carry it so the registration
    walker discovers it."""
    meta = _SKILL_LOADER.load_metadata(SKILL_ID)
    assert meta.get("prompt_registry_name") == "gso_reasoning_failure_clustering"
