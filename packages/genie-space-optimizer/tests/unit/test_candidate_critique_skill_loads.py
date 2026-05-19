"""Plan 6 Task 5 — pin the candidate-critique skill loadability.

Plan-2's conformance suite covers generic invariants; this test pins
skill-specific values that drift would silently break.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.skills._loader import _SKILL_LOADER


SKILL_ID = "candidate_critique"


def test_metadata_skill_id_uses_hyphenated_form() -> None:
    meta = _SKILL_LOADER.load_metadata(SKILL_ID)
    assert meta["skill_id"] == "candidate-critique"


def test_prompt_constant_name_matches_expected() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="CANDIDATE_CRITIQUE_PROMPT",
    )
    assert body


def test_output_schema_class_resolves_to_critique_pydantic() -> None:
    cls = _SKILL_LOADER.load_output_schema_class(SKILL_ID)
    assert cls.__name__ == "LlmCritiqueVerdictOutput"
    assert issubclass(cls, LLMOutputContract)


def test_max_tokens_matches_planned_budget() -> None:
    """500 output × 3 candidates/iter = 1.5k OTPM/iter. Combined with
    Plan 3+4+5 (~25k OTPM/iter) and Databricks 20k OTPM/min cap, this
    fits because iterations are minutes apart in lever-loop traffic."""
    rsm = _SKILL_LOADER.load_reasoning_metadata(SKILL_ID)
    assert rsm is not None
    assert rsm.max_tokens == 500


def test_examples_count_is_four() -> None:
    examples = list(_SKILL_LOADER.iter_examples(SKILL_ID))
    assert len(examples) == 4


def test_eval_cases_cover_three_proceed_one_discard_one_rework_two_abstain() -> None:
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
    """Plan 4 Task 11's walker reads this field to auto-register the
    SKILL.md body to MLflow Prompt Registry on harness startup."""
    rsm = _SKILL_LOADER.load_reasoning_metadata(SKILL_ID)
    assert rsm is not None
    assert rsm.prompt_registry_name == "gso_reasoning_candidate_critique"


def test_skill_md_documents_scoring_rubric() -> None:
    """The four binary dimensions + recommendation rubric must be inline
    in the SKILL.md so the LLM has just-in-time context."""
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="CANDIDATE_CRITIQUE_PROMPT",
    )
    assert "<scoring_rubric>" in body
    for dim in (
        "addresses_target_failure", "is_overgeneralized",
        "likely_neighbor_regressions", "matches_intended_shape",
        "overall_recommendation",
    ):
        assert dim in body
    for rec in ("proceed", "rework", "discard"):
        assert rec in body


def test_skill_md_documents_passing_qids_at_risk_grounding_constraint() -> None:
    """LLM must NEVER add a qid to likely_neighbor_regressions that
    isn't in passing_qids_at_risk."""
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="CANDIDATE_CRITIQUE_PROMPT",
    )
    assert "passing_qids_at_risk" in body
    assert "Never invent qids" in body or "never invent qids" in body.lower()
