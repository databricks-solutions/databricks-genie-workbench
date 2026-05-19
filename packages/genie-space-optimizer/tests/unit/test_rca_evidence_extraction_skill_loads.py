"""Plan 3 Task 8 — pin rca-evidence-extraction end-to-end loadability."""
from __future__ import annotations

from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.skills._loader import _SKILL_LOADER


SKILL_ID = "rca_evidence_extraction"


def test_metadata_skill_id_uses_hyphenated_form() -> None:
    """Folder is underscored (Python module import); skill_id
    frontmatter keeps the hyphenated form for postmortem readability."""
    meta = _SKILL_LOADER.load_metadata(SKILL_ID)
    assert meta["skill_id"] == "rca-evidence-extraction"


def test_prompt_constant_name_matches_expected() -> None:
    body = _SKILL_LOADER.load_prompt(
        SKILL_ID, expected_constant_name="RCA_EVIDENCE_EXTRACTION_PROMPT",
    )
    assert body


def test_output_schema_class_resolves_to_per_qid_pydantic() -> None:
    cls = _SKILL_LOADER.load_output_schema_class(SKILL_ID)
    assert cls.__name__ == "PerQidRcaEvidenceOutput"
    assert issubclass(cls, LLMOutputContract)


def test_max_tokens_matches_roadmap_budget() -> None:
    """Roadmap.md:263-264: ~1500 input + max 800 output tokens per qid."""
    rsm = _SKILL_LOADER.load_reasoning_metadata(SKILL_ID)
    assert rsm is not None
    assert rsm.max_tokens == 800


def test_examples_count_matches_anthropic_guidance() -> None:
    """Anthropic context engineering: ≤4 canonical examples. We ship 4."""
    examples = list(_SKILL_LOADER.iter_examples(SKILL_ID))
    assert len(examples) == 4
    names = sorted(name for name, _ in examples)
    assert names == [
        "01_top_n_collapse.json",
        "02_join_spec_missing.json",
        "03_filter_logic_mismatch.json",
        "04_abstain_ambiguous.json",
    ]


def test_eval_cases_cover_both_branches() -> None:
    """At least one success-branch case and one decline-branch case."""
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
    assert branches == {"result", "declined"}, (
        f"eval cases must cover both branches; got {branches}"
    )
