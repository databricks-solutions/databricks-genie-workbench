"""Plan 2 Task 5 — SkillLoader gains reasoning-skill accessors."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import BaseModel

from genie_space_optimizer.skills._loader import (
    ReasoningSkillMetadata,
    SkillLoader,
)


class _DummyOutput(BaseModel):
    answer: str


def _write_skill(
    root: Path,
    skill_id: str,
    frontmatter: dict,
    body: str = "system prompt body",
) -> None:
    skill_dir = root / skill_id
    skill_dir.mkdir(parents=True)
    fm_lines = ["---"]
    for k, v in frontmatter.items():
        fm_lines.append(f"{k}: {v}")
    fm_lines.append("---")
    fm_lines.append(body)
    (skill_dir / "SKILL.md").write_text(
        "\n".join(fm_lines) + "\n", encoding="utf-8"
    )


def test_load_reasoning_metadata_returns_typed_record(tmp_path: Path) -> None:
    _write_skill(
        tmp_path,
        "rca-evidence-extraction",
        {
            "skill_id": "rca-evidence-extraction",
            "prompt_constant_name": "RCA_EVIDENCE_PROMPT",
            "llm_call_kind": "reasoning",
            "output_schema_class": (
                "test_skill_loader_reasoning_metadata:_DummyOutput"
            ),
            "max_tokens": 1500,
            "abstain_supported": "true",
            "examples_dir": "./examples",
            "eval_dir": "./eval",
        },
    )
    loader = SkillLoader(root=tmp_path)
    meta = loader.load_reasoning_metadata("rca-evidence-extraction")
    assert isinstance(meta, ReasoningSkillMetadata)
    assert meta.llm_call_kind == "reasoning"
    assert meta.max_tokens == 1500
    assert meta.abstain_supported is True
    assert meta.examples_dir == "./examples"
    assert meta.eval_dir == "./eval"
    assert meta.model_override is None


def test_load_reasoning_metadata_returns_none_for_legacy_skill(
    tmp_path: Path,
) -> None:
    _write_skill(
        tmp_path,
        "lever-5b-example-sql",
        {
            "skill_id": "lever-5b-example-sql",
            "prompt_constant_name": "LEVER_5B_EXAMPLE_SQL_PROMPT",
            "causal_or_non_causal": "causal",
            "pickable_by_stage_1": "true",
        },
    )
    loader = SkillLoader(root=tmp_path)
    assert loader.load_reasoning_metadata("lever-5b-example-sql") is None


def test_load_reasoning_metadata_raises_when_required_field_missing(
    tmp_path: Path,
) -> None:
    """When ``llm_call_kind: reasoning`` is set, ``output_schema_class``
    and ``max_tokens`` become required."""
    _write_skill(
        tmp_path,
        "broken-reasoning-skill",
        {
            "skill_id": "broken-reasoning-skill",
            "prompt_constant_name": "BROKEN_PROMPT",
            "llm_call_kind": "reasoning",
        },
    )
    loader = SkillLoader(root=tmp_path)
    with pytest.raises(ValueError, match="output_schema_class"):
        loader.load_reasoning_metadata("broken-reasoning-skill")


def test_load_output_schema_class_resolves_import_path(tmp_path: Path) -> None:
    _write_skill(
        tmp_path,
        "rca-evidence-extraction",
        {
            "skill_id": "rca-evidence-extraction",
            "prompt_constant_name": "RCA_EVIDENCE_PROMPT",
            "llm_call_kind": "reasoning",
            "output_schema_class": (
                "test_skill_loader_reasoning_metadata:_DummyOutput"
            ),
            "max_tokens": 1500,
        },
    )
    loader = SkillLoader(root=tmp_path)
    cls = loader.load_output_schema_class("rca-evidence-extraction")
    assert cls is _DummyOutput


def test_iter_examples_yields_parsed_json_files(tmp_path: Path) -> None:
    _write_skill(
        tmp_path,
        "rca-evidence-extraction",
        {
            "skill_id": "rca-evidence-extraction",
            "prompt_constant_name": "RCA_EVIDENCE_PROMPT",
            "llm_call_kind": "reasoning",
            "output_schema_class": (
                "test_skill_loader_reasoning_metadata:_DummyOutput"
            ),
            "max_tokens": 1500,
            "examples_dir": "./examples",
        },
    )
    ex_dir = tmp_path / "rca-evidence-extraction" / "examples"
    ex_dir.mkdir()
    (ex_dir / "01_canonical.json").write_text(json.dumps({"answer": "yes"}))
    (ex_dir / "02_abstain.json").write_text(
        json.dumps({"declined": {"reason": "other"}})
    )

    loader = SkillLoader(root=tmp_path)
    examples = list(loader.iter_examples("rca-evidence-extraction"))
    assert len(examples) == 2
    names = sorted(name for name, _ in examples)
    assert names == ["01_canonical.json", "02_abstain.json"]


def test_iter_examples_rejects_more_than_four(tmp_path: Path) -> None:
    """Anthropic context engineering: ≤4 canonical examples."""
    _write_skill(
        tmp_path,
        "rca-evidence-extraction",
        {
            "skill_id": "rca-evidence-extraction",
            "prompt_constant_name": "RCA_EVIDENCE_PROMPT",
            "llm_call_kind": "reasoning",
            "output_schema_class": (
                "test_skill_loader_reasoning_metadata:_DummyOutput"
            ),
            "max_tokens": 1500,
            "examples_dir": "./examples",
        },
    )
    ex_dir = tmp_path / "rca-evidence-extraction" / "examples"
    ex_dir.mkdir()
    for i in range(5):
        (ex_dir / f"{i:02d}.json").write_text("{}")

    loader = SkillLoader(root=tmp_path)
    with pytest.raises(ValueError, match="≤4 canonical examples"):
        list(loader.iter_examples("rca-evidence-extraction"))


def test_iter_examples_returns_empty_when_dir_absent(tmp_path: Path) -> None:
    _write_skill(
        tmp_path,
        "rca-evidence-extraction",
        {
            "skill_id": "rca-evidence-extraction",
            "prompt_constant_name": "RCA_EVIDENCE_PROMPT",
            "llm_call_kind": "reasoning",
            "output_schema_class": (
                "test_skill_loader_reasoning_metadata:_DummyOutput"
            ),
            "max_tokens": 1500,
        },
    )
    loader = SkillLoader(root=tmp_path)
    assert list(loader.iter_examples("rca-evidence-extraction")) == []
