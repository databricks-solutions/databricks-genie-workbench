"""Plan 4 / Task 14 — tests for the skills/_loader.py module."""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest


def test_loader_reads_yaml_frontmatter_and_body(tmp_path: Path):
    skill_dir = tmp_path / "test-skill-x"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text(textwrap.dedent("""\
        ---
        skill_id: test-skill-x
        prompt_constant_name: TEST_X_PROMPT
        causal_or_non_causal: causal
        pickable_by_stage_1: true
        ---
        Hello {{ name }}, this is the body.
        """), encoding="utf-8")

    from genie_space_optimizer.skills._loader import SkillLoader
    loader = SkillLoader(root=tmp_path)
    body = loader.load_prompt("test-skill-x",
                                expected_constant_name="TEST_X_PROMPT")
    assert "Hello {{ name }}, this is the body." in body
    assert "---" not in body  # frontmatter stripped
    meta = loader.load_metadata("test-skill-x")
    assert meta["skill_id"] == "test-skill-x"
    assert meta["prompt_constant_name"] == "TEST_X_PROMPT"
    assert meta["causal_or_non_causal"] == "causal"
    assert meta["pickable_by_stage_1"] is True


def test_loader_rejects_constant_name_mismatch(tmp_path: Path):
    skill_dir = tmp_path / "wrong-name"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text(textwrap.dedent("""\
        ---
        skill_id: wrong-name
        prompt_constant_name: ACTUAL_NAME_PROMPT
        causal_or_non_causal: causal
        pickable_by_stage_1: false
        ---
        body
        """), encoding="utf-8")

    from genie_space_optimizer.skills._loader import SkillLoader
    loader = SkillLoader(root=tmp_path)
    with pytest.raises(ValueError, match="constant name mismatch"):
        loader.load_prompt("wrong-name",
                            expected_constant_name="EXPECTED_NAME_PROMPT")


def test_loader_raises_on_missing_skill(tmp_path: Path):
    from genie_space_optimizer.skills._loader import SkillLoader
    loader = SkillLoader(root=tmp_path)
    with pytest.raises(FileNotFoundError, match="not-a-real-skill"):
        loader.load_prompt("not-a-real-skill",
                            expected_constant_name="X")


def test_loader_raises_on_missing_frontmatter(tmp_path: Path):
    skill_dir = tmp_path / "no-frontmatter"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text("just body, no frontmatter\n",
                                          encoding="utf-8")
    from genie_space_optimizer.skills._loader import SkillLoader
    loader = SkillLoader(root=tmp_path)
    with pytest.raises(ValueError, match="frontmatter"):
        loader.load_prompt("no-frontmatter",
                            expected_constant_name="X")


def test_loader_loads_lever_1_skill_from_real_path():
    """Smoke test against the actual src/.../skills/ directory.
    Confirms the L1 SKILL.md migrated in this task is loadable +
    parses correctly."""
    from genie_space_optimizer.skills._loader import SkillLoader
    loader = SkillLoader()
    body = loader.load_prompt(
        "lever-1-table-column-description",
        expected_constant_name="LEVER_1_2_COLUMN_PROMPT",
    )
    assert "{{ failure_type }}" in body
    assert "{{ raw_evidence_block }}" in body  # Plan 4 slot present


def test_loaded_lever_1_2_column_prompt_matches_loader_output():
    """The constant in common.config.py must be byte-identical to the
    loader's output for the L1 skill."""
    from genie_space_optimizer.common import config as cfg
    from genie_space_optimizer.skills._loader import SkillLoader
    loader = SkillLoader()
    body = loader.load_prompt(
        "lever-1-table-column-description",
        expected_constant_name="LEVER_1_2_COLUMN_PROMPT",
    )
    assert cfg.LEVER_1_2_COLUMN_PROMPT == body


_VALID_TARGET_KINDS = {"base_table", "metric_view", "function", "mixed"}


@pytest.mark.parametrize("skill_id", sorted([
    "lever-1-table-column-description",
    "lever-2-mv-column-refinement",
    "lever-3-tvf-routing",
    "lever-4-join-discovery",
    "lever-5a-instructions",
    "lever-5b-example-sql",
    "lever-6-sql-expression",
]))
def test_pickable_skill_frontmatter_has_target_kind_and_min_count(skill_id: str):
    """Every Stage-1-pickable skill must declare target_kind and
    target_min_count. The renderer surfaces them in the Stage-1 prompt's
    skill_catalogue slot; the Stage-1 post-validation coerces picks
    against them. Missing keys would silently regress routing."""
    from genie_space_optimizer.skills._loader import SkillLoader
    loader = SkillLoader()
    meta = loader.load_metadata(skill_id)
    target_kind = meta.get("target_kind")
    assert target_kind in _VALID_TARGET_KINDS, (
        f"{skill_id} has target_kind={target_kind!r}; "
        f"must be one of {sorted(_VALID_TARGET_KINDS)}"
    )
    target_min_count = meta.get("target_min_count")
    assert isinstance(target_min_count, int) and target_min_count >= 0, (
        f"{skill_id} has target_min_count={target_min_count!r}; "
        f"must be a non-negative int"
    )


@pytest.mark.parametrize("skill_id", sorted([
    "lever-1-table-column-description",
    "lever-2-mv-column-refinement",
    "lever-3-tvf-routing",
    "lever-4-join-discovery",
    "lever-5a-instructions",
    "lever-5b-example-sql",
    "lever-6-sql-expression",
]))
def test_pickable_skill_frontmatter_has_description_and_when_to_pick(skill_id: str):
    """Every Stage-1-pickable skill must carry routing-aid metadata
    in its SKILL.md frontmatter. The renderer in three_stage_pipeline
    inlines these fields into the Stage-1 prompt's skill_catalogue
    slot — empty values would silently regress Stage-1 routing."""
    from genie_space_optimizer.skills._loader import SkillLoader
    loader = SkillLoader()
    meta = loader.load_metadata(skill_id)
    assert isinstance(meta.get("description"), str) and meta["description"].strip(), (
        f"{skill_id} missing 'description' frontmatter"
    )
    assert isinstance(meta.get("when_to_pick"), str) and meta["when_to_pick"].strip(), (
        f"{skill_id} missing 'when_to_pick' frontmatter"
    )
    assert meta.get("pickable_by_stage_1") is True, (
        f"{skill_id} not marked pickable_by_stage_1 — fix the registry or this test"
    )
