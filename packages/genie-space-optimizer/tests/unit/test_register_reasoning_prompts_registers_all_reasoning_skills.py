"""Plan 4 Task 11 — ReasoningSkillMetadata.prompt_registry_name field."""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.skills._loader import (
    ReasoningSkillMetadata,
    _SKILL_LOADER,
)


def test_metadata_has_prompt_registry_name_field() -> None:
    field_names = {
        f.name for f in dataclasses.fields(ReasoningSkillMetadata)
    }
    assert "prompt_registry_name" in field_names


def test_metadata_prompt_registry_name_is_populated_for_failure_clustering() -> None:
    rsm = _SKILL_LOADER.load_reasoning_metadata("failure_clustering")
    assert rsm is not None
    assert rsm.prompt_registry_name == "gso_reasoning_failure_clustering"


def test_metadata_prompt_registry_name_is_populated_for_rca_evidence_extraction() -> None:
    """Plan 3 Task 6 did NOT include this field. Plan 4 backfills it
    by editing the Plan-3 SKILL.md."""
    rsm = _SKILL_LOADER.load_reasoning_metadata("rca_evidence_extraction")
    assert rsm is not None
    assert rsm.prompt_registry_name == "gso_reasoning_rca_evidence_extraction"


def test_metadata_prompt_registry_name_is_none_for_smoke_test_skill() -> None:
    """Plan 2's reference smoke-test skill does NOT carry the field."""
    rsm = _SKILL_LOADER.load_reasoning_metadata("_reference_smoke_test")
    if rsm is not None:
        assert rsm.prompt_registry_name is None
