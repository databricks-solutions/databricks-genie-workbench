"""Regression guard: every committed three-stage shadow fixture must
pass these invariants:
  1. Stage-1 picks resolve to known skill_ids in
     _THREE_STAGE_SKILL_NAMES.
  2. Each Stage-2 result envelope has shape
     {skill_id, ag_id, proposals, [error]}.
  3. Structural overlap with the legacy strategist's lever_directives
     is in [0.0, 1.0].

If this test starts failing after a refactor, either:
  * A skill_id was removed from _THREE_STAGE_SKILL_NAMES without
    updating discovery prompt + adapters, or
  * The Stage-2 envelope shape regressed."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_DIR = (
    Path(__file__).resolve().parents[3]
    / "tests" / "fixtures" / "three_stage_v1"
)


def _fixture_files() -> list[Path]:
    return sorted(p for p in FIXTURE_DIR.glob("*.json"))


def test_fixture_directory_exists():
    assert FIXTURE_DIR.is_dir(), FIXTURE_DIR


@pytest.mark.parametrize("fixture_path", _fixture_files())
def test_fixture_stage_1_picks_resolve_to_known_skills(fixture_path: Path):
    from genie_space_optimizer.common.config import _THREE_STAGE_SKILL_NAMES
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert "stage_1_skill_ids" in payload
    for sid in payload["stage_1_skill_ids"]:
        assert sid in _THREE_STAGE_SKILL_NAMES, (fixture_path.name, sid)


@pytest.mark.parametrize("fixture_path", _fixture_files())
def test_fixture_stage_2_envelopes_well_formed(fixture_path: Path):
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert "stage_2_results" in payload
    for r in payload["stage_2_results"]:
        assert isinstance(r, dict)
        assert "skill_id" in r
        assert "ag_id" in r
        assert "proposals" in r
        assert isinstance(r["proposals"], list)


@pytest.mark.parametrize("fixture_path", _fixture_files())
def test_fixture_overlap_in_unit_range(fixture_path: Path):
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    overlap = payload.get("structural_overlap")
    assert overlap is not None
    assert 0.0 <= overlap <= 1.0
