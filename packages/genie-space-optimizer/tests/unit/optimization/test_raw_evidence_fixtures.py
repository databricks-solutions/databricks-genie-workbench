"""Regression guard: every committed raw_evidence fixture must:
  1. Reference a known skill_id from the projector / excluded set.
  2. Have valid structural_diff token.
  3. Have non-negative proposal counts.

If this test fails after a refactor, either:
  * A skill_id was added/removed without updating
    optimization/raw_evidence.py:_PROJECTOR_TABLE / _EXCLUDED_SKILLS.
  * The shadow-comparison record schema changed."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_DIR = (
    Path(__file__).resolve().parents[3]
    / "tests" / "fixtures" / "raw_evidence_v1"
)

_VALID_DIFF_TOKENS = {
    "identical", "count_differs", "keys_differ",
    "content_differs", "both_empty",
}


def _fixture_files() -> list[Path]:
    return sorted(p for p in FIXTURE_DIR.glob("*.json"))


def test_fixture_directory_exists():
    assert FIXTURE_DIR.is_dir(), FIXTURE_DIR


@pytest.mark.parametrize("fixture_path", _fixture_files())
def test_fixture_skill_id_known(fixture_path: Path):
    from genie_space_optimizer.optimization.raw_evidence import (
        _PROJECTOR_TABLE, _EXCLUDED_SKILLS,
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    sid = payload.get("skill_id", "")
    assert (sid in _PROJECTOR_TABLE) or (sid in _EXCLUDED_SKILLS), (
        f"unknown skill_id in fixture {fixture_path.name}: {sid!r}"
    )


@pytest.mark.parametrize("fixture_path", _fixture_files())
def test_fixture_structural_diff_valid(fixture_path: Path):
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert payload.get("structural_diff") in _VALID_DIFF_TOKENS


@pytest.mark.parametrize("fixture_path", _fixture_files())
def test_fixture_proposal_counts_non_negative(fixture_path: Path):
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert payload.get("off_proposal_count", 0) >= 0
    assert payload.get("on_proposal_count", 0) >= 0
    assert payload.get("n_evidence", 0) >= 0
