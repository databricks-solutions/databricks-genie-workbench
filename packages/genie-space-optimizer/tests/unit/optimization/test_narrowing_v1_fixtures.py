"""Regression guard: every committed narrowing-v1 fixture must
demonstrate the contract block is absent from the captured prompt.

If this test ever starts failing after a refactor, a code change has
silently re-injected `_RCA_CONTRACT_HEADER` into a non-causal prompt
and the next trial run's fixture is now stale."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_DIR = (
    Path(__file__).resolve().parents[3]
    / "tests" / "fixtures" / "narrowing_v1"
)

EXPECTED_SKILL_IDS = frozenset({
    "preflight-instruction-expand",
    "lever-4-join-discovery",
    "preflight-sql-expression-seeding",
})


def _fixture_files() -> list[Path]:
    return sorted(p for p in FIXTURE_DIR.glob("*.json"))


def test_fixture_directory_exists():
    assert FIXTURE_DIR.is_dir(), FIXTURE_DIR


@pytest.mark.parametrize("fixture_path", _fixture_files())
def test_fixture_has_contract_stripped(fixture_path: Path):
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert "skill_id" in payload, fixture_path
    assert payload["skill_id"] in EXPECTED_SKILL_IDS, payload["skill_id"]
    assert "prompt_bytes" in payload, fixture_path
    assert "<unified_rca_engine_contract>" not in payload["prompt_bytes"], (
        f"fixture {fixture_path.name} for skill {payload['skill_id']} "
        f"still contains the contract block — Plan 1 narrowing has "
        f"regressed."
    )


def test_fixtures_cover_all_three_skills_when_present():
    """If any fixtures are committed at all, ensure all 3 skills are
    represented. This catches partial fixture sets sneaking into git."""
    files = _fixture_files()
    if not files:
        pytest.skip("no fixtures committed yet — first trial run pending")
    skill_ids_present = set()
    for p in files:
        payload = json.loads(p.read_text(encoding="utf-8"))
        skill_ids_present.add(payload["skill_id"])
    missing = EXPECTED_SKILL_IDS - skill_ids_present
    assert not missing, f"committed fixture set is incomplete: missing={missing}"
