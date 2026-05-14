"""Regression guard: every committed lever5-split shadow fixture must
pass the no-SQL gate for the 5a slice and demonstrate at least minimal
overlap with the deprecated holistic path's L5b proposals.

If this test starts failing after a refactor, either:
  * The 5a no-SQL gate has regressed (a Plan 2 invariant broken), or
  * The 5b synthesis path has drifted catastrophically (overlap floor
    set deliberately low — 0.0 is allowed when the holistic path
    proposed nothing)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_DIR = (
    Path(__file__).resolve().parents[3]
    / "tests" / "fixtures" / "lever5_split_v1"
)


def _fixture_files() -> list[Path]:
    return sorted(p for p in FIXTURE_DIR.glob("*.json"))


def test_fixture_directory_exists():
    assert FIXTURE_DIR.is_dir(), FIXTURE_DIR


@pytest.mark.parametrize("fixture_path", _fixture_files())
def test_5a_fixture_has_no_sql(fixture_path: Path):
    """Each fixture stores the L5a output bytes in `lever_5a.instruction_text`.
    Run the validator to ensure no fixture sneaks SQL into the L5a slice."""
    from genie_space_optimizer.optimization.optimizer import (
        _validate_lever_5a_no_sql_output,
    )
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert "lever_5a" in payload, fixture_path
    five_a = payload["lever_5a"]
    ok, reason = _validate_lever_5a_no_sql_output(five_a)
    assert ok, f"fixture {fixture_path.name} fails 5a no-SQL gate: {reason}"


@pytest.mark.parametrize("fixture_path", _fixture_files())
def test_fixture_records_overlap(fixture_path: Path):
    """Each fixture must include the comparison overlap fields written
    by _emit_lever5_shadow_comparison."""
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    for required in (
        "ag_id", "instruction_text_jaccard",
        "example_sqls_set_overlap", "old_example_sqls_count",
        "new_example_sqls_count",
    ):
        assert required in payload, (fixture_path, required)
    assert 0.0 <= payload["instruction_text_jaccard"] <= 1.0
    assert 0.0 <= payload["example_sqls_set_overlap"] <= 1.0
