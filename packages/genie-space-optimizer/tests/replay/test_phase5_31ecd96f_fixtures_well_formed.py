from __future__ import annotations
import json
from pathlib import Path

import pytest

FIX_ROOT = Path(__file__).parent / "fixtures" / "phase5"
RUN_ID = "31ecd96f-5d56-4b5a-af8e-38e9e5c549af"

REQUIRED_FILES = (
    "31ecd96f_iter1_h001.json",
    "31ecd96f_iter2_collateral.json",
    "31ecd96f_iter2_iter4_alternation.json",
)


@pytest.mark.parametrize("filename", REQUIRED_FILES)
def test_phase5_31ecd96f_fixture_exists(filename: str) -> None:
    path = FIX_ROOT / filename
    assert path.exists(), f"missing Phase 5 fixture: {path}"
    payload = json.loads(path.read_text())
    assert payload["run_id"] == RUN_ID


def test_iter1_h001_directive_is_no_structural_candidate() -> None:
    payload = json.loads((FIX_ROOT / "31ecd96f_iter1_h001.json").read_text())
    assert payload["ag_id"].startswith("H001")
    l5 = payload["directives"]["L5"]
    assert l5["outcome"] == "no_structural_candidate"


def test_iter2_collateral_drop_against_gs003() -> None:
    payload = json.loads(
        (FIX_ROOT / "31ecd96f_iter2_collateral.json").read_text()
    )
    dropped = payload["dropped_patches"]
    matches = [
        p for p in dropped
        if p["patch_type"] == "add_sql_snippet_measure"
        and p["target_table"].endswith("tkt_payment")
        and "gs_003" in p["dropped_for_dependents"]
    ]
    assert matches, f"expected collateral drop, found: {dropped}"


def test_alternation_shows_h002_repeats() -> None:
    payload = json.loads(
        (FIX_ROOT / "31ecd96f_iter2_iter4_alternation.json").read_text()
    )
    seq = payload["ag_selection_sequence"]
    assert seq == ["H002", "H001", "H002"] or seq.count("H002") >= 2
    assert all(
        e == "no_applied_patches"
        for e in payload["terminal_reason_sequence_for_h002"]
    )
