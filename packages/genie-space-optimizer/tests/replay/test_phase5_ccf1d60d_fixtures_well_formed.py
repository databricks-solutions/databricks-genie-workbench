"""Validates that the five Phase 5 ccf1d60d fixtures exist and have the
shape required by Tasks 3-7. This test runs FIRST in the Phase 5 CI gate
so a broken fixture is detected before downstream test failures hide it.
"""
from __future__ import annotations
import json
from pathlib import Path

import pytest

FIX_ROOT = Path(__file__).parent / "fixtures" / "phase5"
RUN_ID = "ccf1d60d-d686-467b-bafa-1640131b4393"


REQUIRED_FILES = (
    "ccf1d60d_iter1.json",
    "ccf1d60d_iter2.json",
    "ccf1d60d_iter3.json",
    "ccf1d60d_rca_card_gs026.json",
    "ccf1d60d_iter1_surviving_patches.json",
)


@pytest.mark.parametrize("filename", REQUIRED_FILES)
def test_phase5_ccf1d60d_fixture_exists(filename: str) -> None:
    path = FIX_ROOT / filename
    assert path.exists(), f"missing Phase 5 fixture: {path}"
    payload = json.loads(path.read_text())
    # All five fixtures are top-level dicts that carry the run_id.
    # The surviving-patches fixture wraps its patch list under
    # `patches` so this contract holds; see _source notes in the
    # fixture body for provenance.
    assert isinstance(payload, dict), (
        f"{filename}: expected top-level dict, got {type(payload).__name__}"
    )
    assert payload.get("run_id") == RUN_ID


def test_iter1_carries_aggregate_gain_and_regression() -> None:
    payload = json.loads((FIX_ROOT / "ccf1d60d_iter1.json").read_text())
    assert payload["iteration"] == 1
    assert payload["baseline_post_arbiter"] == pytest.approx(87.0, abs=0.05)
    assert payload["candidate_post_arbiter"] == pytest.approx(91.0, abs=0.5)
    assert "gs_021" in payload["out_of_target_regressed_qids"]
    assert "gs_026" in payload["target_qids"]
    assert payload["accepted_in_recorded_run"] is False


def test_iter2_carries_zero_proposals_for_same_ag() -> None:
    payload = json.loads((FIX_ROOT / "ccf1d60d_iter2.json").read_text())
    assert payload["iteration"] == 2
    assert payload["ag_id_selected"] == payload.get(
        "iter1_ag_id_selected"
    ), "iter2 must re-select the same AG as iter1"
    assert payload["proposal_count"] == 0


def test_rca_card_intended_patch_shape_is_structural() -> None:
    card = json.loads((FIX_ROOT / "ccf1d60d_rca_card_gs026.json").read_text())
    assert card["target_qid"] == "gs_026"
    assert card["intended_patch_shape"] == "structural"


def test_iter1_surviving_patches_metadata_only() -> None:
    payload = json.loads(
        (FIX_ROOT / "ccf1d60d_iter1_surviving_patches.json").read_text()
    )
    patches = payload["patches"]
    assert len(patches) >= 2
    types = {p["patch_type"] for p in patches}
    assert types <= {
        "update_column_description",
        "add_column_synonym",
        "add_sql_snippet",
    }, f"non-metadata patch types present: {types}"
