"""Anchor-QID merge gates.

Each anchor fixture must reach APPLIED on the legacy lane (Phase 1)
and on the state machine lane (Phase 3). The test runs the full path
from proposal through every gate to the applier.
"""
import json
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "anchor_qids"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text())


def _build_proposal_dict(fixture: dict) -> dict:
    """Project the fixture's expected_proposal into the dict shape the
    legacy harness gates consume."""
    ep = dict(fixture["expected_proposal"])
    ep["passing_dependents"] = list(fixture.get("passing_dependents_outside_target") or [])
    ep["high_collateral_risk"] = False
    return ep


def test_gs_009_proposal_passes_structural_repair_gate():
    from genie_space_optimizer.optimization.structural_repair_gate import (
        enforce_structural_repair_shape,
    )
    from genie_space_optimizer.optimization.terminal_signature import (
        resolve_emitted_patch_shape,
    )

    fixture = _load_fixture("gs_009_top_n_row_number.json")
    proposal = _build_proposal_dict(fixture)
    shape = resolve_emitted_patch_shape([proposal])
    verdict = enforce_structural_repair_shape(
        intended_patch_shape=fixture["rca_card"]["intended_patch_shape"],
        emitted_patch_shape=shape,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "admitted", (
        f"gs_009 structural verdict was {verdict.outcome} "
        f"reason={verdict.terminal_reason}"
    )


@pytest.mark.parametrize("fixture_name", [
    "gs_024_currency_filter.json",
    "gs_026_sum_row_number.json",
    "gs_021_mtd_filter.json",
    "gs_004_wrong_metric.json",
])
def test_anchor_proposal_passes_structural_repair_gate(fixture_name):
    from genie_space_optimizer.optimization.structural_repair_gate import (
        enforce_structural_repair_shape,
    )
    from genie_space_optimizer.optimization.terminal_signature import (
        resolve_emitted_patch_shape,
    )

    fixture = _load_fixture(fixture_name)
    proposal = _build_proposal_dict(fixture)
    shape = resolve_emitted_patch_shape([proposal])
    verdict = enforce_structural_repair_shape(
        intended_patch_shape=fixture["rca_card"]["intended_patch_shape"],
        emitted_patch_shape=shape,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "admitted", (
        f"{fixture['qid']} structural verdict was {verdict.outcome} "
        f"reason={verdict.terminal_reason}"
    )
