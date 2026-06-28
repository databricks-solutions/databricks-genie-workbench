"""P0 replay assertion: the fixture's H002 expression patch survives
narrow-replacement when GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION is on.
"""

from __future__ import annotations

import json
import pathlib

import pytest


_FIXTURE = (
    pathlib.Path(__file__).parent / "fixtures"
    / "run_900000000000001_11110001_pre_p0_fix.json"
)


def _h002_dropped_expression_from_fixture(fixture_data: dict) -> dict | None:
    """Find the first H002 add_sql_snippet_expression drop in the
    fixture's per-iteration drop logs."""
    for it in (fixture_data.get("iterations") or []):
        for drop in (it.get("blast_radius_drops") or []):
            if (
                str(drop.get("reason") or "")
                == "high_collateral_risk_flagged"
                and str(drop.get("patch_type") or "")
                == "add_sql_snippet_expression"
            ):
                return drop.get("original_patch") or drop
    return None


def test_h002_expression_drop_produces_narrow_survivor_when_flag_on(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION", "1")
    monkeypatch.setenv(
        "GSO_L6_NARROW_REPLACEMENT_PATCH_AWARE", "1"
    )
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_narrow_l6_replacement,
    )

    data = json.loads(_FIXTURE.read_text())
    original = _h002_dropped_expression_from_fixture(data)
    if original is None:
        pytest.skip(
            "Fixture does not record a per-iteration HCRF drop with "
            "patch_type=add_sql_snippet_expression for H002; the "
            "P0 unit-level proof is sufficient."
        )

    out = build_narrow_l6_replacement(
        original_patch=original,
        ag_target_qids=("7now_delivery_analytics_space_gs_026",),
        root_cause="plural_top_n_collapse",
    )
    assert out is not None, (
        "P0 contract: an HCRF-dropped expression patch must produce "
        "a narrow-replacement candidate when the flag is on."
    )
    assert out["narrowing_strategy"] == "expression_qid_scope"
    assert (
        "7now_delivery_analytics_space_gs_026"
        in out["sql_expression"]
    )


def test_h002_expression_drop_produces_no_survivor_when_flag_off(
    monkeypatch,
) -> None:
    monkeypatch.delenv(
        "GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION", raising=False
    )
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        build_narrow_l6_replacement,
    )

    data = json.loads(_FIXTURE.read_text())
    original = _h002_dropped_expression_from_fixture(data)
    if original is None:
        pytest.skip(
            "Fixture does not record an HCRF expression drop; "
            "byte-stability proof is in unit tests."
        )

    out = build_narrow_l6_replacement(
        original_patch=original,
        ag_target_qids=("7now_delivery_analytics_space_gs_026",),
        root_cause="plural_top_n_collapse",
    )
    assert out is None, (
        "Byte-stability: with GSO_L6_NARROW_REPLACEMENT_FOR_EXPRESSION "
        "off, the H002 expression drop must continue to produce no "
        "narrow-replacement candidate."
    )
