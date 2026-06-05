"""Trial 21 W6+C1 unit tests — per-patch-family required-asset gate.

Pins the table in ``repair_diagnosis._REQUIRED_ASSET_TABLE`` against
the Trial 21 postmortem-replay fixture's
``expected_after_w6_per_family`` vocabulary so the Evidence Actuator's
drop-reason output is stable across releases.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.repair_diagnosis import (
    required_assets_for_patch_family,
)


@pytest.mark.parametrize(
    "patch_type",
    [
        "add_column_description",
        "update_column_description",
        "add_table_description",
        "add_description",
        "update_description",
    ],
)
def test_description_families_require_implicated_assets(patch_type: str):
    """Description families: drop with MISSING_IMPLICATED_ASSETS when
    implicated_assets is empty."""
    verdict = required_assets_for_patch_family(
        patch_type=patch_type,
        implicated_assets=[],
        justification="",
    )
    assert verdict.outcome == "drop"
    assert verdict.drop_reason == "MISSING_IMPLICATED_ASSETS"


@pytest.mark.parametrize(
    "patch_type",
    [
        "add_sql_snippet_filter",
        "add_sql_snippet_expression",
        "add_sql_snippet_measure",
        "add_sql_snippet_join",
    ],
)
def test_snippet_families_require_implicated_assets(patch_type: str):
    verdict = required_assets_for_patch_family(
        patch_type=patch_type,
        implicated_assets=[],
        justification="",
    )
    assert verdict.outcome == "drop"
    assert verdict.drop_reason == "MISSING_IMPLICATED_ASSETS"


def test_add_example_sql_requires_sql_shape_delta():
    """add_example_sql: drop with MISSING_IMPLICATED_ASSETS when
    sql_shape_delta is empty (the "shape" the LLM is supposed to
    describe). Non-empty sql_shape_delta admits the proposal even
    without implicated_assets."""
    drop = required_assets_for_patch_family(
        patch_type="add_example_sql",
        implicated_assets=[],
        justification="",
        sql_shape_delta="",
    )
    assert drop.outcome == "drop"
    assert drop.drop_reason == "MISSING_IMPLICATED_ASSETS"

    admit = required_assets_for_patch_family(
        patch_type="add_example_sql",
        implicated_assets=[],
        justification="",
        sql_shape_delta="GROUP BY store_id ORDER BY total_sales DESC LIMIT 5",
    )
    assert admit.outcome == "admitted"


def test_add_instruction_requires_justification():
    """add_instruction: drop with UNJUSTIFIED_SINGLE_LEVER when
    justification is empty, admit when a non-empty justification is
    supplied."""
    drop = required_assets_for_patch_family(
        patch_type="add_instruction",
        implicated_assets=[],
        justification="",
    )
    assert drop.outcome == "drop"
    assert drop.drop_reason == "UNJUSTIFIED_SINGLE_LEVER"

    admit = required_assets_for_patch_family(
        patch_type="add_instruction",
        implicated_assets=[],
        justification="prior iterations missed the rank-by-metric step",
    )
    assert admit.outcome == "admitted"


def test_unrecognized_patch_type_fails_open():
    """Unknown patch_type → admitted (the table is the source of
    truth; new families must be added explicitly with a matching
    postmortem-replay assertion)."""
    verdict = required_assets_for_patch_family(
        patch_type="add_brand_new_family_v999",
        implicated_assets=[],
        justification="",
    )
    assert verdict.outcome == "admitted"
    assert verdict.required == "none"


def test_implicated_assets_present_admits_description_family():
    verdict = required_assets_for_patch_family(
        patch_type="add_column_description",
        implicated_assets=["main.sales.orders.store_id"],
        justification="",
    )
    assert verdict.outcome == "admitted"


def test_drop_carries_feedback_for_retry():
    """The dropped verdict carries a non-empty feedback string so the
    next iteration's Stage 3 prompt can include it."""
    verdict = required_assets_for_patch_family(
        patch_type="add_column_description",
        implicated_assets=[],
        justification="",
    )
    assert verdict.outcome == "drop"
    assert verdict.feedback


# ── Trial 24 W24.3 — kit-aware justification waiver ───────────────────


@pytest.mark.parametrize("patch_type", ["add_instruction", "update_instruction"])
def test_kit_member_waives_justification(patch_type: str):
    """An instruction family member of a multi-lever kit is admitted
    even with an empty justification — the kit IS the justification."""
    admit = required_assets_for_patch_family(
        patch_type=patch_type,
        implicated_assets=[],
        justification="",
        in_multi_lever_kit=True,
    )
    assert admit.outcome == "admitted"


def test_lone_instruction_still_dropped_without_kit():
    """The waiver is scoped to kit members — a lone instruction with no
    justification and no kit still drops as UNJUSTIFIED_SINGLE_LEVER."""
    drop = required_assets_for_patch_family(
        patch_type="add_instruction",
        implicated_assets=[],
        justification="",
        in_multi_lever_kit=False,
    )
    assert drop.outcome == "drop"
    assert drop.drop_reason == "UNJUSTIFIED_SINGLE_LEVER"


def test_kit_waiver_does_not_relax_asset_bearing_families():
    """The kit waiver only covers the justification shape. A snippet
    family in a kit still must carry its own implicated_assets."""
    drop = required_assets_for_patch_family(
        patch_type="add_sql_snippet_filter",
        implicated_assets=[],
        justification="",
        in_multi_lever_kit=True,
    )
    assert drop.outcome == "drop"
    assert drop.drop_reason == "MISSING_IMPLICATED_ASSETS"
