"""Pin RCA target binding before Lever-1 patch-theme emission."""

from __future__ import annotations

from genie_space_optimizer.optimization.rca import (
    _bind_column_targets,
    _split_table_column,
)


def test_split_table_column_handles_fully_qualified_column() -> None:
    assert _split_table_column("cat.sch.mv_esr_dim_location.zone_vp_name") == (
        "cat.sch.mv_esr_dim_location",
        "zone_vp_name",
    )


def test_split_table_column_rejects_list_shaped_string() -> None:
    assert _split_table_column(
        "[mv_7now_fact_sales, mv_esr_dim_location, zone_vp_name, cy_sales]"
    ) == ("", "")


def test_bind_column_targets_uses_metric_view_hint_for_unqualified_column() -> None:
    bound = _bind_column_targets(
        ("zone_vp_name",),
        metadata_snapshot={
            "_uc_columns": [
                {
                    "table_full_name": "cat.sch.mv_esr_dim_location",
                    "column_name": "zone_vp_name",
                }
            ]
        },
    )
    assert bound == (("cat.sch.mv_esr_dim_location", "zone_vp_name"),)


def test_bind_column_targets_drops_ambiguous_unqualified_column() -> None:
    bound = _bind_column_targets(
        ("time_window",),
        metadata_snapshot={
            "_uc_columns": [
                {"table_full_name": "cat.sch.mv_fact", "column_name": "time_window"},
                {"table_full_name": "cat.sch.mv_store", "column_name": "time_window"},
            ]
        },
    )
    assert bound == ()


def test_bind_column_targets_deduplicates_concrete_pairs() -> None:
    bound = _bind_column_targets(
        (
            "cat.sch.mv_esr_dim_location.zone_vp_name",
            "cat.sch.mv_esr_dim_location.zone_vp_name",
        ),
        metadata_snapshot={},
    )
    assert bound == (("cat.sch.mv_esr_dim_location", "zone_vp_name"),)
