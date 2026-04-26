"""Unit tests for the unified asset-semantics contract (PR 27).

Locks the contract that ``build_asset_semantics`` correctly normalises
every available signal — Genie's explicit metric_views shelf, the
column-flag heuristic on tables, the catalog YAML cache, and the
catalog detection outcomes — into a single ``config["_asset_semantics"]``
map keyed by lower-cased fully-qualified identifier. Downstream PRs
(28-32) read through these helpers, so locking the shape here is what
keeps each consumer agreeing on every ref's kind.
"""

from __future__ import annotations

import pytest


def test_explicit_metric_view_shelf_classified_as_mv():
    """Entries on ``data_sources.metric_views`` always map to kind=metric_view."""
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        PROVENANCE_GENIE_METRIC_VIEWS,
        build_asset_semantics,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                "metric_views": [
                    {
                        "identifier": "cat.sch.mv_sales",
                        "column_configs": [
                            {"column_name": "store_id"},
                            {"column_name": "total_revenue", "column_type": "measure"},
                        ],
                    }
                ],
                "tables": [],
            },
        },
    }

    sem = build_asset_semantics(config)
    entry = sem["cat.sch.mv_sales"]
    assert entry["kind"] == KIND_METRIC_VIEW
    assert entry["short_name"] == "mv_sales"
    assert "total_revenue" in entry["measures"]
    assert "store_id" in entry["dimensions"]
    assert PROVENANCE_GENIE_METRIC_VIEWS in entry["provenance"]


def test_table_with_measure_column_flag_reclassified_as_mv():
    """A ``data_sources.tables`` entry with a measure column is an MV."""
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        PROVENANCE_COLUMN_FLAGS,
        build_asset_semantics,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                "metric_views": [],
                "tables": [
                    {
                        "identifier": "cat.sch.fact_sales",
                        "column_configs": [
                            {"column_name": "qty", "is_measure": True},
                            {"column_name": "store_id"},
                        ],
                    }
                ],
            },
        },
    }

    sem = build_asset_semantics(config)
    entry = sem["cat.sch.fact_sales"]
    assert entry["kind"] == KIND_METRIC_VIEW
    assert "qty" in entry["measures"]
    assert "store_id" in entry["dimensions"]
    assert PROVENANCE_COLUMN_FLAGS in entry["provenance"]


def test_catalog_only_metric_view_via_yaml():
    """An MV present only in the catalog YAML cache is still classified."""
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        PROVENANCE_CATALOG,
        build_asset_semantics,
    )

    config = {"_parsed_space": {"data_sources": {"tables": [], "metric_views": []}}}
    catalog_yamls = {
        "cat.sch.mv_only": {
            "source": "cat.sch.fact",
            "measures": [{"name": "revenue", "expr": "SUM(amt)"}],
            "dimensions": [{"name": "store_id"}],
        }
    }

    sem = build_asset_semantics(
        config, catalog_yamls=catalog_yamls, catalog_outcomes={},
    )
    entry = sem["cat.sch.mv_only"]
    assert entry["kind"] == KIND_METRIC_VIEW
    assert "revenue" in entry["measures"]
    assert "store_id" in entry["dimensions"]
    assert PROVENANCE_CATALOG in entry["provenance"]
    assert entry["metric_view_yaml"]["source"] == "cat.sch.fact"


def test_catalog_outcome_attached_to_unknown_ref():
    """Refs that catalog probed but no signal classified retain the outcome."""
    from genie_space_optimizer.common.asset_semantics import (
        KIND_UNKNOWN,
        build_asset_semantics,
    )

    config = {"_parsed_space": {"data_sources": {"tables": [], "metric_views": []}}}
    sem = build_asset_semantics(
        config,
        catalog_yamls={},
        catalog_outcomes={"cat.sch.regular_view": "no_view_text"},
    )
    entry = sem["cat.sch.regular_view"]
    assert entry["kind"] == KIND_UNKNOWN
    assert entry["outcome"] == "no_view_text"


def test_table_without_measure_columns_classified_as_table():
    """A plain table maps to kind=table, not unknown."""
    from genie_space_optimizer.common.asset_semantics import (
        KIND_TABLE,
        build_asset_semantics,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                "metric_views": [],
                "tables": [
                    {
                        "identifier": "cat.sch.dim_store",
                        "column_configs": [
                            {"column_name": "store_id"},
                            {"column_name": "store_name"},
                        ],
                    }
                ],
            },
        },
    }
    sem = build_asset_semantics(config)
    entry = sem["cat.sch.dim_store"]
    assert entry["kind"] == KIND_TABLE
    assert "store_id" in entry["dimensions"]


def test_multi_signal_provenance_merges_when_signals_agree():
    """When both column flags and catalog signal MV, both contribute provenance."""
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        PROVENANCE_CATALOG,
        PROVENANCE_COLUMN_FLAGS,
        build_asset_semantics,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                "metric_views": [],
                "tables": [
                    {
                        "identifier": "cat.sch.fact_sales",
                        "column_configs": [
                            {"column_name": "qty", "is_measure": True},
                        ],
                    }
                ],
            },
        },
    }
    catalog_yamls = {
        "cat.sch.fact_sales": {
            "source": "cat.sch.fact_sales_raw",
            "measures": [{"name": "qty"}, {"name": "revenue"}],
            "dimensions": [],
        }
    }
    sem = build_asset_semantics(config, catalog_yamls=catalog_yamls)
    entry = sem["cat.sch.fact_sales"]
    assert entry["kind"] == KIND_METRIC_VIEW
    assert PROVENANCE_COLUMN_FLAGS in entry["provenance"]
    assert PROVENANCE_CATALOG in entry["provenance"]
    # Both ``qty`` and ``revenue`` are MV measures (catalog adds revenue).
    measure_names = {m.lower() for m in entry["measures"]}
    assert "qty" in measure_names
    assert "revenue" in measure_names


def test_table_refs_backfill_creates_unknown_entries():
    """Refs the run knows about always appear in semantics, even unclassified."""
    from genie_space_optimizer.common.asset_semantics import (
        KIND_UNKNOWN,
        build_asset_semantics,
    )

    config = {"_parsed_space": {"data_sources": {"tables": [], "metric_views": []}}}
    sem = build_asset_semantics(
        config,
        table_refs=[("cat", "sch", "uncategorized_thing")],
    )
    assert "cat.sch.uncategorized_thing" in sem
    assert sem["cat.sch.uncategorized_thing"]["kind"] == KIND_UNKNOWN


def test_uc_columns_populate_dimensions_when_entry_empty():
    """When semantics entry has no measures/dimensions yet, UC cols fill in."""
    from genie_space_optimizer.common.asset_semantics import (
        build_asset_semantics,
    )

    config = {"_parsed_space": {"data_sources": {"tables": [], "metric_views": []}}}
    sem = build_asset_semantics(
        config,
        table_refs=[("cat", "sch", "tbl")],
        uc_columns=[
            {"table_name": "cat.sch.tbl", "column_name": "id"},
            {"table_name": "cat.sch.tbl", "column_name": "name"},
        ],
    )
    entry = sem["cat.sch.tbl"]
    assert {d.lower() for d in entry["dimensions"]} == {"id", "name"}


def test_stamp_asset_semantics_mirrors_to_parsed_space():
    """``stamp_asset_semantics`` writes onto config and parsed_space."""
    from genie_space_optimizer.common.asset_semantics import (
        get_asset_semantics,
        stamp_asset_semantics,
    )

    parsed = {"data_sources": {"tables": [], "metric_views": []}}
    config = {"_parsed_space": parsed}
    sem_map = {"cat.sch.tbl": {"identifier": "cat.sch.tbl", "kind": "table"}}
    stamp_asset_semantics(config, sem_map, mirror_parsed=True)
    assert config["_asset_semantics"] is sem_map
    assert parsed["_asset_semantics"] is sem_map
    # And the reader returns the stamped value.
    assert get_asset_semantics(config) is sem_map


def test_summarize_semantics_counts_kinds_and_mv_with_without_measures():
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        KIND_TABLE,
        summarize_semantics,
    )

    sem = {
        "cat.sch.mv_a": {"kind": KIND_METRIC_VIEW, "measures": ["x"], "outcome": "detected_via_yaml"},
        "cat.sch.mv_b": {"kind": KIND_METRIC_VIEW, "measures": [], "outcome": "detected_via_type"},
        "cat.sch.tbl": {"kind": KIND_TABLE, "measures": []},
        "cat.sch.unknown": {"kind": "unknown"},
    }
    counts = summarize_semantics(sem)
    assert counts["metric_view"] == 2
    assert counts["mv_with_measures"] == 1
    assert counts["mv_without_measures"] == 1
    assert counts["table"] == 1
    assert counts["unknown"] == 1
    assert counts["total"] == 4
    assert counts["with_outcome"] == 2


def test_format_semantics_block_renders_lines_and_outcomes():
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        format_semantics_block,
    )

    sem = {
        "cat.sch.mv_a": {
            "identifier": "cat.sch.mv_a",
            "kind": KIND_METRIC_VIEW,
            "measures": ["x"],
        },
        "cat.sch.failed": {
            "identifier": "cat.sch.failed",
            "kind": "unknown",
            "outcome": "describe_error",
        },
    }
    lines = format_semantics_block(sem)
    head = lines[0]
    assert "metric_views=1" in head
    assert any("describe_error" in line for line in lines)


def test_invariant_warning_fires_only_when_zero_mv_and_mv_rejects_present():
    from genie_space_optimizer.common.asset_semantics import (
        KIND_TABLE,
        invariant_warning_lines,
    )

    sem = {"cat.sch.tbl": {"kind": KIND_TABLE}}

    # No mv_* rejection bucket — no warning.
    assert invariant_warning_lines(sem, {"explain_or_execute_subbuckets": {}}) == []

    # mv_* rejection bucket present — warning fires.
    rc = {"explain_or_execute_subbuckets": {"mv_missing_measure_function": 4}}
    lines = invariant_warning_lines(sem, rc)
    assert lines
    assert "INVARIANT WARNING" in lines[0]


def test_invariant_warning_silent_when_mv_present():
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        invariant_warning_lines,
    )

    sem = {"cat.sch.mv": {"kind": KIND_METRIC_VIEW, "measures": ["x"]}}
    rc = {"explain_or_execute_subbuckets": {"mv_missing_measure_function": 4}}
    assert invariant_warning_lines(sem, rc) == []


def test_metric_view_identifiers_returns_only_mv_kinds():
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        KIND_TABLE,
        metric_view_identifiers,
        stamp_asset_semantics,
    )

    config = {}
    stamp_asset_semantics(
        config,
        {
            "cat.sch.mv_a": {"identifier": "cat.sch.mv_a", "kind": KIND_METRIC_VIEW},
            "cat.sch.tbl": {"identifier": "cat.sch.tbl", "kind": KIND_TABLE},
        },
    )
    assert metric_view_identifiers(config) == {"cat.sch.mv_a"}


def test_metric_view_measures_by_short_name_drops_empty_sets():
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        metric_view_measures_by_short_name,
        stamp_asset_semantics,
    )

    config = {}
    stamp_asset_semantics(
        config,
        {
            "cat.sch.mv_with_measures": {
                "identifier": "cat.sch.mv_with_measures",
                "short_name": "mv_with_measures",
                "kind": KIND_METRIC_VIEW,
                "measures": ["Revenue"],
            },
            "cat.sch.mv_no_measures": {
                "identifier": "cat.sch.mv_no_measures",
                "short_name": "mv_no_measures",
                "kind": KIND_METRIC_VIEW,
                "measures": [],
            },
        },
    )
    out = metric_view_measures_by_short_name(config)
    assert "mv_with_measures" in out
    assert "mv_no_measures" not in out
    assert {m.lower() for m in out["mv_with_measures"]} == {"revenue"}


def test_asset_kind_short_name_fallback_when_only_short_provided():
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        asset_kind,
        is_metric_view,
        stamp_asset_semantics,
    )

    config = {}
    stamp_asset_semantics(
        config,
        {
            "cat.sch.mv_sales": {
                "identifier": "cat.sch.mv_sales",
                "short_name": "mv_sales",
                "kind": KIND_METRIC_VIEW,
            },
        },
    )
    assert asset_kind(config, "cat.sch.mv_sales") == KIND_METRIC_VIEW
    assert asset_kind(config, "MV_SALES") == KIND_METRIC_VIEW
    assert is_metric_view(config, "mv_sales")
    assert not is_metric_view(config, "unknown_thing")


def test_build_and_stamp_from_run_returns_and_stamps():
    from genie_space_optimizer.common.asset_semantics import (
        KIND_METRIC_VIEW,
        build_and_stamp_from_run,
        get_asset_semantics,
    )

    config = {"_parsed_space": {"data_sources": {"tables": [], "metric_views": []}}}
    catalog_yamls = {
        "cat.sch.mv_only": {
            "source": "cat.sch.fact",
            "measures": [{"name": "revenue"}],
            "dimensions": [],
        }
    }
    sem = build_and_stamp_from_run(
        config, catalog_yamls=catalog_yamls,
    )
    assert "cat.sch.mv_only" in sem
    assert get_asset_semantics(config) is sem
    assert sem["cat.sch.mv_only"]["kind"] == KIND_METRIC_VIEW
