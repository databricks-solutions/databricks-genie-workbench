from __future__ import annotations

from genie_space_optimizer.common.asset_semantics import (
    KIND_METRIC_VIEW,
    KIND_TABLE,
    KIND_UNKNOWN,
    KIND_VIEW,
    effective_data_source_split,
    stamp_asset_semantics,
)


def _source(identifier: str, *cols: str) -> dict:
    return {
        "identifier": identifier,
        "name": identifier.split(".")[-1],
        "column_configs": [{"column_name": c, "data_type": "STRING"} for c in cols],
    }


def test_table_shelf_metric_view_is_promoted_from_semantics() -> None:
    config = {
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    _source("cat.sch.mv_sales", "store_id", "total_sales"),
                    _source("cat.sch.dim_store", "store_id", "region"),
                ],
                "metric_views": [],
            },
        },
    }
    stamp_asset_semantics(config, {
        "cat.sch.mv_sales": {
            "identifier": "cat.sch.mv_sales",
            "short_name": "mv_sales",
            "kind": KIND_METRIC_VIEW,
            "measures": ["total_sales"],
            "dimensions": ["store_id"],
        },
        "cat.sch.dim_store": {
            "identifier": "cat.sch.dim_store",
            "short_name": "dim_store",
            "kind": KIND_TABLE,
        },
    })

    split = effective_data_source_split(config)

    assert [t["identifier"] for t in split.tables] == ["cat.sch.dim_store"]
    assert [mv["identifier"] for mv in split.metric_views] == ["cat.sch.mv_sales"]
    assert split.unknown == []
    assert split.raw_table_count == 2
    assert split.raw_metric_view_count == 0


def test_mv_named_plain_view_is_not_promoted_by_name() -> None:
    config = {
        "data_sources": {
            "tables": [
                _source("cat.sch.mv_dim_location", "location_id", "region"),
                _source("cat.sch.fact_sales", "location_id", "sales"),
            ],
            "metric_views": [],
        },
    }
    stamp_asset_semantics(config, {
        "cat.sch.mv_dim_location": {
            "identifier": "cat.sch.mv_dim_location",
            "short_name": "mv_dim_location",
            "kind": KIND_VIEW,
        },
        "cat.sch.fact_sales": {
            "identifier": "cat.sch.fact_sales",
            "short_name": "fact_sales",
            "kind": KIND_TABLE,
        },
    })

    split = effective_data_source_split(config)

    assert {t["identifier"] for t in split.tables} == {
        "cat.sch.mv_dim_location",
        "cat.sch.fact_sales",
    }
    assert split.metric_views == []


def test_unresolved_table_shelf_asset_is_reported_unknown_not_table_safe() -> None:
    config = {
        "data_sources": {
            "tables": [
                _source("cat.sch.mv_maybe", "store_id", "revenue"),
                _source("cat.sch.fact_sales", "store_id", "sale_date"),
            ],
            "metric_views": [],
        },
    }
    stamp_asset_semantics(config, {
        "cat.sch.mv_maybe": {
            "identifier": "cat.sch.mv_maybe",
            "short_name": "mv_maybe",
            "kind": KIND_UNKNOWN,
            "outcome": "no_warehouse",
        },
        "cat.sch.fact_sales": {
            "identifier": "cat.sch.fact_sales",
            "short_name": "fact_sales",
            "kind": KIND_TABLE,
        },
    })

    split = effective_data_source_split(config)

    assert [t["identifier"] for t in split.tables] == ["cat.sch.fact_sales"]
    assert split.metric_views == []
    assert [u["identifier"] for u in split.unknown] == ["cat.sch.mv_maybe"]


def test_explicit_metric_views_are_preserved_and_deduped() -> None:
    mv = _source("cat.sch.mv_sales", "store_id", "total_sales")
    config = {
        "data_sources": {
            "tables": [mv],
            "metric_views": [mv],
        },
    }
    stamp_asset_semantics(config, {
        "cat.sch.mv_sales": {
            "identifier": "cat.sch.mv_sales",
            "short_name": "mv_sales",
            "kind": KIND_METRIC_VIEW,
        },
    })

    split = effective_data_source_split(config)

    assert split.tables == []
    assert [mv["identifier"] for mv in split.metric_views] == ["cat.sch.mv_sales"]
    assert split.raw_table_count == 1
    assert split.raw_metric_view_count == 1
