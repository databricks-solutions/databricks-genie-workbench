"""PR 29 — Join discovery / join-spec gating via asset semantics.

Locks the contract that:

1. ``discover_join_candidates`` drops hint pairs whose left or right
   identifier resolves to ``kind == metric_view`` per
   ``_asset_semantics``.
2. ``filter_join_specs_by_semantics`` drops existing specs that touch
   metric views, and reports skip counters for the call-site banner.
3. Plain table-table joins are unaffected.
4. ``mv_*``-named *views* are not skipped unless asset semantics
   actively classifies them as metric views (no name-based skipping).
"""

from __future__ import annotations

from genie_space_optimizer.common.asset_semantics import (
    KIND_METRIC_VIEW,
    KIND_TABLE,
    KIND_UNKNOWN,
    KIND_VIEW,
    stamp_asset_semantics,
)
from genie_space_optimizer.optimization.optimizer import (
    discover_join_candidates,
    filter_join_specs_by_semantics,
)


def _make_table(identifier: str, columns: list[tuple[str, str]]) -> dict:
    return {
        "identifier": identifier,
        "name": identifier.split(".")[-1],
        "column_configs": [
            {"column_name": name, "data_type": dt}
            for (name, dt) in columns
        ],
    }


def _stamp(config: dict, semantics_map: dict[str, dict]) -> None:
    stamp_asset_semantics(config, semantics_map)


def test_discover_drops_pair_when_left_is_metric_view():
    metadata = {
        "data_sources": {
            "tables": [
                _make_table("cat.sch.mv_revenue", [
                    ("store_id", "STRING"),
                    ("region", "STRING"),
                    ("revenue", "DOUBLE"),
                ]),
                _make_table("cat.sch.fact_sales", [
                    ("store_id", "STRING"),
                    ("sale_date", "DATE"),
                ]),
            ],
        },
    }
    metadata["tables"] = metadata["data_sources"]["tables"]

    _stamp(metadata, {
        "cat.sch.mv_revenue": {
            "identifier": "cat.sch.mv_revenue",
            "short_name": "mv_revenue",
            "kind": KIND_METRIC_VIEW,
            "measures": ["revenue"],
            "dimensions": ["store_id", "region"],
            "provenance": ["catalog"],
        },
        "cat.sch.fact_sales": {
            "identifier": "cat.sch.fact_sales",
            "short_name": "fact_sales",
            "kind": KIND_TABLE,
        },
    })

    hints = discover_join_candidates(metadata)
    pairs = {(h["left_table"], h["right_table"]) for h in hints}
    assert ("cat.sch.mv_revenue", "cat.sch.fact_sales") not in pairs
    assert ("cat.sch.fact_sales", "cat.sch.mv_revenue") not in pairs


def test_discover_keeps_table_table_pairs_unchanged():
    metadata = {
        "data_sources": {
            "tables": [
                _make_table("cat.sch.fact_sales", [
                    ("store_id", "STRING"),
                    ("sale_date", "DATE"),
                ]),
                _make_table("cat.sch.dim_store", [
                    ("store_id", "STRING"),
                    ("region", "STRING"),
                ]),
            ],
        },
    }
    metadata["tables"] = metadata["data_sources"]["tables"]

    _stamp(metadata, {
        "cat.sch.fact_sales": {
            "identifier": "cat.sch.fact_sales",
            "short_name": "fact_sales",
            "kind": KIND_TABLE,
        },
        "cat.sch.dim_store": {
            "identifier": "cat.sch.dim_store",
            "short_name": "dim_store",
            "kind": KIND_TABLE,
        },
    })

    hints = discover_join_candidates(metadata)
    assert any(
        h["left_table"] == "cat.sch.fact_sales"
        and h["right_table"] == "cat.sch.dim_store"
        for h in hints
    )


def test_discover_keeps_mv_named_view_when_kind_is_view():
    """``mv_*`` named *views* (kind=view, not metric_view) must not be
    skipped — name-only classification is forbidden."""
    metadata = {
        "data_sources": {
            "tables": [
                _make_table("cat.sch.mv_dim_location", [
                    ("location_id", "STRING"),
                    ("region", "STRING"),
                ]),
                _make_table("cat.sch.fact_sales", [
                    ("location_id", "STRING"),
                    ("sale_date", "DATE"),
                ]),
            ],
        },
    }
    metadata["tables"] = metadata["data_sources"]["tables"]

    _stamp(metadata, {
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

    hints = discover_join_candidates(metadata)
    assert any(
        {
            h["left_table"], h["right_table"],
        } == {"cat.sch.mv_dim_location", "cat.sch.fact_sales"}
        for h in hints
    )


def test_discover_drops_unresolved_catalog_failure_pair():
    metadata = {
        "data_sources": {
            "tables": [
                _make_table("cat.sch.mv_maybe", [
                    ("store_id", "STRING"),
                    ("revenue", "DOUBLE"),
                ]),
                _make_table("cat.sch.fact_sales", [
                    ("store_id", "STRING"),
                    ("sale_date", "DATE"),
                ]),
            ],
        },
    }
    metadata["tables"] = metadata["data_sources"]["tables"]

    _stamp(metadata, {
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

    hints = discover_join_candidates(metadata)
    assert not any(
        {h["left_table"], h["right_table"]} == {"cat.sch.mv_maybe", "cat.sch.fact_sales"}
        for h in hints
    )


def test_filter_join_specs_drops_mv_left_with_counters():
    metadata: dict = {}
    _stamp(metadata, {
        "cat.sch.mv_revenue": {
            "identifier": "cat.sch.mv_revenue",
            "short_name": "mv_revenue",
            "kind": KIND_METRIC_VIEW,
        },
        "cat.sch.fact_sales": {
            "identifier": "cat.sch.fact_sales",
            "short_name": "fact_sales",
            "kind": KIND_TABLE,
        },
        "cat.sch.dim_store": {
            "identifier": "cat.sch.dim_store",
            "short_name": "dim_store",
            "kind": KIND_TABLE,
        },
    })
    specs = [
        {
            "left": {"identifier": "cat.sch.mv_revenue"},
            "right": {"identifier": "cat.sch.fact_sales"},
            "sql": ["mv_revenue.store_id = fact_sales.store_id"],
        },
        {
            "left": {"identifier": "cat.sch.fact_sales"},
            "right": {"identifier": "cat.sch.dim_store"},
            "sql": ["fact_sales.store_id = dim_store.store_id"],
        },
    ]
    counters: dict[str, int] = {}
    skipped: list[tuple[str, str]] = []
    kept = filter_join_specs_by_semantics(
        metadata, specs, counters=counters, skipped_examples=skipped,
    )
    assert len(kept) == 1
    assert kept[0]["left"]["identifier"] == "cat.sch.fact_sales"
    assert counters.get("joins_skipped_metric_view_left") == 1
    assert (
        "cat.sch.mv_revenue", "cat.sch.fact_sales",
    ) in skipped


def test_filter_join_specs_drops_mv_right():
    metadata: dict = {}
    _stamp(metadata, {
        "cat.sch.mv_revenue": {
            "identifier": "cat.sch.mv_revenue",
            "short_name": "mv_revenue",
            "kind": KIND_METRIC_VIEW,
        },
        "cat.sch.fact_sales": {
            "identifier": "cat.sch.fact_sales",
            "short_name": "fact_sales",
            "kind": KIND_TABLE,
        },
    })
    specs = [
        {
            "left": {"identifier": "cat.sch.fact_sales"},
            "right": {"identifier": "cat.sch.mv_revenue"},
            "sql": ["fact_sales.store_id = mv_revenue.store_id"],
        },
    ]
    counters: dict[str, int] = {}
    kept = filter_join_specs_by_semantics(
        metadata, specs, counters=counters,
    )
    assert kept == []
    assert counters.get("joins_skipped_metric_view_right") == 1


def test_filter_join_specs_drops_unresolved_catalog_failure():
    metadata: dict = {}
    _stamp(metadata, {
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
        "cat.sch.dim_store": {
            "identifier": "cat.sch.dim_store",
            "short_name": "dim_store",
            "kind": KIND_TABLE,
        },
    })
    specs = [
        {
            "left": {"identifier": "cat.sch.mv_maybe"},
            "right": {"identifier": "cat.sch.fact_sales"},
            "sql": ["mv_maybe.store_id = fact_sales.store_id"],
        },
        {
            "left": {"identifier": "cat.sch.fact_sales"},
            "right": {"identifier": "cat.sch.dim_store"},
            "sql": ["fact_sales.store_id = dim_store.store_id"],
        },
    ]
    counters: dict[str, int] = {}
    skipped: list[tuple[str, str]] = []

    kept = filter_join_specs_by_semantics(
        metadata, specs, counters=counters, skipped_examples=skipped,
    )

    assert len(kept) == 1
    assert kept[0]["right"]["identifier"] == "cat.sch.dim_store"
    assert counters.get("joins_skipped_unresolved_asset_left") == 1
    assert ("cat.sch.mv_maybe", "cat.sch.fact_sales") in skipped


def test_filter_join_specs_no_semantics_keeps_all():
    """When semantics is empty (older snapshot), the filter must be a
    no-op rather than dropping every spec."""
    metadata: dict = {}
    specs = [
        {
            "left": {"identifier": "cat.sch.a"},
            "right": {"identifier": "cat.sch.b"},
            "sql": ["a.id = b.id"],
        },
    ]
    kept = filter_join_specs_by_semantics(metadata, specs)
    assert len(kept) == 1
