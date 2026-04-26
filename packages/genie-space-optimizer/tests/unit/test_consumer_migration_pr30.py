"""PR 30 — MV consumers delegate to asset semantics first.

Locks the contract that:

1. ``effective_metric_view_identifiers_with_catalog`` includes assets
   classified by semantics even when ``_metric_view_yaml`` and the
   column-flag heuristic miss them.
2. ``build_metric_view_measures`` includes measures from semantics even
   when only semantics knows about them.
3. ``schema_traits`` returns ``has_metric_view`` when only semantics
   identifies an MV (no legacy signal).
4. Preflight ``_effective_data_source_split`` promotes
   semantics-classified entries into the metric_views list.
"""

from __future__ import annotations

from genie_space_optimizer.common.asset_semantics import (
    KIND_METRIC_VIEW,
    KIND_TABLE,
    stamp_asset_semantics,
)


def _stamp(config: dict, mapping: dict[str, dict]) -> None:
    stamp_asset_semantics(config, mapping)


def test_effective_metric_view_identifiers_includes_semantics_only_mvs():
    from genie_space_optimizer.optimization.evaluation import (
        effective_metric_view_identifiers_with_catalog,
    )

    config = {
        "data_sources": {"tables": [], "metric_views": []},
    }
    _stamp(config, {
        "cat.sch.mv_revenue": {
            "identifier": "cat.sch.mv_revenue",
            "short_name": "mv_revenue",
            "kind": KIND_METRIC_VIEW,
            "measures": ["revenue"],
            "dimensions": ["region"],
        },
    })

    idents = effective_metric_view_identifiers_with_catalog(config)
    assert "cat.sch.mv_revenue" in idents


def test_build_metric_view_measures_unions_semantics_layer():
    from genie_space_optimizer.optimization.evaluation import (
        build_metric_view_measures,
    )

    config: dict = {"data_sources": {}}
    _stamp(config, {
        "cat.sch.mv_revenue": {
            "identifier": "cat.sch.mv_revenue",
            "short_name": "mv_revenue",
            "kind": KIND_METRIC_VIEW,
            "measures": ["revenue", "Profit"],
        },
    })

    out = build_metric_view_measures(config)
    assert "mv_revenue" in out
    assert {"revenue", "profit"} <= out["mv_revenue"]


def test_schema_traits_has_metric_view_from_semantics_only():
    from genie_space_optimizer.optimization.archetypes import schema_traits

    snapshot = {
        "data_sources": {
            "tables": [
                {"identifier": "cat.sch.fact", "column_configs": []},
            ],
            "metric_views": [],
        },
    }
    _stamp(snapshot, {
        "cat.sch.mv_x": {
            "identifier": "cat.sch.mv_x",
            "short_name": "mv_x",
            "kind": KIND_METRIC_VIEW,
            "measures": ["m1"],
        },
    })

    traits = schema_traits(snapshot)
    assert "has_metric_view" in traits


def test_effective_data_source_split_promotes_semantics_classified():
    from genie_space_optimizer.optimization.preflight_synthesis import (
        _effective_data_source_split,
    )

    snapshot = {
        "data_sources": {
            "tables": [
                # Looks like a table — no measure flags — but semantics
                # says metric_view.
                {
                    "identifier": "cat.sch.mv_revenue",
                    "column_configs": [
                        {"column_name": "region", "data_type": "STRING"},
                        {"column_name": "revenue", "data_type": "DOUBLE"},
                    ],
                },
                {
                    "identifier": "cat.sch.dim_store",
                    "column_configs": [
                        {"column_name": "store_id", "data_type": "STRING"},
                    ],
                },
            ],
            "metric_views": [],
        },
    }
    _stamp(snapshot, {
        "cat.sch.mv_revenue": {
            "identifier": "cat.sch.mv_revenue",
            "short_name": "mv_revenue",
            "kind": KIND_METRIC_VIEW,
            "measures": ["revenue"],
        },
        "cat.sch.dim_store": {
            "identifier": "cat.sch.dim_store",
            "short_name": "dim_store",
            "kind": KIND_TABLE,
        },
    })

    real_tables, metric_views = _effective_data_source_split(snapshot)
    table_ids = {t.get("identifier") for t in real_tables}
    mv_ids = {m.get("identifier") for m in metric_views}
    assert "cat.sch.mv_revenue" in mv_ids
    assert "cat.sch.dim_store" in table_ids
    assert "cat.sch.mv_revenue" not in table_ids


def test_count_mv_detection_sources_includes_semantics_only_mvs():
    from genie_space_optimizer.optimization.evaluation import (
        _count_mv_detection_sources,
    )

    config = {
        "data_sources": {"tables": [], "metric_views": []},
    }
    _stamp(config, {
        "cat.sch.mv_only_in_semantics": {
            "identifier": "cat.sch.mv_only_in_semantics",
            "short_name": "mv_only_in_semantics",
            "kind": KIND_METRIC_VIEW,
        },
    })

    counts = _count_mv_detection_sources(config)
    assert counts["catalog"] >= 1


def test_legacy_only_paths_still_work_without_semantics():
    """Snapshots that pre-date the asset semantics contract must still
    be classified correctly via the legacy MV-yaml cache."""
    from genie_space_optimizer.optimization.evaluation import (
        effective_metric_view_identifiers_with_catalog,
        build_metric_view_measures,
    )

    config = {
        "data_sources": {"tables": [], "metric_views": []},
        "_metric_view_yaml": {
            "cat.sch.mv_legacy": {
                "source": "cat.sch.fact_sales",
                "measures": [{"name": "total_revenue"}],
                "dimensions": [{"name": "region"}],
            },
        },
    }
    idents = effective_metric_view_identifiers_with_catalog(config)
    assert any("mv_legacy" in i.lower() for i in idents)
    measures = build_metric_view_measures(config)
    assert "mv_legacy" in measures
    assert "total_revenue" in measures["mv_legacy"]
