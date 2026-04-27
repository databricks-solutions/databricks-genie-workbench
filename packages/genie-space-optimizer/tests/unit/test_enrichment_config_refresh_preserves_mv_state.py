"""PR 33 — Enrichment config refresh must preserve metric-view state.

Locks the contract that
:func:`genie_space_optimizer.optimization.harness._refresh_config_preserving_mv_state`
re-fetches the Genie space config without dropping the
``_metric_view_yaml`` / ``_asset_semantics`` caches that were stamped
earlier in the run.

Background: ``fetch_space_config`` rebuilds the config from the Genie
REST API and has no awareness of the catalog-detection caches. Eight
refresh sites in ``_run_enrichment`` previously dropped MV state
mid-run, which is the root cause of the recurring ``MVs detected: 0``,
``METRIC_VIEW_JOIN_NOT_SUPPORTED``, and
``METRIC_VIEW_MISSING_MEASURE_FUNCTION`` cluster.
"""

from __future__ import annotations

from unittest.mock import patch

import genie_space_optimizer.common.genie_client as _genie_client_mod
from genie_space_optimizer.common.asset_semantics import (
    KIND_METRIC_VIEW,
    build_and_stamp_from_run,
    get_asset_semantics,
)
from genie_space_optimizer.optimization.evaluation import (
    _count_mv_detection_sources,
    effective_metric_view_identifiers_with_catalog,
)


def _bare_rest_config() -> dict:
    """Return the shape ``fetch_space_config`` produces from the Genie REST
    API (no MV caches, no semantics — just ``_parsed_space`` + the
    convenience id lists)."""
    return {
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.fact_sales",
                        "column_configs": [
                            {"column_name": "store_id", "data_type": "STRING"},
                        ],
                    },
                ],
                "metric_views": [],
            },
        },
        "_tables": ["cat.sch.fact_sales"],
        "_metric_views": [],
        "_functions": [],
        "_instructions": [],
    }


def _seeded_pre_refresh_config() -> tuple[dict, dict, list[tuple[str, str, str]]]:
    """Return a config with ``_metric_view_yaml`` and ``_asset_semantics``
    populated, mimicking the state immediately after
    ``_prepare_lever_loop`` finishes."""
    yaml_cache = {
        "cat.sch.mv1": {
            "version": 0.1,
            "source": "cat.sch.fact_sales",
            "measures": [{"name": "total_revenue", "expr": "SUM(revenue)"}],
            "dimensions": [{"name": "store_id"}],
        },
    }
    config: dict = {
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.fact_sales",
                        "column_configs": [
                            {"column_name": "store_id", "data_type": "STRING"},
                        ],
                    },
                ],
                "metric_views": [],
            },
        },
        "_tables": ["cat.sch.fact_sales"],
        "_metric_views": ["cat.sch.mv1"],
        "_functions": [],
        "_instructions": [],
        "_metric_view_yaml": dict(yaml_cache),
    }
    table_refs: list[tuple[str, str, str]] = [
        ("cat", "sch", "fact_sales"),
        ("cat", "sch", "mv1"),
    ]
    build_and_stamp_from_run(
        config,
        table_refs=table_refs,
        catalog_yamls=yaml_cache,
        catalog_outcomes={},
        catalog_diagnostic_samples={},
        uc_columns=None,
    )
    return config, yaml_cache, table_refs


def test_refresh_preserves_metric_view_yaml_cache():
    """After a refresh the ``_metric_view_yaml`` cache must still classify
    ``cat.sch.mv1`` as an MV."""
    from genie_space_optimizer.optimization import harness

    pre_config, yaml_cache, table_refs = _seeded_pre_refresh_config()
    assert "cat.sch.mv1" in effective_metric_view_identifiers_with_catalog(pre_config)

    bare = _bare_rest_config()
    with patch.object(_genie_client_mod, "fetch_space_config", return_value=bare):
        new_config, _ = harness._refresh_config_preserving_mv_state(
            w=None,
            space_id="space-id",
            uc_columns=[],
            data_profile={},
            yaml_cache=yaml_cache,
            table_refs=table_refs,
        )

    assert "cat.sch.mv1" in effective_metric_view_identifiers_with_catalog(new_config)
    counts = _count_mv_detection_sources(new_config)
    assert counts["catalog"] >= 1


def test_refresh_preserves_asset_semantics_stamp():
    """After a refresh ``_asset_semantics`` must still classify
    ``cat.sch.mv1`` as ``kind=metric_view``."""
    from genie_space_optimizer.optimization import harness

    _, yaml_cache, table_refs = _seeded_pre_refresh_config()
    bare = _bare_rest_config()
    with patch.object(_genie_client_mod, "fetch_space_config", return_value=bare):
        new_config, metadata_snapshot = harness._refresh_config_preserving_mv_state(
            w=None,
            space_id="space-id",
            uc_columns=[],
            data_profile={},
            yaml_cache=yaml_cache,
            table_refs=table_refs,
        )

    sem = get_asset_semantics(new_config)
    assert "cat.sch.mv1" in sem
    assert sem["cat.sch.mv1"]["kind"] == KIND_METRIC_VIEW

    # The mirror on metadata_snapshot (== _parsed_space) must agree so
    # plan_asset_coverage and other consumers reading from there see
    # the same answer.
    sem_snapshot = metadata_snapshot.get("_asset_semantics") or {}
    assert "cat.sch.mv1" in sem_snapshot
    assert sem_snapshot["cat.sch.mv1"]["kind"] == KIND_METRIC_VIEW


def test_refresh_preserves_data_profile_and_uc_columns():
    """Existing behaviour: ``_uc_columns`` and ``_data_profile`` continue
    to round-trip across the refresh."""
    from genie_space_optimizer.optimization import harness

    _, yaml_cache, table_refs = _seeded_pre_refresh_config()
    uc_columns = [
        {
            "catalog_name": "cat",
            "schema_name": "sch",
            "table_name": "fact_sales",
            "column_name": "store_id",
            "data_type": "STRING",
        },
    ]
    data_profile = {"cat.sch.fact_sales": {"row_count": 42}}

    bare = _bare_rest_config()
    with patch.object(_genie_client_mod, "fetch_space_config", return_value=bare):
        new_config, metadata_snapshot = harness._refresh_config_preserving_mv_state(
            w=None,
            space_id="space-id",
            uc_columns=uc_columns,
            data_profile=data_profile,
            yaml_cache=yaml_cache,
            table_refs=table_refs,
        )

    assert new_config["_uc_columns"] == uc_columns
    assert metadata_snapshot["_data_profile"] == data_profile


def test_refresh_with_empty_yaml_cache_is_noop_for_mv_state():
    """When the pre-refresh yaml cache is empty (no MVs detected), the
    helper must not invent any MV state."""
    from genie_space_optimizer.optimization import harness

    bare = _bare_rest_config()
    with patch.object(_genie_client_mod, "fetch_space_config", return_value=bare):
        new_config, _ = harness._refresh_config_preserving_mv_state(
            w=None,
            space_id="space-id",
            uc_columns=[],
            data_profile={},
            yaml_cache={},
            table_refs=[("cat", "sch", "fact_sales")],
        )

    assert effective_metric_view_identifiers_with_catalog(new_config) == set()
    counts = _count_mv_detection_sources(new_config)
    assert counts["catalog"] == 0
