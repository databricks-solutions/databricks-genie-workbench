"""Unit tests for catalog-level metric-view detection.

Locks the contract that ``_detect_metric_views_via_catalog`` correctly
classifies entities based on their UC ``DESCRIBE TABLE EXTENDED ... AS
JSON`` payload, and that
``effective_metric_view_identifiers_with_catalog`` unions the catalog
detection with the existing column-config heuristic.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest


METRIC_VIEW_YAML = """\
version: 0.1
source: cat.sch.fact_sales
dimensions:
  - name: store_id
    expr: store_id
  - name: sale_date
    expr: sale_date
measures:
  - name: total_revenue
    expr: SUM(revenue)
  - name: avg_revenue
    expr: AVG(revenue)
"""


REGULAR_VIEW_DDL = (
    "CREATE VIEW cat.sch.regular_view AS\n"
    "SELECT * FROM cat.sch.fact_sales WHERE region = 'US'\n"
)


def _make_describe_response(view_text: str) -> str:
    """Mimic the JSON shape Spark emits for DESCRIBE TABLE EXTENDED AS JSON."""
    return json.dumps({"view_text": view_text, "table_type": "VIEW"})


def test_detect_metric_view_yaml_payload():
    """A ref whose DESCRIBE returns metric-view YAML is classified as MV."""
    from genie_space_optimizer.optimization.preflight import (
        _detect_metric_views_via_catalog,
    )

    spark = MagicMock()
    refs = [("cat", "sch", "mv_sales")]

    def fake_exec_sql(sql, *args, **kwargs):
        import pandas as pd

        return pd.DataFrame(
            [{"json_metadata": _make_describe_response(METRIC_VIEW_YAML)}]
        )

    detected, yamls = _detect_metric_views_via_catalog(
        spark,
        refs,
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
        exec_sql=fake_exec_sql,
    )

    assert "cat.sch.mv_sales" in detected
    assert "cat.sch.mv_sales" in yamls
    yaml_dict = yamls["cat.sch.mv_sales"]
    assert yaml_dict.get("source") == "cat.sch.fact_sales"
    assert {d["name"] for d in yaml_dict.get("dimensions", [])} == {
        "store_id",
        "sale_date",
    }
    assert {m["name"] for m in yaml_dict.get("measures", [])} == {
        "total_revenue",
        "avg_revenue",
    }


def test_detect_regular_view_not_classified():
    """A ref whose DESCRIBE returns regular view DDL is NOT classified."""
    from genie_space_optimizer.optimization.preflight import (
        _detect_metric_views_via_catalog,
    )

    spark = MagicMock()
    refs = [("cat", "sch", "regular_view")]

    def fake_exec_sql(sql, *args, **kwargs):
        import pandas as pd

        return pd.DataFrame(
            [{"json_metadata": _make_describe_response(REGULAR_VIEW_DDL)}]
        )

    detected, yamls = _detect_metric_views_via_catalog(
        spark,
        refs,
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
        exec_sql=fake_exec_sql,
    )

    assert detected == set()
    assert yamls == {}


def test_detect_failure_does_not_propagate():
    """When DESCRIBE raises, ref is treated as non-MV; no exception escapes."""
    from genie_space_optimizer.optimization.preflight import (
        _detect_metric_views_via_catalog,
    )

    spark = MagicMock()
    refs = [("cat", "sch", "missing_table")]

    def fake_exec_sql(sql, *args, **kwargs):
        raise RuntimeError("table not found")

    detected, yamls = _detect_metric_views_via_catalog(
        spark,
        refs,
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
        exec_sql=fake_exec_sql,
    )

    assert detected == set()
    assert yamls == {}


def test_detect_handles_multiple_refs_mixed_outcomes():
    """One MV + one regular view + one failure → exactly one MV classified."""
    from genie_space_optimizer.optimization.preflight import (
        _detect_metric_views_via_catalog,
    )

    spark = MagicMock()
    refs = [
        ("cat", "sch", "mv_sales"),
        ("cat", "sch", "regular_view"),
        ("cat", "sch", "broken_table"),
    ]

    def fake_exec_sql(sql, *args, **kwargs):
        import pandas as pd

        if "mv_sales" in sql:
            return pd.DataFrame(
                [{"json_metadata": _make_describe_response(METRIC_VIEW_YAML)}]
            )
        if "regular_view" in sql:
            return pd.DataFrame(
                [{"json_metadata": _make_describe_response(REGULAR_VIEW_DDL)}]
            )
        raise RuntimeError("not found")

    detected, yamls = _detect_metric_views_via_catalog(
        spark,
        refs,
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
        exec_sql=fake_exec_sql,
    )

    assert detected == {"cat.sch.mv_sales"}
    assert set(yamls) == {"cat.sch.mv_sales"}


def test_effective_with_catalog_unions_sources():
    """Catalog-detected MV unions with column_config heuristic."""
    from genie_space_optimizer.optimization.evaluation import (
        effective_metric_view_identifiers_with_catalog,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                # column_config heuristic catches this one.
                "tables": [
                    {
                        "identifier": "cat.sch.mv_via_columns",
                        "column_configs": [
                            {"column_name": "amount", "column_type": "measure"},
                        ],
                    },
                ],
                "metric_views": [
                    {"identifier": "cat.sch.mv_explicit"},
                ],
            },
        },
        # Catalog detector adds this one (no measure column configs).
        "_metric_view_yaml": {
            "cat.sch.mv_via_catalog": {"source": "cat.sch.fact"},
            # Already known by column-config path; should still appear once.
            "cat.sch.mv_via_columns": {"source": "cat.sch.fact2"},
        },
    }

    result = effective_metric_view_identifiers_with_catalog(config)

    # Identifiers come back lower-cased? Reuse evaluation's existing
    # casing rules — the function returns identifiers as-stored.
    lowered = {ident.lower() for ident in result}
    assert "cat.sch.mv_via_columns" in lowered
    assert "cat.sch.mv_explicit" in lowered
    assert "cat.sch.mv_via_catalog" in lowered


def test_effective_with_catalog_falls_back_when_yaml_cache_absent():
    """No catalog cache on config → behaviour matches column-config path."""
    from genie_space_optimizer.optimization.evaluation import (
        effective_metric_view_identifiers,
        effective_metric_view_identifiers_with_catalog,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                "metric_views": [{"identifier": "cat.sch.mv_only"}],
                "tables": [],
            },
        },
    }

    base = effective_metric_view_identifiers(config)
    extended = effective_metric_view_identifiers_with_catalog(config)
    assert base == extended


# ── PR 19: cache-aware measures map + traits + harness wiring ────────


def test_build_metric_view_measures_unions_yaml_cache():
    """``_metric_view_yaml`` alone (no measure column_configs) yields measures."""
    from genie_space_optimizer.optimization.evaluation import (
        build_metric_view_measures,
    )

    config = {
        # Genie reported the MV as a regular table with no measure flags.
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.mv_esr_store_sales",
                        "column_configs": [
                            {"column_name": "store_id", "column_type": "dimension"},
                            {"column_name": "total_sales_usd_day"},
                        ],
                    },
                ],
                "metric_views": [],
            },
        },
        # Catalog DESCRIBE recovered the real MV definition.
        "_metric_view_yaml": {
            "cat.sch.mv_esr_store_sales": {
                "source": "cat.sch.fact_sales",
                "dimensions": [{"name": "store_id", "expr": "store_id"}],
                "measures": [
                    {"name": "total_sales_usd_day", "expr": "SUM(total_sales)"},
                    {"name": "store_day_count_day", "expr": "COUNT(*)"},
                ],
            },
        },
    }

    measures = build_metric_view_measures(config)
    assert "mv_esr_store_sales" in measures
    assert measures["mv_esr_store_sales"] == {
        "total_sales_usd_day",
        "store_day_count_day",
    }


def test_build_metric_view_measures_unions_columns_and_yaml():
    """Both column_configs *and* YAML cache contribute to the same MV."""
    from genie_space_optimizer.optimization.evaluation import (
        build_metric_view_measures,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.mv_sales",
                        "column_configs": [
                            {
                                "column_name": "revenue",
                                "column_type": "measure",
                            },
                        ],
                    },
                ],
            },
        },
        "_metric_view_yaml": {
            "cat.sch.mv_sales": {
                "source": "cat.sch.fact",
                "measures": [
                    {"name": "qty", "expr": "SUM(qty)"},
                ],
            },
        },
    }

    measures = build_metric_view_measures(config)
    assert measures.get("mv_sales") == {"revenue", "qty"}


def test_build_metric_view_measures_reads_yaml_cache_from_parsed_space():
    """Cache stamped under ``_parsed_space`` is also picked up."""
    from genie_space_optimizer.optimization.evaluation import (
        build_metric_view_measures,
    )

    config = {
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.mv_only",
                        "column_configs": [],
                    },
                ],
            },
            "_metric_view_yaml": {
                "cat.sch.mv_only": {
                    "source": "cat.sch.src",
                    "measures": [{"name": "amount", "expr": "SUM(x)"}],
                },
            },
        },
    }

    measures = build_metric_view_measures(config)
    assert measures.get("mv_only") == {"amount"}


def test_schema_traits_yaml_cache_only():
    """No metric_views, no measure flags, but populated cache → has_metric_view."""
    from genie_space_optimizer.optimization.archetypes import schema_traits

    metadata_snapshot = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.mv_via_catalog",
                    "column_configs": [
                        {"column_name": "id", "data_type": "string"},
                    ],
                },
            ],
            "metric_views": [],
        },
        "_metric_view_yaml": {
            "cat.sch.mv_via_catalog": {
                "source": "cat.sch.src",
                "measures": [{"name": "x", "expr": "SUM(x)"}],
            },
        },
    }

    traits = schema_traits(metadata_snapshot)
    assert "has_metric_view" in traits


def test_schema_traits_no_cache_and_no_flags_no_mv_trait():
    """Sanity: empty cache must not synthesize the trait."""
    from genie_space_optimizer.optimization.archetypes import schema_traits

    metadata_snapshot = {
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.regular",
                    "column_configs": [
                        {"column_name": "id", "data_type": "string"},
                    ],
                },
            ],
            "metric_views": [],
        },
        "_metric_view_yaml": {},
    }

    traits = schema_traits(metadata_snapshot)
    assert "has_metric_view" not in traits


def test_run_enrichment_invokes_catalog_detection(monkeypatch):
    """``_prepare_lever_loop`` runs ``detect_metric_views_via_catalog`` and
    stamps ``_metric_view_yaml`` on the loaded config."""
    from genie_space_optimizer.optimization import harness as _harness

    yaml_payload = {
        "cat.sch.mv_x": {
            "source": "cat.sch.src",
            "measures": [{"name": "amount", "expr": "SUM(amount)"}],
        },
    }

    fetched_config = {
        "_parsed_space": {
            "data_sources": {
                "tables": [
                    {"identifier": "cat.sch.mv_x", "column_configs": []},
                ],
                "metric_views": [],
            },
        },
    }

    # Stub out everything _prepare_lever_loop touches around the new
    # block. ``fetch_space_config``, ``extract_genie_space_table_refs``,
    # and ``get_columns_for_tables_rest`` are imported lazily inside the
    # function — patch their source modules.
    monkeypatch.setattr(_harness, "load_run", lambda *a, **k: {})
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.fetch_space_config",
        lambda *a, **k: fetched_config,
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.extract_genie_space_table_refs",
        lambda config: [("cat", "sch", "mv_x")],
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.uc_metadata.get_columns_for_tables_rest",
        lambda w, refs: [],
    )

    # Stub the catalog detector itself.
    called = {"args": None}

    def fake_detect(spark, refs, *, w, warehouse_id, catalog, schema):
        called["args"] = (refs, catalog, schema)
        return set(yaml_payload), yaml_payload

    monkeypatch.setattr(
        "genie_space_optimizer.common.metric_view_catalog."
        "detect_metric_views_via_catalog",
        fake_detect,
    )

    # Disable downstream-of-detection branches that would require Spark
    # / Workspace clients we don't have in the unit test.
    monkeypatch.setattr(_harness, "ENABLE_PROMPT_MATCHING_AUTO_APPLY", False)
    monkeypatch.setattr(
        "genie_space_optimizer.iq_scan.collect_rls_audit",
        lambda *a, **k: {},
    )

    config = _harness._prepare_lever_loop(
        w=MagicMock(),
        spark=MagicMock(),
        run_id="run-1",
        space_id="space-1",
        catalog="cat",
        schema="sch",
        benchmarks=None,
    )

    assert called["args"] is not None
    assert config.get("_metric_view_yaml") == yaml_payload
    assert config["_parsed_space"]["_metric_view_yaml"] == yaml_payload
