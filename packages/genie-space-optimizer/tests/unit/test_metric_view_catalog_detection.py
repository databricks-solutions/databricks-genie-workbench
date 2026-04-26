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

    def fake_detect_with_outcomes(
        spark, refs, *, w, warehouse_id, catalog, schema,
    ):
        called["args"] = (refs, catalog, schema)
        outcomes = {fq: "detected" for fq in yaml_payload}
        return set(yaml_payload), yaml_payload, outcomes

    monkeypatch.setattr(
        "genie_space_optimizer.common.metric_view_catalog."
        "detect_metric_views_via_catalog",
        fake_detect,
    )
    # PR 23 — harness now invokes the ``_with_outcomes`` variant so the
    # detection summary line can break down per-ref outcomes even when
    # zero MVs are detected. Patch both for robustness.
    monkeypatch.setattr(
        "genie_space_optimizer.common.metric_view_catalog."
        "detect_metric_views_via_catalog_with_outcomes",
        fake_detect_with_outcomes,
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


# ── PR 23: per-ref outcomes dict + always-on summary log ─────────────


class TestPR23OutcomesAndObservability:
    """PR 23 — per-ref outcomes dict + always-on summary log line."""

    def test_outcomes_describe_error(self):
        """``DESCRIBE`` raise → ``describe_error`` outcome, no detection."""
        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DESCRIBE_ERROR,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def boom(*args, **kwargs):
            raise RuntimeError("permission denied")

        detected, yamls, outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark, [("cat", "sch", "tbl")],
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=boom,
            )
        )

        assert detected == set()
        assert yamls == {}
        assert outcomes == {"cat.sch.tbl": OUTCOME_DESCRIBE_ERROR}

    def test_outcomes_empty_result(self):
        """Empty DataFrame → ``empty_result`` outcome."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_EMPTY_RESULT,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def empty(*args, **kwargs):
            return pd.DataFrame()

        _, _, outcomes = detect_metric_views_via_catalog_with_outcomes(
            spark, [("cat", "sch", "tbl")],
            w=None, warehouse_id="", catalog="cat", schema="sch",
            exec_sql=empty,
        )

        assert outcomes == {"cat.sch.tbl": OUTCOME_EMPTY_RESULT}

    def test_outcomes_no_envelope(self):
        """Row whose values aren't JSON-parseable → ``no_envelope``."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_NO_ENVELOPE,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def garbage(*args, **kwargs):
            return pd.DataFrame([{"col": "not-json", "col2": 42}])

        _, _, outcomes = detect_metric_views_via_catalog_with_outcomes(
            spark, [("cat", "sch", "tbl")],
            w=None, warehouse_id="", catalog="cat", schema="sch",
            exec_sql=garbage,
        )

        assert outcomes == {"cat.sch.tbl": OUTCOME_NO_ENVELOPE}

    def test_outcomes_no_view_text(self):
        """Envelope with no ``view_text`` (regular table) → ``no_view_text``."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_NO_VIEW_TEXT,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def regular_table(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "table_type": "MANAGED",
                "type": "TABLE",
                "columns": [{"name": "id", "type": "string"}],
            })}])

        _, _, outcomes = detect_metric_views_via_catalog_with_outcomes(
            spark, [("cat", "sch", "tbl")],
            w=None, warehouse_id="", catalog="cat", schema="sch",
            exec_sql=regular_table,
        )

        assert outcomes == {"cat.sch.tbl": OUTCOME_NO_VIEW_TEXT}

    def test_outcomes_yaml_parse_error(self):
        """``view_text`` that isn't valid YAML → ``yaml_parse_error``."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_YAML_PARSE_ERROR,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def bad_yaml(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "view_text": "::: not valid yaml :::\n  - and: [unclosed",
            })}])

        _, _, outcomes = detect_metric_views_via_catalog_with_outcomes(
            spark, [("cat", "sch", "tbl")],
            w=None, warehouse_id="", catalog="cat", schema="sch",
            exec_sql=bad_yaml,
        )

        assert outcomes == {"cat.sch.tbl": OUTCOME_YAML_PARSE_ERROR}

    def test_outcomes_not_mv_shape(self):
        """Regular-view DDL parses but lacks MV shape → ``not_mv_shape``."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_NOT_MV_SHAPE,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def regular_view(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "view_text": REGULAR_VIEW_DDL,
            })}])

        _, _, outcomes = detect_metric_views_via_catalog_with_outcomes(
            spark, [("cat", "sch", "tbl")],
            w=None, warehouse_id="", catalog="cat", schema="sch",
            exec_sql=regular_view,
        )

        assert outcomes == {"cat.sch.tbl": OUTCOME_NOT_MV_SHAPE}

    def test_outcomes_detected(self):
        """Valid metric-view YAML → ``detected`` outcome."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DETECTED,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def mv(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "view_text": METRIC_VIEW_YAML,
            })}])

        detected, yamls, outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark, [("cat", "sch", "mv1")],
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=mv,
            )
        )

        assert detected == {"cat.sch.mv1"}
        assert "cat.sch.mv1" in yamls
        assert outcomes == {"cat.sch.mv1": OUTCOME_DETECTED}

    def test_summarize_outcomes_returns_stable_columns(self):
        """``summarize_outcomes`` always returns every known column."""
        from genie_space_optimizer.common.metric_view_catalog import (
            summarize_outcomes,
        )

        counts = summarize_outcomes({})
        # PR 24 — also surfaces per-signal detection breakdowns; the
        # umbrella ``detected`` count rolls them up so legacy callers
        # see no shape change.
        assert {
            "detected",
            "describe_error",
            "empty_result",
            "no_envelope",
            "no_view_text",
            "yaml_parse_error",
            "not_mv_shape",
        }.issubset(counts.keys())
        assert all(v == 0 for v in counts.values())

    def test_summarize_outcomes_counts_correctly(self):
        """Counts are consistent with the input outcomes dict."""
        from genie_space_optimizer.common.metric_view_catalog import (
            summarize_outcomes,
        )

        outcomes = {
            "cat.sch.a": "detected",
            "cat.sch.b": "describe_error",
            "cat.sch.c": "describe_error",
            "cat.sch.d": "no_view_text",
        }
        counts = summarize_outcomes(outcomes)
        assert counts["detected"] == 1
        assert counts["describe_error"] == 2
        assert counts["no_view_text"] == 1
        assert counts["empty_result"] == 0

    def test_describe_error_logged_at_info(self, caplog):
        """``describe_error`` outcome is announced at INFO with exception info."""
        import logging

        from genie_space_optimizer.common.metric_view_catalog import (
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def boom(*args, **kwargs):
            raise PermissionError("user lacks SELECT")

        with caplog.at_level(
            logging.INFO,
            logger="genie_space_optimizer.common.metric_view_catalog",
        ):
            detect_metric_views_via_catalog_with_outcomes(
                spark, [("cat", "sch", "missing")],
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=boom,
            )

        msgs = [r.getMessage() for r in caplog.records]
        assert any("DESCRIBE failed for cat.sch.missing" in m for m in msgs)
        assert any("PermissionError" in m for m in msgs)

    def test_yaml_parse_error_logged_at_info_with_snippet(self, caplog):
        """``yaml_parse_error`` outcome includes view_text snippet at INFO."""
        import logging

        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def bad_yaml(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "view_text": "::: invalid yaml ::: [unclosed",
            })}])

        with caplog.at_level(
            logging.INFO,
            logger="genie_space_optimizer.common.metric_view_catalog",
        ):
            detect_metric_views_via_catalog_with_outcomes(
                spark, [("cat", "sch", "broken")],
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=bad_yaml,
            )

        msgs = [r.getMessage() for r in caplog.records]
        assert any("YAML parse failed for cat.sch.broken" in m for m in msgs)
        assert any("invalid yaml" in m for m in msgs)


class TestPR24MultiSignalClassification:
    """PR 24 — multi-signal MV classification with synthetic skeleton fallback."""

    def test_detected_via_type_with_no_view_text(self):
        """``type=METRIC_VIEW`` envelope without ``view_text`` is still detected.

        Synthetic YAML skeleton is populated from ``columns[].is_measure``
        flags so downstream consumers can find dimensions and measures.
        """
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DETECTED_VIA_TYPE,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def envelope(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "type": "METRIC_VIEW",
                "language": "YAML",
                # view_text intentionally missing.
                "columns": [
                    {"name": "store_id", "is_measure": False},
                    {"name": "region", "is_measure": False},
                    {"name": "total_revenue", "is_measure": True},
                ],
            })}])

        detected, yamls, outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark, [("cat", "sch", "mv")],
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=envelope,
            )
        )

        assert detected == {"cat.sch.mv"}
        assert outcomes == {"cat.sch.mv": OUTCOME_DETECTED_VIA_TYPE}
        skeleton = yamls["cat.sch.mv"]
        assert skeleton["_source"] == "structural_signal"
        measure_names = {m["name"] for m in skeleton["measures"]}
        dim_names = {d["name"] for d in skeleton["dimensions"]}
        assert measure_names == {"total_revenue"}
        assert dim_names == {"store_id", "region"}

    def test_detected_via_yaml_takes_precedence_when_real_yaml_present(self):
        """Valid YAML body keeps the per-signal code as ``detected_via_yaml``
        (or ``detected_via_type`` when both signals fire — type wins per
        the documented confidence ordering)."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DETECTED_VIA_TYPE,
            OUTCOME_DETECTED_VIA_YAML,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        # Envelope with YAML present but no top-level ``type`` field —
        # classification should be ``detected_via_yaml`` and the cached
        # YAML should be the real parsed one (no synthetic skeleton).
        def yaml_only(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "view_text": METRIC_VIEW_YAML,
            })}])

        _, yamls, outcomes = detect_metric_views_via_catalog_with_outcomes(
            spark, [("cat", "sch", "mv")],
            w=None, warehouse_id="", catalog="cat", schema="sch",
            exec_sql=yaml_only,
        )
        assert outcomes == {"cat.sch.mv": OUTCOME_DETECTED_VIA_YAML}
        assert yamls["cat.sch.mv"].get("source") == "cat.sch.fact_sales"
        assert "_source" not in yamls["cat.sch.mv"]

        # Envelope with both ``type=METRIC_VIEW`` and YAML — type wins.
        def both(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "type": "METRIC_VIEW",
                "view_text": METRIC_VIEW_YAML,
            })}])

        _, yamls2, outcomes2 = detect_metric_views_via_catalog_with_outcomes(
            spark, [("cat", "sch", "mv2")],
            w=None, warehouse_id="", catalog="cat", schema="sch",
            exec_sql=both,
        )
        assert outcomes2 == {"cat.sch.mv2": OUTCOME_DETECTED_VIA_TYPE}
        # Real YAML still preferred over synthetic skeleton when valid.
        assert yamls2["cat.sch.mv2"].get("source") == "cat.sch.fact_sales"

    def test_detected_via_is_measure_only(self):
        """Envelope with only ``is_measure`` flags → detected via that signal."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DETECTED_VIA_IS_MEASURE,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def is_measure_only(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "table_type": "VIEW",
                "columns": [
                    {"name": "id", "is_measure": False},
                    {"name": "total", "is_measure": True},
                ],
            })}])

        detected, yamls, outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark, [("cat", "sch", "mv_inferred")],
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=is_measure_only,
            )
        )

        assert detected == {"cat.sch.mv_inferred"}
        assert outcomes == {
            "cat.sch.mv_inferred": OUTCOME_DETECTED_VIA_IS_MEASURE,
        }
        skeleton = yamls["cat.sch.mv_inferred"]
        assert skeleton["_source"] == "structural_signal"
        assert {m["name"] for m in skeleton["measures"]} == {"total"}
        assert {d["name"] for d in skeleton["dimensions"]} == {"id"}

    def test_yaml_parse_error_with_no_other_signal_does_not_detect(self):
        """``view_text`` present but YAML parse fails AND no structural
        signals → ``yaml_parse_error`` outcome, not detected."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_YAML_PARSE_ERROR,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def bad_yaml_no_signal(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "view_text": "::: invalid yaml ::: [unclosed",
                "columns": [{"name": "id", "is_measure": False}],
            })}])

        detected, yamls, outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark, [("cat", "sch", "broken")],
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=bad_yaml_no_signal,
            )
        )

        assert detected == set()
        assert yamls == {}
        assert outcomes == {"cat.sch.broken": OUTCOME_YAML_PARSE_ERROR}

    def test_yaml_parse_error_with_type_signal_still_detected(self):
        """``type=METRIC_VIEW`` rescues a ref whose YAML can't be parsed."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DETECTED_VIA_TYPE,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def bad_yaml_with_signal(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "type": "METRIC_VIEW",
                "view_text": "::: invalid yaml ::: [unclosed",
                "columns": [
                    {"name": "id", "is_measure": False},
                    {"name": "amt", "is_measure": True},
                ],
            })}])

        detected, yamls, outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark, [("cat", "sch", "rescued")],
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=bad_yaml_with_signal,
            )
        )

        assert detected == {"cat.sch.rescued"}
        assert outcomes == {"cat.sch.rescued": OUTCOME_DETECTED_VIA_TYPE}
        skel = yamls["cat.sch.rescued"]
        assert skel["_source"] == "structural_signal"
        assert {m["name"] for m in skel["measures"]} == {"amt"}

    def test_regression_regular_view_not_detected(self):
        """Regression: regular VIEW DDL with no ``is_measure`` cols stays non-MV."""
        import pandas as pd

        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_NOT_MV_SHAPE,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()

        def regular_view(*args, **kwargs):
            return pd.DataFrame([{"json_metadata": json.dumps({
                "view_text": REGULAR_VIEW_DDL,
                "columns": [{"name": "id", "is_measure": False}],
            })}])

        detected, yamls, outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark, [("cat", "sch", "rv")],
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=regular_view,
            )
        )
        assert detected == set()
        assert yamls == {}
        assert outcomes == {"cat.sch.rv": OUTCOME_NOT_MV_SHAPE}

    def test_summarize_counts_per_signal_breakdown(self):
        """``summarize_outcomes`` exposes per-signal counts and umbrella sum."""
        from genie_space_optimizer.common.metric_view_catalog import (
            summarize_outcomes,
        )

        outcomes = {
            "a": "detected_via_type",
            "b": "detected_via_yaml",
            "c": "detected_via_yaml",
            "d": "detected_via_is_measure",
            "e": "no_view_text",
        }
        counts = summarize_outcomes(outcomes)
        assert counts["detected_via_type"] == 1
        assert counts["detected_via_yaml"] == 2
        assert counts["detected_via_is_measure"] == 1
        assert counts["detected"] == 4  # umbrella sum
        assert counts["no_view_text"] == 1


class TestPR23HarnessSummaryLine:
    """The harness call site emits a single INFO summary line per run."""

    def test_summary_line_emitted_when_zero_detected(
        self, monkeypatch, caplog,
    ):
        """Even when zero MVs detected, the summary line surfaces every bucket."""
        import logging

        from genie_space_optimizer.optimization import harness as _harness

        fetched_config = {
            "_parsed_space": {
                "data_sources": {
                    "tables": [
                        {"identifier": "cat.sch.tbl1", "column_configs": []},
                        {"identifier": "cat.sch.tbl2", "column_configs": []},
                    ],
                    "metric_views": [],
                },
            },
        }

        monkeypatch.setattr(_harness, "load_run", lambda *a, **k: {})
        monkeypatch.setattr(
            "genie_space_optimizer.common.genie_client.fetch_space_config",
            lambda *a, **k: fetched_config,
        )
        monkeypatch.setattr(
            "genie_space_optimizer.common.uc_metadata."
            "extract_genie_space_table_refs",
            lambda config: [
                ("cat", "sch", "tbl1"),
                ("cat", "sch", "tbl2"),
            ],
        )
        monkeypatch.setattr(
            "genie_space_optimizer.common.uc_metadata."
            "get_columns_for_tables_rest",
            lambda w, refs: [],
        )

        # Stub catalog detector: both refs hit ``no_view_text`` (regular tables).
        def fake_detect_with_outcomes(
            spark, refs, *, w, warehouse_id, catalog, schema,
        ):
            outcomes = {
                f"{c}.{s}.{n}".lower(): "no_view_text"
                for c, s, n in refs
            }
            return set(), {}, outcomes

        monkeypatch.setattr(
            "genie_space_optimizer.common.metric_view_catalog."
            "detect_metric_views_via_catalog_with_outcomes",
            fake_detect_with_outcomes,
        )
        monkeypatch.setattr(_harness, "ENABLE_PROMPT_MATCHING_AUTO_APPLY", False)
        monkeypatch.setattr(
            "genie_space_optimizer.iq_scan.collect_rls_audit",
            lambda *a, **k: {},
        )

        with caplog.at_level(
            logging.INFO,
            logger="genie_space_optimizer.optimization.harness",
        ):
            _harness._prepare_lever_loop(
                w=MagicMock(),
                spark=MagicMock(),
                run_id="run-zero",
                space_id="space-1",
                catalog="cat",
                schema="sch",
                benchmarks=None,
            )

        msgs = [r.getMessage() for r in caplog.records]
        # Must always emit the summary line, even though detected=0.
        assert any(
            "Catalog metric-view detection summary for run-zero" in m
            and "refs=2" in m
            and "detected=0" in m
            and "no_view_text=2" in m
            for m in msgs
        ), f"summary line not found in: {msgs}"


class TestPR25NonAsJsonFallback:
    """PR 25 — non-AS-JSON Spark-path fallback for environments where
    ``DESCRIBE ... AS JSON`` is unavailable.

    Locks two contracts:

    1. When ``AS JSON`` raises a syntax / unsupported-feature error, the
       detector falls back to plain ``DESCRIBE EXTENDED`` and still
       classifies the ref correctly.
    2. When ``AS JSON`` raises a permission / network / table-not-found
       error, the detector does *not* call the fallback (no noisy second
       DESCRIBE) and records ``describe_error``.
    """

    @staticmethod
    def _legacy_describe_rows(*, view_text: str | None,
                              type_str: str | None = None,
                              language_str: str | None = None,
                              measure_cols: list[str] | None = None):
        """Build a pandas DataFrame mimicking standard
        ``DESCRIBE EXTENDED`` output (3-column: col_name, data_type,
        comment) with the column section followed by detail rows.
        """
        import pandas as pd

        rows: list[dict] = []
        # Column section
        for c in (measure_cols or ["region", "total_revenue"]):
            rows.append({
                "col_name": c,
                "data_type": "double" if "revenue" in c else "string",
                "comment": "",
            })
        # Section break
        rows.append({"col_name": "", "data_type": "", "comment": ""})
        rows.append({
            "col_name": "# Detailed Table Information",
            "data_type": "",
            "comment": "",
        })
        if type_str:
            rows.append({
                "col_name": "Type", "data_type": type_str, "comment": "",
            })
        if language_str:
            rows.append({
                "col_name": "Language",
                "data_type": language_str,
                "comment": "",
            })
        if view_text is not None:
            rows.append({
                "col_name": "View Text",
                "data_type": view_text,
                "comment": "",
            })
        return pd.DataFrame(rows)

    def test_as_json_unsupported_falls_back_and_detects(self):
        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DETECTED_VIA_TYPE,
            OUTCOME_DETECTED_VIA_YAML,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()
        refs = [("cat", "sch", "mv_sales")]

        def fake_exec_sql(sql: str, *args, **kwargs):
            sql_lower = sql.lower()
            if "as json" in sql_lower:
                raise SyntaxError(
                    "PARSE_SYNTAX_ERROR: mismatched input 'JSON' "
                    "expecting <EOF>"
                )
            # Plain DESCRIBE EXTENDED — return legacy rows including a
            # ``Type=METRIC_VIEW`` detail row so multi-signal classification
            # still classifies it as an MV via PR 24's signal cascade.
            return TestPR25NonAsJsonFallback._legacy_describe_rows(
                view_text=METRIC_VIEW_YAML,
                type_str="METRIC_VIEW",
                language_str="YAML",
            )

        detected, yamls, outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark, refs,
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=fake_exec_sql,
            )
        )

        assert "cat.sch.mv_sales" in detected
        # Type=METRIC_VIEW is the highest-priority signal, so the
        # outcome should be detected_via_type even though view_text is
        # also present.
        assert outcomes["cat.sch.mv_sales"] in (
            OUTCOME_DETECTED_VIA_TYPE, OUTCOME_DETECTED_VIA_YAML,
        )
        # YAML payload still gets parsed for downstream consumers.
        yaml_dict = yamls["cat.sch.mv_sales"]
        assert yaml_dict.get("source") == "cat.sch.fact_sales"

    def test_as_json_unsupported_fallback_without_view_text(self):
        """Fallback DESCRIBE returns Type=METRIC_VIEW but no view_text;
        PR 24's synthetic-skeleton path should still classify the ref.
        """
        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DETECTED_VIA_TYPE,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()
        refs = [("cat", "sch", "mv_no_text")]

        def fake_exec_sql(sql, *args, **kwargs):
            if "as json" in sql.lower():
                raise Exception(
                    "AnalysisException: 'AS JSON' is not supported"
                )
            return TestPR25NonAsJsonFallback._legacy_describe_rows(
                view_text=None,
                type_str="METRIC_VIEW",
                language_str=None,
            )

        detected, yamls, outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark, refs,
                w=None, warehouse_id="", catalog="cat", schema="sch",
                exec_sql=fake_exec_sql,
            )
        )

        assert "cat.sch.mv_no_text" in detected
        assert outcomes["cat.sch.mv_no_text"] == OUTCOME_DETECTED_VIA_TYPE
        # Synthetic skeleton populated from columns.
        yaml_dict = yamls["cat.sch.mv_no_text"]
        assert yaml_dict.get("_source") == "structural_signal"

    def test_permission_error_does_not_call_fallback(self):
        """A non-syntax exception should record describe_error and NOT
        invoke a second DESCRIBE."""
        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DESCRIBE_ERROR,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()
        refs = [("cat", "sch", "blocked_table")]
        call_count = {"n": 0}

        def fake_exec_sql(sql, *args, **kwargs):
            call_count["n"] += 1
            raise PermissionError(
                "User does not have SELECT on cat.sch.blocked_table"
            )

        _, _, outcomes = detect_metric_views_via_catalog_with_outcomes(
            spark, refs,
            w=None, warehouse_id="", catalog="cat", schema="sch",
            exec_sql=fake_exec_sql,
        )

        assert outcomes["cat.sch.blocked_table"] == OUTCOME_DESCRIBE_ERROR
        # PermissionError is not unsupported-syntax-shaped; fallback must
        # not be attempted.
        assert call_count["n"] == 1

    def test_fallback_describe_also_failing_records_describe_error(self):
        """AS JSON raises unsupported, fallback DESCRIBE also raises ->
        record describe_error and don't crash."""
        from genie_space_optimizer.common.metric_view_catalog import (
            OUTCOME_DESCRIBE_ERROR,
            detect_metric_views_via_catalog_with_outcomes,
        )

        spark = MagicMock()
        refs = [("cat", "sch", "weird")]

        def fake_exec_sql(sql, *args, **kwargs):
            if "as json" in sql.lower():
                raise Exception("PARSE_SYNTAX_ERROR")
            raise Exception("ANALYSIS_ERROR: table not found")

        _, _, outcomes = detect_metric_views_via_catalog_with_outcomes(
            spark, refs,
            w=None, warehouse_id="", catalog="cat", schema="sch",
            exec_sql=fake_exec_sql,
        )

        assert outcomes["cat.sch.weird"] == OUTCOME_DESCRIBE_ERROR

    def test_is_as_json_unsupported_error_heuristic(self):
        from genie_space_optimizer.common.metric_view_catalog import (
            _is_as_json_unsupported_error,
        )

        assert _is_as_json_unsupported_error(
            Exception("PARSE_SYNTAX_ERROR mismatched input 'JSON'")
        )
        assert _is_as_json_unsupported_error(
            Exception("AS JSON is not supported in this context")
        )
        assert _is_as_json_unsupported_error(
            SyntaxError("syntax error near AS")
        )
        assert not _is_as_json_unsupported_error(
            PermissionError("user lacks SELECT privilege")
        )
        assert not _is_as_json_unsupported_error(
            Exception("Connection refused")
        )

    def test_describe_metric_view_fallback_parses_envelope(self):
        from genie_space_optimizer.common.metric_view_catalog import (
            _describe_metric_view_fallback,
        )

        spark = MagicMock()
        rows = TestPR25NonAsJsonFallback._legacy_describe_rows(
            view_text=METRIC_VIEW_YAML,
            type_str="METRIC_VIEW",
            language_str="YAML",
            measure_cols=["region", "total_revenue", "avg_revenue"],
        )

        envelope = _describe_metric_view_fallback(
            "`cat`.`sch`.`mv`", "cat.sch.mv",
            spark=spark, w=None, warehouse_id="",
            catalog="cat", schema="sch",
            exec_sql=lambda *a, **kw: rows,
        )

        assert envelope is not None
        assert envelope["type"] == "METRIC_VIEW"
        assert envelope["language"] == "YAML"
        assert envelope["view_text"].startswith("version:")
        cols = envelope["columns"]
        assert {c["name"] for c in cols} == {
            "region", "total_revenue", "avg_revenue",
        }

    def test_describe_metric_view_fallback_returns_none_on_exec_error(self):
        from genie_space_optimizer.common.metric_view_catalog import (
            _describe_metric_view_fallback,
        )

        def fail(*args, **kwargs):
            raise Exception("transient error")

        envelope = _describe_metric_view_fallback(
            "`cat`.`sch`.`mv`", "cat.sch.mv",
            spark=MagicMock(), w=None, warehouse_id="",
            catalog="cat", schema="sch", exec_sql=fail,
        )
        assert envelope is None
