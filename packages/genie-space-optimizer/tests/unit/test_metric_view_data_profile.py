"""Metric-view-aware data profiling.

Replaces the silent skip path with an MV-legal profile that issues
per-dimension ``GROUP BY`` queries (never ``SELECT *``) and records the
YAML measure expressions verbatim so the synthesis prompt builder can
describe what each measure does.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd


_MV_FQN = "cat.sch.mv_orders"

_MV_YAML = {
    "source": "cat.sch.orders",
    "dimensions": [
        {"name": "region"},
        {"name": "channel"},
        {"name": "order_date"},
    ],
    "measures": [
        {"name": "total_revenue", "expr": "SUM(amount)"},
        {"name": "distinct_customers", "expr": "COUNT(DISTINCT customer_id)"},
    ],
}


def _uc_columns_for_mv(mv_fqn: str) -> list[dict]:
    """Synthesize UC column rows for the MV — used as a fallback when
    the YAML is empty."""
    parts = mv_fqn.split(".")
    return [
        {
            "catalog_name": parts[0],
            "schema_name": parts[1],
            "table_name": parts[-1],
            "column_name": "region",
            "data_type": "string",
            "column_type": "dimension",
        },
        {
            "catalog_name": parts[0],
            "schema_name": parts[1],
            "table_name": parts[-1],
            "column_name": "channel",
            "data_type": "string",
            "column_type": "dimension",
        },
        {
            "catalog_name": parts[0],
            "schema_name": parts[1],
            "table_name": parts[-1],
            "column_name": "total_revenue",
            "data_type": "double",
            "column_type": "measure",
        },
    ]


def test_profile_dispatches_to_mv_path_for_dimensions():
    """When an entity is in the effective MV set, ``_collect_data_profile``
    routes to ``_profile_metric_view`` and never issues a ``SELECT *``."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    sql_log: list[str] = []

    def fake_exec_sql(sql, *args, **kwargs):
        sql_log.append(sql)
        upper = sql.upper()
        if upper.startswith("SELECT COUNT(*)"):
            return pd.DataFrame([{"cnt": 1234}])
        if "_CARD_" in upper:
            # Dimension query: count distinct + min + max.
            return pd.DataFrame([
                {"_card_region": 4, "_min_region": "EMEA", "_max_region": "NA"},
            ])
        if "COLLECT_SET" in upper:
            return pd.DataFrame([{"vals": '["EMEA","NA","APAC","LATAM"]'}])
        raise AssertionError(f"Unexpected SQL: {sql[:120]!r}")

    profile = {
        "_metric_view_yaml": {_MV_FQN.lower(): _MV_YAML},
    }
    with patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql):
        result = preflight._profile_metric_view(
            spark=MagicMock(),
            mv_fqn=_MV_FQN,
            mv_yaml=_MV_YAML,
            uc_columns=_uc_columns_for_mv(_MV_FQN),
            sample_size=100,
            low_cardinality_threshold=200,
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    assert result is not None
    # No ``SELECT *`` was ever issued — every query reads explicit columns
    # through a ``GROUP BY`` envelope.
    assert all("SELECT *" not in s.upper() for s in sql_log), (
        f"SELECT * leaked into MV profile path; queries: {sql_log!r}"
    )

    # Three dimensions → at least three dimension queries fire (one per
    # dimension; possibly more for low-cardinality COLLECT_SET).
    dim_queries = [s for s in sql_log if "_card_" in s]
    assert len(dim_queries) >= 3, (
        f"Expected one dimension query per YAML dim; got {dim_queries!r}"
    )

    # Result shape: row_count + per-dimension column entries + measures map.
    assert result.get("row_count") == 1234
    assert "columns" in result
    assert "measures" in result


def test_dimensions_resolved_from_yaml_when_present():
    """YAML dimensions take precedence over UC column rows."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    queried_dims: list[str] = []

    def fake_exec_sql(sql, *args, **kwargs):
        upper = sql.upper()
        if upper.startswith("SELECT COUNT(*)"):
            return pd.DataFrame([{"cnt": 10}])
        if "_CARD_" in upper:
            # Capture which dimension was queried by extracting the alias.
            for tok in sql.split():
                if "_card_" in tok:
                    queried_dims.append(
                        tok.replace("`", "").replace("_card_", "").strip(",")
                    )
                    break
            return pd.DataFrame([
                {f"_card_{queried_dims[-1]}": 3,
                 f"_min_{queried_dims[-1]}": "a",
                 f"_max_{queried_dims[-1]}": "z"},
            ])
        if "COLLECT_SET" in upper:
            return pd.DataFrame([{"vals": '["a","b","c"]'}])
        raise AssertionError(f"Unexpected SQL: {sql[:120]!r}")

    with patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql):
        result = preflight._profile_metric_view(
            spark=MagicMock(),
            mv_fqn=_MV_FQN,
            mv_yaml=_MV_YAML,
            uc_columns=[],
            sample_size=100,
            low_cardinality_threshold=200,
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    assert result is not None
    assert set(queried_dims) == {"region", "channel", "order_date"}


def test_dimensions_fallback_to_uc_columns_without_yaml():
    """When no YAML is available, dimensions come from UC columns whose
    ``column_type`` is not ``measure``."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    queried_cols: list[str] = []

    def fake_exec_sql(sql, *args, **kwargs):
        upper = sql.upper()
        if upper.startswith("SELECT COUNT(*)"):
            return pd.DataFrame([{"cnt": 5}])
        if "_CARD_" in upper:
            for tok in sql.split():
                if "_card_" in tok:
                    queried_cols.append(
                        tok.replace("`", "").replace("_card_", "").strip(",")
                    )
                    break
            return pd.DataFrame([
                {f"_card_{queried_cols[-1]}": 2,
                 f"_min_{queried_cols[-1]}": None,
                 f"_max_{queried_cols[-1]}": None},
            ])
        if "COLLECT_SET" in upper:
            return pd.DataFrame([{"vals": '["x","y"]'}])
        raise AssertionError(f"Unexpected SQL: {sql[:120]!r}")

    with patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql):
        result = preflight._profile_metric_view(
            spark=MagicMock(),
            mv_fqn=_MV_FQN,
            mv_yaml=None,
            uc_columns=_uc_columns_for_mv(_MV_FQN),
            sample_size=100,
            low_cardinality_threshold=200,
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    assert result is not None
    # Only dimension columns are profiled; the measure column is excluded.
    assert "total_revenue" not in queried_cols
    assert {"region", "channel"}.issubset(set(queried_cols))


def test_measures_recorded_from_yaml_with_expressions():
    """Measures appear in the result dict with their YAML expressions."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    def fake_exec_sql(sql, *args, **kwargs):
        upper = sql.upper()
        if upper.startswith("SELECT COUNT(*)"):
            return pd.DataFrame([{"cnt": 10}])
        if "_CARD_" in upper:
            return pd.DataFrame([{"_card_x": 1, "_min_x": None, "_max_x": None}])
        if "COLLECT_SET" in upper:
            return pd.DataFrame([{"vals": '["x"]'}])
        raise AssertionError(f"Unexpected SQL: {sql[:120]!r}")

    with patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql):
        result = preflight._profile_metric_view(
            spark=MagicMock(),
            mv_fqn=_MV_FQN,
            mv_yaml=_MV_YAML,
            uc_columns=[],
            sample_size=100,
            low_cardinality_threshold=200,
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    assert result is not None
    measures = result.get("measures") or {}
    assert "total_revenue" in measures
    assert "distinct_customers" in measures
    # The YAML ``expr`` is preserved for the synthesis prompt builder.
    assert "SUM(amount)" in str(measures["total_revenue"])


def test_returns_none_when_no_dimensions_resolvable():
    """No YAML + no UC dimension columns → return ``None`` (caller skips)."""
    from genie_space_optimizer.optimization import preflight

    result = preflight._profile_metric_view(
        spark=MagicMock(),
        mv_fqn=_MV_FQN,
        mv_yaml=None,
        uc_columns=[],
        sample_size=100,
        low_cardinality_threshold=200,
        w=None,
        warehouse_id="",
        catalog="cat",
        schema="sch",
    )
    assert result is None


def test_row_count_falls_back_to_minus_one_on_mv_error():
    """If ``SELECT count(*)`` is rejected with an MV error, row_count = -1."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    def fake_exec_sql(sql, *args, **kwargs):
        upper = sql.upper()
        if upper.startswith("SELECT COUNT(*)"):
            raise RuntimeError("[METRIC_VIEW_UNSUPPORTED_USAGE] count blocked")
        if "_CARD_" in upper:
            return pd.DataFrame([{"_card_region": 4, "_min_region": "A", "_max_region": "Z"}])
        if "COLLECT_SET" in upper:
            return pd.DataFrame([{"vals": '["A","B"]'}])
        raise AssertionError(f"Unexpected SQL: {sql[:120]!r}")

    with patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql):
        result = preflight._profile_metric_view(
            spark=MagicMock(),
            mv_fqn=_MV_FQN,
            mv_yaml=_MV_YAML,
            uc_columns=[],
            sample_size=100,
            low_cardinality_threshold=200,
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    assert result is not None
    assert result.get("row_count") == -1


def test_data_profiling_stage_detail_includes_mv_metrics():
    """The DATA_PROFILING stage detail surfaces three new keys so MLflow
    + the persisted run snapshot can show MV behaviour without humans
    grepping stdout: ``metric_views_detected_via_catalog``,
    ``metric_views_reclassified_at_runtime``, and
    ``metric_view_profile_outcomes`` (one entry per effective MV with
    its outcome and dimensions_profiled count)."""
    from genie_space_optimizer.optimization import preflight

    config: dict = {
        "_tables": ["cat.sch.real_table", "cat.sch.mv_in_disguise"],
        "_metric_views": ["cat.sch.mv_orders"],
        "_functions": [],
        "_parsed_space": {},
    }

    cols = [
        {
            "catalog_name": "cat", "schema_name": "sch",
            "table_name": "real_table",
            "column_name": "id", "data_type": "string",
        },
        {
            "catalog_name": "cat", "schema_name": "sch",
            "table_name": "mv_orders",
            "column_name": "region", "data_type": "string",
            "column_type": "dimension",
        },
    ]

    captured_stage_detail: dict = {}

    def fake_write_stage(spark, run_id, stage_name, status, **kwargs):
        if stage_name == "DATA_PROFILING" and status == "COMPLETE":
            captured_stage_detail.update(kwargs.get("detail") or {})

    mv_profile_result = {
        "row_count": 50,
        "columns": {
            "region": {"cardinality": 4},
            "channel": {"cardinality": 3},
        },
        "measures": {"total_revenue": {"expression": "SUM(amount)"}},
        "kind": "metric_view",
    }

    table_profile_result = (
        {"cat.sch.real_table": {"row_count": 100, "columns": {"id": {"cardinality": 5}}}},
        ["cat.sch.mv_in_disguise"],  # reclassified at runtime
    )

    with (
        patch.object(preflight, "_compute_join_overlaps", return_value=[]),
        patch.object(preflight, "_validate_core_access"),
        patch.object(preflight, "write_stage", side_effect=fake_write_stage),
        patch.object(preflight, "_update_run_status"),
        patch.object(preflight, "_collect_or_empty", return_value=(cols, None)),
        patch.object(
            preflight, "_collect_data_profile",
            return_value=table_profile_result,
        ),
        patch.object(
            preflight, "_detect_metric_views_via_catalog",
            return_value=({"cat.sch.mv_orders"}, {"cat.sch.mv_orders": {"source": "x"}}),
        ),
        patch.object(preflight, "get_columns_for_tables_rest", return_value=[]),
        patch.object(preflight, "get_tags_for_tables_rest", return_value=[]),
        patch.object(preflight, "get_routines_for_schemas_rest", return_value=[]),
        patch.object(preflight, "get_foreign_keys_for_tables_rest", return_value=[]),
    ):
        # Pre-seed the data_profile so the stage detail can read it
        # (the actual MV-aware dispatch is exercised in the dedicated
        # ``_profile_metric_view`` tests above; this assertion is only
        # about the stage-detail enrichment).
        preflight._collect_data_profile.return_value = (
            {**table_profile_result[0], "cat.sch.mv_orders": mv_profile_result},
            ["cat.sch.mv_in_disguise"],
        )
        preflight.preflight_collect_uc_metadata(
            w=MagicMock(),
            spark=MagicMock(),
            run_id="run-test",
            catalog="cat",
            schema="sch",
            config=config,
            snapshot={},
            genie_table_refs=[
                ("cat", "sch", "real_table"),
                ("cat", "sch", "mv_in_disguise"),
                ("cat", "sch", "mv_orders"),
            ],
        )

    assert captured_stage_detail.get("metric_views_detected_via_catalog") == 1, (
        f"Expected 1 catalog-detected MV; got detail={captured_stage_detail!r}"
    )
    assert captured_stage_detail.get("metric_views_reclassified_at_runtime") == 1, (
        f"Expected 1 runtime reclassification; got detail={captured_stage_detail!r}"
    )
    outcomes = captured_stage_detail.get("metric_view_profile_outcomes")
    assert isinstance(outcomes, list) and outcomes, (
        f"Expected non-empty outcomes list; got {outcomes!r}"
    )
    fqns = {o.get("fqn") for o in outcomes}
    assert "cat.sch.mv_orders" in fqns
    assert "cat.sch.mv_in_disguise" in fqns
    by_fqn = {o["fqn"]: o for o in outcomes}
    assert by_fqn["cat.sch.mv_orders"]["outcome"] == "profiled"
    assert by_fqn["cat.sch.mv_orders"].get("dimensions_profiled") == 2
    assert by_fqn["cat.sch.mv_in_disguise"]["outcome"] == "reclassified"


def test_collect_data_profile_dispatches_mvs_to_mv_path():
    """Effective MVs in ``tables`` argument are routed through the MV
    profile path; their result lands in ``profile`` (not in
    ``reclassified_mvs``)."""
    from genie_space_optimizer.optimization import evaluation as ev_mod
    from genie_space_optimizer.optimization import preflight

    table_fqn = _MV_FQN
    mv_short = table_fqn.split(".")[-1].lower()

    def fake_exec_sql(sql, *args, **kwargs):
        upper = sql.upper()
        if upper.startswith("SELECT COUNT(*)"):
            return pd.DataFrame([{"cnt": 100}])
        if "_CARD_" in upper:
            return pd.DataFrame([
                {"_card_region": 4, "_min_region": "A", "_max_region": "Z"},
            ])
        if "COLLECT_SET" in upper:
            return pd.DataFrame([{"vals": '["A","B","C","D"]'}])
        raise AssertionError(f"Unexpected SQL: {sql[:120]!r}")

    with (
        patch.object(ev_mod, "_exec_sql", side_effect=fake_exec_sql),
        patch.object(
            preflight,
            "_profile_metric_view",
            return_value={
                "row_count": 100,
                "columns": {"region": {"cardinality": 4}},
                "measures": {"total_revenue": {"expression": "SUM(amount)"}},
            },
        ) as mock_mv,
    ):
        profile, reclassified = preflight._collect_data_profile(
            spark=MagicMock(),
            tables=[table_fqn],
            uc_columns=_uc_columns_for_mv(table_fqn),
            metric_view_names=frozenset({mv_short}),
            metric_view_yaml={table_fqn.lower(): _MV_YAML},
            w=None,
            warehouse_id="",
            catalog="cat",
            schema="sch",
        )

    assert reclassified == []
    assert table_fqn in profile or table_fqn.lower() in profile
    assert mock_mv.called, "_profile_metric_view was not dispatched"
